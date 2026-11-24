"""PostgreSQL 访问层与死锁实跑执行器。

试跑隔离
========
- 所有试跑在独立 schema ``deadlock_trial`` 下的临时表进行，与业务表
  ``public.work_orders`` 物理隔离；
- 每轮试跑 DROP SCHEMA ... CASCADE 后重建，上一轮的行、锁、改派结果
  全部物理清除；
- 每批一个独立连接/线程，事务只可能 COMMIT 或 ROLLBACK，结束后连接
  归还并关闭；试跑收尾再用 ``pg_stat_activity`` 核查无未结束事务、
  用全表扫描核查无改派残留。
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import psycopg
from psycopg import sql

from .config import settings
from .models import (
    BatchInput,
    BatchWait,
    CycleReport,
    ReplayReport,
    ReplayStep,
)

DEADLOCK_SQLSTATE = "40P01"


def connect(autocommit: bool = False) -> psycopg.Connection:
    conn = psycopg.connect(settings.database_url, autocommit=autocommit)
    return conn


# --------------------------------------------------------------------------- #
# 业务表（演示用，真实环境由迁移脚本维护）
# --------------------------------------------------------------------------- #
def ensure_business_schema() -> None:
    with connect(autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS public.work_orders (
                order_id   text PRIMARY KEY,
                assignee   text NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )


def seed_orders(order_ids: List[str]) -> None:
    """确保工单行存在（幂等），负责人初始为 'init'。"""
    if not order_ids:
        return
    ensure_business_schema()
    with connect(autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO public.work_orders(order_id, assignee)
            VALUES (%s, 'init')
            ON CONFLICT (order_id) DO NOTHING
            """,
            [(oid,) for oid in sorted(set(order_ids))],
        )


def list_orders() -> List[Tuple[str, str]]:
    ensure_business_schema()
    with connect(autocommit=True) as conn:
        return conn.execute(
            "SELECT order_id, assignee FROM public.work_orders ORDER BY order_id"
        ).fetchall()


# --------------------------------------------------------------------------- #
# 试跑 schema 管理
# --------------------------------------------------------------------------- #
def reset_trial_schema(conn: psycopg.Connection) -> None:
    conn.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
            sql.Identifier(settings.trial_schema)
        )
    )
    conn.execute(
        sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(settings.trial_schema))
    )
    conn.execute(
        sql.SQL(
            """
            CREATE TABLE {schema}.work_orders (
                order_id   text PRIMARY KEY,
                assignee   text NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        ).format(schema=sql.Identifier(settings.trial_schema))
    )


def _trial_table() -> sql.Composed:
    return sql.SQL("{}.work_orders").format(sql.Identifier(settings.trial_schema))


# --------------------------------------------------------------------------- #
# 实跑执行器
# --------------------------------------------------------------------------- #
@dataclass
class _BatchRun:
    batch: BatchInput
    error_sqlstate: Optional[str] = None
    error_message: Optional[str] = None
    committed: bool = False
    blocked_order: Optional[str] = None
    blocked_holder: Optional[str] = None
    last_step: int = 0
    steps: List[ReplayStep] = field(default_factory=list)


def _holder_of(admin_conn: psycopg.Connection, order_id: str) -> Optional[str]:
    """查当前持有指定工单行锁的试跑后端 pid。

    PostgreSQL 的行锁不体现在 pg_locks（tuple 锁只在阻塞等待时出现），
    持有者事务 id 记录在行头 xmax。通过 pg_stat_activity.backend_xid
    关联即可拿到持有行锁的后端。返回 pid 字符串，查不到返回 None。
    """
    row = admin_conn.execute(
        sql.SQL(
            """
            SELECT a.pid
            FROM {tbl} w
            JOIN pg_stat_activity a
              ON a.backend_xid = w.xmax
            WHERE w.order_id = %s
              AND a.datname = current_database()
              AND a.pid <> pg_backend_pid()
            LIMIT 1
            """
        ).format(tbl=_trial_table()),
        (order_id,),
    ).fetchone()
    return str(row[0]) if row else None


def _update_order(conn: psycopg.Connection, order_id: str, assignee: str) -> None:
    conn.execute(
        sql.SQL(
            "UPDATE {tbl} SET assignee = %s, updated_at = now() WHERE order_id = %s"
        ).format(tbl=_trial_table()),
        (assignee, order_id),
    )


def _is_waiting(admin_conn: psycopg.Connection, pid: int) -> Optional[str]:
    """指定后端当前是否在等锁；返回等待事件名，否则 None。"""
    row = admin_conn.execute(
        """
        SELECT wait_event_type, wait_event
        FROM pg_stat_activity
        WHERE pid = %s AND wait_event_type = 'Lock'
        """,
        (pid,),
    ).fetchone()
    return f"{row[0]}.{row[1]}" if row else None


def replay_deadlock(
    batches: List[BatchInput], cycle: CycleReport
) -> ReplayReport:
    """按检测到的环构造交错调度并实跑，验证是否收到 SQLSTATE 40P01。"""
    admin = connect(autocommit=True)
    admin.execute(f"SET lock_timeout = {int(settings.lock_timeout_ms)}")
    reset_trial_schema(admin)
    try:
        return _replay_inner(batches, cycle, admin)
    finally:
        # 任何异常路径都不允许遗留试跑 schema / 未结束事务
        try:
            admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(settings.trial_schema)
                )
            )
        finally:
            admin.close()


def _replay_inner(
    batches: List[BatchInput],
    cycle: CycleReport,
    admin: psycopg.Connection,
) -> ReplayReport:
    all_orders = sorted({oid for b in batches for oid in b.order_ids})
    with admin.cursor() as cur:
        cur.executemany(
            sql.SQL(
                "INSERT INTO {tbl}(order_id, assignee) VALUES (%s, 'init')"
            ).format(tbl=_trial_table()),
            [(oid,) for oid in all_orders],
        )

    # 环上的批次（cycle_batches 不重复），cycle_orders[k] 是
    # cycle_batches[k] 等待下一个批次时所争用的工单。
    cyc_batches = cycle.cycle_batches
    cyc_orders = cycle.cycle_orders
    n = len(cyc_batches)

    # 每个环上批次的"阻塞工单"= 它等待的下一个批次先拿到的那张单
    block_order: Dict[str, str] = {}
    for k, bid in enumerate(cyc_batches):
        block_order[bid] = cyc_orders[k]

    # 非环上批次（三批输入、二环时存在）按各自完整顺序参与调度，
    # 它们不持有环上的争用单（否则也会在环里），正常跑完即可。

    barrier = threading.Barrier(len(batches))
    runs: Dict[str, _BatchRun] = {
        b.batch_id: _BatchRun(batch=b) for b in batches
    }
    seq_lock = threading.Lock()
    seq_counter = {"v": 0}

    def next_seq() -> int:
        with seq_lock:
            seq_counter["v"] += 1
            return seq_counter["v"]

    def worker(batch: BatchInput) -> None:
        run = runs[batch.batch_id]
        conn = connect()
        conn.execute(f"SET lock_timeout = {int(settings.lock_timeout_ms)}")
        own_pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
        block_recorded = threading.Event()
        try:
            conn.execute("BEGIN")
            for step, oid in enumerate(batch.order_ids, start=1):
                # 锁步：保证环上各批按同一全局节拍交错，避免某个批次抢跑
                # 直接拿齐两把锁把死锁退化成普通等待。
                barrier.wait(settings.trial_timeout_s)

                # UPDATE 放在内部线程执行，本线程轮询 pg_stat_activity：
                # 一旦观察到本后端进入 Lock 等待，先落一条 block 记录。
                update_err: dict[str, Exception] = {}

                def do_update() -> None:
                    try:
                        _update_order(conn, oid, batch.assignee)
                    except Exception as exc:  # noqa: BLE001
                        update_err["exc"] = exc

                ut = threading.Thread(target=do_update, daemon=True)
                ut.start()
                while ut.is_alive():
                    ut.join(timeout=0.02)
                    wait = _is_waiting(admin, own_pid)
                    if wait and not block_recorded.is_set():
                        run.blocked_order = oid
                        run.blocked_holder = _holder_of(admin, oid)
                        run.steps.append(
                            ReplayStep(
                                seq=next_seq(),
                                batch_id=batch.batch_id,
                                step=step,
                                order_id=oid,
                                action="block",
                                note=(
                                    f"进入锁等待（{wait}），持有方连接 "
                                    f"pid={run.blocked_holder}"
                                ),
                            )
                        )
                        block_recorded.set()
                if "exc" in update_err:
                    exc = update_err["exc"]
                    if isinstance(exc, psycopg.errors.DeadlockDetected):
                        run.error_sqlstate = exc.sqlstate
                        run.error_message = str(exc).strip()
                        run.blocked_order = oid
                        run.last_step = step - 1
                        run.steps.append(
                            ReplayStep(
                                seq=next_seq(),
                                batch_id=batch.batch_id,
                                step=step,
                                order_id=oid,
                                action="abort",
                                note=(
                                    "PostgreSQL 死锁检测中止本事务，"
                                    f"SQLSTATE={exc.sqlstate}"
                                ),
                            )
                        )
                        conn.rollback()
                        return
                    if isinstance(exc, psycopg.errors.LockNotAvailable):
                        run.error_sqlstate = exc.sqlstate
                        run.error_message = str(exc).strip()
                        run.blocked_order = oid
                        run.last_step = step - 1
                        if not block_recorded.is_set():
                            run.steps.append(
                                ReplayStep(
                                    seq=next_seq(),
                                    batch_id=batch.batch_id,
                                    step=step,
                                    order_id=oid,
                                    action="block",
                                    note=(
                                        f"lock_timeout {settings.lock_timeout_ms}ms"
                                        " 触发，未及采样到等待事件"
                                    ),
                                )
                            )
                        conn.rollback()
                        return
                    raise exc
                # UPDATE 正常返回：行锁已到手（若此前在等待，等待已结束）
                run.last_step = step
                run.steps.append(
                    ReplayStep(
                        seq=next_seq(),
                        batch_id=batch.batch_id,
                        step=step,
                        order_id=oid,
                        action="acquire",
                        note="曾阻塞等待后获得锁"
                        if block_recorded.is_set()
                        else None,
                    )
                )
                block_recorded.clear()
            conn.execute("COMMIT")
            run.committed = True
        except Exception:  # noqa: BLE001 - 试跑线程内兜底，保证连接关闭
            try:
                conn.rollback()
            except Exception:  # pragma: no cover
                pass
            raise
        finally:
            try:
                conn.close()
            except Exception:  # pragma: no cover
                pass

    timeout = settings.trial_timeout_s + 5
    with ThreadPoolExecutor(max_workers=len(batches)) as pool:
        futures = [pool.submit(worker, b) for b in batches]
        worker_errors: List[str] = []
        for f in futures:
            try:
                f.result(timeout=timeout)
            except FutTimeout:
                worker_errors.append("试跑线程整体超时")
            except Exception as exc:  # noqa: BLE001
                worker_errors.append(f"{type(exc).__name__}: {exc}")

    # 阻塞信息补全：被死锁检测中止时当前事务已回滚，持锁方可能已查不到，
    # 用等待图静态信息补齐 holder 批次。
    holder_by_order: Dict[str, str] = {}
    for k, bid in enumerate(cyc_batches):
        holder_by_order[cyc_orders[k]] = cyc_batches[(k + 1) % n]

    victims = [bid for bid, r in runs.items() if r.error_sqlstate == DEADLOCK_SQLSTATE]
    lock_timeouts = [
        bid for bid, r in runs.items()
        if r.error_sqlstate == "55P03"
    ]

    # 汇总操作顺序
    all_steps: List[ReplayStep] = sorted(
        [s for r in runs.values() for s in r.steps],
        key=lambda s: s.seq,
    )

    batch_waits: List[BatchWait] = []
    for bid in cyc_batches:
        r = runs[bid]
        waits_order = block_order[bid]
        batch_waits.append(
            BatchWait(
                batch_id=bid,
                waits_for_order=waits_order,
                waits_for_holder=holder_by_order[waits_order],
                last_acquired_step=r.last_step,
            )
        )

    reproduced = len(victims) > 0
    if reproduced:
        detail = (
            f"PostgreSQL deadlock_detector 中止了批次 {sorted(victims)}，"
            f"收到 SQLSTATE {DEADLOCK_SQLSTATE}（deadlock_detected），死锁复现成功。"
        )
        sqlstate = DEADLOCK_SQLSTATE
    elif lock_timeouts:
        detail = (
            f"未收到 40P01：环上批次 {sorted(lock_timeouts)} 先触发了 lock_timeout"
            f"（SQLSTATE 55P03，等待 {settings.lock_timeout_ms}ms）。"
            "说明调度形成了真实锁等待但死锁检测未在超时前打破环；"
            "可调大 LOCK_TIMEOUT_MS 或调小服务端 deadlock_timeout 后重试。"
        )
        sqlstate = "55P03"
    elif worker_errors:
        detail = "试跑执行异常，未能确认死锁：" + "；".join(worker_errors)
        sqlstate = None
    else:
        detail = (
            "未收到 40P01：所有批次均正常 COMMIT，调度未形成环。"
            "可能原因：环上工单在某批中并非排他更新、或数据库未走行锁路径。"
        )
        sqlstate = None

    cleanup_ok, cleanup_note = _verify_and_cleanup(admin, all_orders)
    if not cleanup_ok:
        detail += f" 收尾核查失败：{cleanup_note}"

    return ReplayReport(
        reproduced=reproduced,
        sqlstate=sqlstate,
        victim_batch=sorted(victims)[0] if victims else None,
        detail=detail,
        steps=all_steps,
        batch_waits=batch_waits,
        cleanup_ok=cleanup_ok,
    )


def _verify_and_cleanup(
    admin: psycopg.Connection,
    order_ids: List[str],
) -> Tuple[bool, str]:
    """核查无残留事务，并物理销毁试跑 schema（含全部改派结果与锁）。"""
    # 1. 无未结束的试跑事务（排除当前 admin 连接与本查询自身）
    rows = admin.execute(
        """
        SELECT pid, state, xact_start
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND xact_start IS NOT NULL
          AND state IN ('idle in transaction', 'active')
        """
    ).fetchall()
    if rows:
        return False, f"仍有未结束事务：{rows}"

    # 2. 统计 DROP 前试跑表内的改派行数（死锁复现时存活方会 COMMIT，
    # 存在已更新行是预期现象；它们随下一步 schema DROP 物理消失）。
    dirty = admin.execute(
        sql.SQL(
            "SELECT count(*) FROM {tbl} WHERE assignee <> 'init'"
        ).format(tbl=_trial_table())
    ).fetchone()[0]

    # 3. 物理销毁试跑 schema（行、锁、表全部消失）
    admin.execute(
        sql.SQL("DROP SCHEMA {} CASCADE").format(
            sql.Identifier(settings.trial_schema)
        )
    )
    left = admin.execute(
        "SELECT count(*) FROM pg_namespace WHERE nspname = %s",
        (settings.trial_schema,),
    ).fetchone()[0]
    if left:
        return False, "试跑 schema 删除后仍存在"
    return True, f"ok（DROP 前试跑表有 {dirty} 行已改派，已随 schema 清除）"

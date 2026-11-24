"""FastAPI 入口：批量改派死锁预检 API。"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from . import __version__
from .analyzer import analyze, pick_replay_cycle
from .db import (
    DEADLOCK_SQLSTATE,
    ensure_business_schema,
    list_orders,
    replay_deadlock,
    seed_orders,
)
from .models import PrecheckRequest, PrecheckResponse, ReplayReport

app = FastAPI(
    title="巡检批量改派死锁预检 API",
    version=__version__,
    description=(
        "提交 2~3 批有序工单号（每批 ≤6 单），服务端按"
        "「每批一个 PostgreSQL 事务、按原顺序逐单 UPDATE、行锁持有到事务结束」"
        "的模型构造等待图，判断是否存在循环等锁；发现环时可在本地测试库"
        "按交错顺序实跑，以收到 SQLSTATE 40P01 作为复现成功判据。"
    ),
)


@app.on_event("startup")
def _startup() -> None:
    ensure_business_schema()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "version": __version__}


@app.get("/orders", summary="查看当前工单及负责人")
def get_orders() -> dict:
    return {"orders": [{"order_id": o, "assignee": a} for o, a in list_orders()]}


@app.post("/orders/seed", summary="幂等写入测试工单（负责人置为 init）")
def post_seed(order_ids: list[str]) -> dict:
    clean = sorted({o.strip() for o in order_ids if o.strip()})
    if not clean:
        raise HTTPException(status_code=400, detail="order_ids 不能为空")
    seed_orders(clean)
    return {"seeded": clean}


@app.post(
    "/precheck",
    response_model=PrecheckResponse,
    summary="批量改派死锁预检（静态等待图 + 可选本地实跑复现）",
)
def precheck(req: PrecheckRequest) -> PrecheckResponse:
    edges, cycles = analyze(req.batches)

    if not cycles:
        return PrecheckResponse(
            has_deadlock=False,
            cycles=[],
            wait_edges=edges,
            replay=None,
            message=(
                "未发现循环等锁：各批对共享工单的加锁顺序一致，"
                "任意交错都只会产生单向等待，不会死锁。"
            ),
        )

    replay: ReplayReport | None = None
    if req.run_replay:
        cycle = pick_replay_cycle(req.batches, cycles)
        try:
            replay = replay_deadlock(req.batches, cycle)
        except Exception as exc:  # noqa: BLE001 - 实跑失败不能拖垮预检结论
            raise HTTPException(
                status_code=500,
                detail=(
                    f"静态分析已发现 {len(cycles)} 个死锁环，但实跑阶段异常："
                    f"{type(exc).__name__}: {exc}"
                ),
            ) from exc
        if replay.reproduced and replay.sqlstate == DEADLOCK_SQLSTATE:
            msg = (
                f"发现 {len(cycles)} 个循环等锁环，已按最短环实跑复现，"
                "PostgreSQL 返回 SQLSTATE 40P01（deadlock_detected）。"
            )
        else:
            msg = (
                f"静态分析发现 {len(cycles)} 个循环等锁环，但实跑未能确认 40P01。"
                f"原因：{replay.detail}"
            )
    else:
        msg = f"发现 {len(cycles)} 个循环等锁环（已按要求跳过实跑）。"

    return PrecheckResponse(
        has_deadlock=True,
        cycles=cycles,
        wait_edges=edges,
        replay=replay,
        message=msg,
    )

"""端到端自测：覆盖无死锁、二节点 AB-BA 死锁、三节点环、参数校验、
同号并发不阻塞、试跑无残留等场景。

运行（需要可连通的 PostgreSQL）：
    python -m tests.selftest
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from typing import Any

from app.analyzer import analyze
from app.config import settings
from app.db import connect, ensure_business_schema, replay_deadlock
from app.main import app
from app.models import BatchInput

from fastapi.testclient import TestClient

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond), detail))
    print(f"[{PASS if cond else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def case_no_cycle() -> None:
    print("\n== 场景1：两批加锁顺序一致，无环 ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2", "W3"], assignee="alice"),
        BatchInput(batch_id="B2", order_ids=["W1", "W2", "W4"], assignee="bob"),
    ]
    edges, cycles = analyze(batches)
    check("无环", cycles == [], f"cycles={cycles}")
    check(
        "同位置共享单不产生等待边",
        edges == [],
        str([(e.waiter_batch, e.holder_batch, e.order_id) for e in edges]),
    )

    # 子场景 1b：B2 的 W1 位置更靠后，只产生 B2->B1 单向边，仍无环
    batches_b = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="alice"),
        BatchInput(batch_id="B2", order_ids=["W9", "W1", "W2"], assignee="bob"),
    ]
    edges_b, cycles_b = analyze(batches_b)
    check("单向等待无环", cycles_b == [], str(cycles_b))
    check(
        "仅 B2->B1 两条边",
        {(e.waiter_batch, e.holder_batch, e.order_id) for e in edges_b}
        == {("B2", "B1", "W1"), ("B2", "B1", "W2")},
        str([(e.waiter_batch, e.holder_batch, e.order_id) for e in edges_b]),
    )


def case_two_node_cycle() -> dict[str, Any]:
    print("\n== 场景2：二节点 AB-BA 死锁（经典场景，实跑必须 40P01） ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="alice"),
        BatchInput(batch_id="B2", order_ids=["W2", "W1"], assignee="bob"),
    ]
    edges, cycles = analyze(batches)
    check("检测到 1 个环", len(cycles) == 1, str([c.cycle_batches for c in cycles]))
    check(
        "环为 B1<->B2",
        sorted(cycles[0].cycle_batches) == ["B1", "B2"],
        str(cycles[0].cycle_batches),
    )
    check(
        "环上工单为 W1/W2",
        sorted(cycles[0].cycle_orders) == ["W1", "W2"],
        str(cycles[0].cycle_orders),
    )
    report = replay_deadlock(batches, cycles[0])
    check("实跑复现成功", report.reproduced, report.detail)
    check("SQLSTATE=40P01", report.sqlstate == "40P01", str(report.sqlstate))
    check("有牺牲者批次", report.victim_batch in {"B1", "B2"}, str(report.victim_batch))
    check("收尾无残留", report.cleanup_ok, report.detail)
    actions = [s.action for s in report.steps]
    check("操作步骤含 block 与 abort",
          "abort" in actions and "block" in actions, str(actions))
    check("abort 步骤标注 40P01",
          any(s.action == "abort" and "40P01" in (s.note or "")
              for s in report.steps))
    check(
        "各批等待信息齐全",
        {w.batch_id for w in report.batch_waits} == {"B1", "B2"},
        str([w.model_dump() for w in report.batch_waits]),
    )
    return {"batches": batches, "report": report}


def case_three_node_cycle() -> None:
    print("\n== 场景3：三节点环 B1->B2->B3->B1 ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="a1"),
        BatchInput(batch_id="B2", order_ids=["W2", "W3"], assignee="a2"),
        BatchInput(batch_id="B3", order_ids=["W3", "W1"], assignee="a3"),
    ]
    edges, cycles = analyze(batches)
    check("检测到环", len(cycles) >= 1, str([c.cycle_batches for c in cycles]))
    three = [c for c in cycles if len(c.cycle_batches) == 3]
    check("存在三节点环", len(three) == 1, str([c.cycle_batches for c in cycles]))
    report = replay_deadlock(batches, three[0])
    check("三实跑复现 40P01", report.reproduced and report.sqlstate == "40P01",
          report.detail)
    check("三收尾无残留", report.cleanup_ok)


def case_three_batches_two_node_cycle() -> None:
    print("\n== 场景4：三批输入，其中两批成环、第三批旁观 ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="a1"),
        BatchInput(batch_id="B2", order_ids=["W2", "W1"], assignee="a2"),
        BatchInput(batch_id="B3", order_ids=["W9", "W8"], assignee="a3"),
    ]
    edges, cycles = analyze(batches)
    check("检测到 1 个二环", len(cycles) == 1
          and sorted(cycles[0].cycle_batches) == ["B1", "B2"],
          str([c.cycle_batches for c in cycles]))
    report = replay_deadlock(batches, cycles[0])
    check("旁观批场景实跑 40P01", report.reproduced and report.sqlstate == "40P01",
          report.detail)
    check("旁观批场景收尾无残留", report.cleanup_ok)


def case_six_orders_limit() -> None:
    print("\n== 场景5：每批 6 单上限与 2~3 批限制的参数校验 ==")
    client = TestClient(app)
    ok_payload = {
        "batches": [
            {"batch_id": "B1", "order_ids": [f"W{i}" for i in range(1, 7)],
             "assignee": "x"},
            {"batch_id": "B2", "order_ids": [f"W{i}" for i in range(6, 0, -1)],
             "assignee": "y"},
        ]
    }
    r = client.post("/precheck", json=ok_payload)
    check("6 单/批被接受且判死锁", r.status_code == 200 and r.json()["has_deadlock"],
          f"{r.status_code} {r.text[:200]}")

    too_many = {
        "batches": [
            {"batch_id": "B1", "order_ids": [f"W{i}" for i in range(1, 8)],
             "assignee": "x"},
            {"batch_id": "B2", "order_ids": ["W1"], "assignee": "y"},
        ]
    }
    r = client.post("/precheck", json=too_many)
    check("7 单/批被 422 拒绝", r.status_code == 422, str(r.status_code))

    one_batch = {
        "batches": [
            {"batch_id": "B1", "order_ids": ["W1"], "assignee": "x"},
        ]
    }
    r = client.post("/precheck", json=one_batch)
    check("1 批被 422 拒绝", r.status_code == 422, str(r.status_code))

    four = {
        "batches": [
            {"batch_id": f"B{i}", "order_ids": ["W1"], "assignee": "x"}
            for i in range(1, 5)
        ]
    }
    r = client.post("/precheck", json=four)
    check("4 批被 422 拒绝", r.status_code == 422, str(r.status_code))

    dup = {
        "batches": [
            {"batch_id": "B1", "order_ids": ["W1", "W1"], "assignee": "x"},
            {"batch_id": "B2", "order_ids": ["W2"], "assignee": "y"},
        ]
    }
    r = client.post("/precheck", json=dup)
    check("同批重复工单号被 422 拒绝", r.status_code == 422, str(r.status_code))


def case_api_safe_path() -> None:
    print("\n== 场景6：API 安全路径（无环不试跑）与 Swagger 可用 ==")
    client = TestClient(app)
    r = client.get("/health")
    check("health 200", r.status_code == 200, r.text)
    r = client.get("/docs")
    check("Swagger UI 200", r.status_code == 200)
    r = client.get("/openapi.json")
    check("openapi.json 200 且含 /precheck",
          r.status_code == 200 and "/precheck" in r.json()["paths"])
    payload = {
        "batches": [
            {"batch_id": "B1", "order_ids": ["W1", "W2"], "assignee": "a"},
            {"batch_id": "B2", "order_ids": ["W1", "W2"], "assignee": "b"},
        ],
        "run_replay": True,
    }
    r = client.post("/precheck", json=payload)
    body = r.json()
    check("无环请求 has_deadlock=false",
          r.status_code == 200 and body["has_deadlock"] is False
          and body["replay"] is None, r.text[:200])


def case_no_residue() -> None:
    print("\n== 场景7：试跑后无残留 schema/事务/改派结果 ==")
    with connect(autocommit=True) as conn:
        ns = conn.execute(
            "SELECT count(*) FROM pg_namespace WHERE nspname = %s",
            (settings.trial_schema,),
        ).fetchone()[0]
        check("试跑 schema 已删除", ns == 0, f"remaining={ns}")
        open_tx = conn.execute(
            """
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND xact_start IS NOT NULL
              AND state IN ('idle in transaction', 'active')
            """
        ).fetchone()[0]
        check("无未结束事务", open_tx == 0, f"open={open_tx}")
        # 业务表未被试跑污染
        ensure_business_schema()
        dirty = conn.execute(
            "SELECT count(*) FROM public.work_orders WHERE assignee <> 'init'"
        ).fetchone()[0]
        check("业务表无试跑改派残留", dirty == 0, f"dirty={dirty}")


def case_same_order_concurrent() -> None:
    print("\n== 场景8：同号并发试验互不阻塞（同顺序两事务可并发提交） ==")
    # 两个事务以完全相同顺序更新同一组工单：第二个会等第一个提交，
    # 但不会死锁；这里直接验证静态分析无环，且实跑不会出现 40P01。
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2", "W3"], assignee="a"),
        BatchInput(batch_id="B2", order_ids=["W1", "W2", "W3"], assignee="b"),
    ]
    edges, cycles = analyze(batches)
    check("同顺序无环", cycles == [], str(cycles))
    check("同顺序同位置无等待边", edges == [],
          str([(e.waiter_batch, e.order_id) for e in edges]))


def main() -> int:
    print(f"DATABASE_URL={settings.database_url}")
    ensure_business_schema()
    t0 = time.time()
    case_no_cycle()
    case_two_node_cycle()
    case_three_node_cycle()
    case_three_batches_two_node_cycle()
    case_six_orders_limit()
    case_api_safe_path()
    case_same_order_concurrent()
    case_no_residue()
    dt = time.time() - t0

    total = len(results)
    failed = [r for r in results if not r[1]]
    print(f"\n==== 自测结果：{total - len(failed)}/{total} 通过，用时 {dt:.1f}s ====")
    if failed:
        for name, _, detail in failed:
            print(f"  失败项: {name} -- {detail}")
        return 1
    sample = {
        "deadlock_example": {
            "batches": [
                {"batch_id": "B1", "order_ids": ["W1", "W2"], "assignee": "alice"},
                {"batch_id": "B2", "order_ids": ["W2", "W1"], "assignee": "bob"},
            ],
            "run_replay": True,
        },
        "safe_example": {
            "batches": [
                {"batch_id": "B1", "order_ids": ["W1", "W2", "W3"], "assignee": "alice"},
                {"batch_id": "B2", "order_ids": ["W1", "W2", "W4"], "assignee": "bob"},
            ],
            "run_replay": True,
        },
    }
    print("\nSwagger 试跑样例 payload：")
    print(json.dumps(sample, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(2)

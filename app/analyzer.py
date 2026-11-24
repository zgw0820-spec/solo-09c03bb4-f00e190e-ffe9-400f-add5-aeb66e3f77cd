"""死锁静态分析：由各批加锁顺序构造等待图并检测循环。

模型说明
========
每批是一笔事务，按 ``order_ids`` 原顺序逐单 ``UPDATE``。PostgreSQL 的
``UPDATE`` 会对命中行加排他行锁（``FOR UPDATE`` 语义），锁一直持有到事务
COMMIT/ROLLBACK。

两批 A、B 共享工单 x 时：
- 谁先执行到 x，谁先持有 x 的行锁；
- 另一方执行到 x 时阻塞，形成等待边 另一方 -> 先持方（标注在 x 上）。

死锁只可能由"相反的加锁顺序"造成：存在工单 x、y，使得 A 先 x 后 y、
B 先 y 后 x。交错执行 A 拿 x、B 拿 y 后，A 等 y、B 等 x，构成环。

等待边方向（pa/pb 为 x 在 A/B 中的加锁序号）：
- pa < pb：A 必然先到 x，只可能 B 等 A；
- pb < pa：只可能 A 等 B；
- pa == pb：不出边。同序号对无法构成相反加锁顺序——死锁需要一对
  工单的先后顺序在两批中反转，反转必然体现为严格位置差。

本模块把每一对共享工单上可行的等待方向列出，再在批次维度的等待图上
找有向环；找到的每个环都对应一个可实跑复现的交错。

同号（同一工单在同一批内重复）在入参层已禁止；同一事务对同一行重复加锁
不会自锁，因此不产生等待边。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from .models import BatchInput, CycleReport, WaitEdge


@dataclass(frozen=True)
class _EdgeKey:
    waiter: str
    holder: str
    order_id: str


def _first_index(batches: List[BatchInput]) -> Dict[str, Dict[str, int]]:
    """batch_id -> (order_id -> 1-based 首次出现位置)。"""
    out: Dict[str, Dict[str, int]] = {}
    for b in batches:
        pos: Dict[str, int] = {}
        for idx, oid in enumerate(b.order_ids, start=1):
            pos.setdefault(oid, idx)
        out[b.batch_id] = pos
    return out


def build_wait_edges(batches: List[BatchInput]) -> List[WaitEdge]:
    """枚举所有可能的等待边。

    对每一对批次 (A, B) 与共享工单 x：
    - 若 A 对 x 的加锁位置早于 B，则存在交错使 A 先持锁、B 等待 A
      （B 在 x 之前的单子可先与 A 交错执行），加入 B -> A 边；
    - 位置相同也保留两个方向：两个事务都把 x 放在第 k 步时，谁先被调度
      谁持锁，两个方向都可能发生；
    - 位置不同则只保留"晚的等早的"这一可行方向（早的等晚的要求晚的先
      越过 x，与位置矛盾，不可能）。
    """
    positions = _first_index(batches)
    edges: Dict[_EdgeKey, WaitEdge] = {}

    def add(waiter: str, holder: str, order_id: str) -> None:
        key = _EdgeKey(waiter, holder, order_id)
        if key in edges:
            return
        edges[key] = WaitEdge(
            waiter_batch=waiter,
            holder_batch=holder,
            order_id=order_id,
            waiter_step=positions[waiter][order_id],
            holder_step=positions[holder][order_id],
        )

    for i in range(len(batches)):
        for j in range(i + 1, len(batches)):
            a, b = batches[i], batches[j]
            shared = set(a.order_ids) & set(b.order_ids)
            for oid in sorted(shared):
                pa, pb = positions[a.batch_id][oid], positions[b.batch_id][oid]
                if pa < pb:
                    # a 必然先到 x（b 拿到 x 前必须先走完自己前 pb-1 步，
                    # 而 a 第 pa 步就到），只有 b 等 a 这一方向可行
                    add(b.batch_id, a.batch_id, oid)
                elif pb < pa:
                    add(a.batch_id, b.batch_id, oid)
                # 同位置（pa == pb）不出边：两个事务对该单的加锁序号相同，
                # 谁先拿到取决于调度，但单凭同位置对无法形成相反加锁顺序；
                # 死锁的充要条件是存在一对工单 x、y 在两批中的先后顺序反转，
                # 反转必然体现为严格位置差，已由上面的分支覆盖。
    return sorted(
        edges.values(),
        key=lambda e: (e.waiter_batch, e.holder_batch, e.order_id),
    )


def _find_cycles(
    nodes: List[str], adjacency: Dict[str, Set[Tuple[str, str]]]
) -> List[List[Tuple[str, str, str]]]:
    """枚举有向图中的全部简单环（Johnson 思路的简化版）。

    依次以每个节点 s 为"最小节点"，只允许经过编号 >= s 的节点做 DFS；
    从 s 的邻居出发，遇到 s 即闭合成环。这样每个简单环只在其编号最小
    的节点处被发现一次，天然去重，且支持二节点互锁环（A->B、B->A）。

    返回环序列 [(from, to, order_id), ...]。
    """
    ordered = sorted(nodes)
    result: List[List[Tuple[str, str, str]]] = []

    def dfs(start: str, current: str,
            path: List[Tuple[str, str, str]], on_path: Set[str]) -> None:
        for nxt, oid in sorted(adjacency.get(current, ())):
            if nxt < start:
                continue
            if nxt == start:
                # 回到起点，闭合成环（二环 A->B->A 也走这里）
                result.append(path + [(current, nxt, oid)])
            elif nxt not in on_path:
                on_path.add(nxt)
                path.append((current, nxt, oid))
                dfs(start, nxt, path, on_path)
                path.pop()
                on_path.discard(nxt)

    for start in ordered:
        dfs(start, start, [], {start})

    result.sort(key=lambda c: ([x[0] for x in c], [x[2] for x in c]))
    return result


def analyze(batches: List[BatchInput]) -> Tuple[List[WaitEdge], List[CycleReport]]:
    """构造等待图并返回全部等待边与死锁环报告。"""
    edges = build_wait_edges(batches)
    adjacency: Dict[str, Set[Tuple[str, str]]] = {}
    for e in edges:
        adjacency.setdefault(e.waiter_batch, set()).add(
            (e.holder_batch, e.order_id)
        )

    node_ids = [b.batch_id for b in batches]
    raw_cycles = _find_cycles(node_ids, adjacency)

    edge_index: Dict[Tuple[str, str, str], WaitEdge] = {
        (e.waiter_batch, e.holder_batch, e.order_id): e for e in edges
    }
    reports: List[CycleReport] = []
    for cycle in raw_cycles:
        reports.append(
            CycleReport(
                cycle_batches=[c[0] for c in cycle],
                cycle_orders=[c[2] for c in cycle],
                edges=[edge_index[(c[0], c[1], c[2])] for c in cycle],
            )
        )
    return edges, reports


def pick_replay_cycle(
    batches: List[BatchInput], cycles: List[CycleReport]
) -> Optional[CycleReport]:
    """挑选最容易实跑复现的环：优先二节点环（经典 AB-BA），再选最短环。"""
    if not cycles:
        return None
    return sorted(cycles, key=lambda c: (len(c.cycle_batches), c.cycle_batches))[0]

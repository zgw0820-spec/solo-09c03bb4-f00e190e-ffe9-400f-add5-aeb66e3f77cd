"""请求/响应数据模型与入参校验。

规则：
- 2~3 批，每批 1~6 单；
- 工单号在同一批内不允许重复（同号并发试验互不阻塞，见 README）；
- 每批是一笔事务，按列表原顺序逐单 UPDATE，行锁持有到事务结束。
"""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .config import settings


class BatchInput(BaseModel):
    batch_id: str = Field(..., min_length=1, max_length=64, description="批次标识")
    order_ids: List[str] = Field(
        ...,
        min_length=1,
        max_length=settings.max_orders_per_batch,
        description="本批工单号，顺序即事务内加锁顺序",
    )
    assignee: str = Field(..., min_length=1, max_length=64, description="目标负责人")

    @field_validator("order_ids")
    @classmethod
    def _no_duplicate_in_batch(cls, v: List[str]) -> List[str]:
        seen: set[str] = set()
        for oid in v:
            if oid in seen:
                raise ValueError(
                    f"工单号 {oid} 在同一批内重复；同号并发试验互不阻塞，重复项无意义"
                )
            seen.add(oid)
        return v


class PrecheckRequest(BaseModel):
    batches: List[BatchInput] = Field(
        ...,
        description=f"{settings.min_batches}~{settings.max_batches} 批有序工单",
        json_schema_extra={
            "examples": [
                {
                    "batch_id": "B1",
                    "order_ids": ["W1", "W2"],
                    "assignee": "alice",
                },
                {
                    "batch_id": "B2",
                    "order_ids": ["W2", "W1"],
                    "assignee": "bob",
                },
            ]
        },
    )
    run_replay: bool = Field(
        default=True,
        description="发现死锁时是否在本地测试库按交错顺序实跑复现",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "summary": "AB-BA 死锁（必现 40P01）",
                    "value": {
                        "batches": [
                            {"batch_id": "B1", "order_ids": ["W1", "W2"],
                             "assignee": "alice"},
                            {"batch_id": "B2", "order_ids": ["W2", "W1"],
                             "assignee": "bob"},
                        ],
                        "run_replay": True,
                    },
                },
                {
                    "summary": "三节点循环等锁",
                    "value": {
                        "batches": [
                            {"batch_id": "B1", "order_ids": ["W1", "W2"],
                             "assignee": "a1"},
                            {"batch_id": "B2", "order_ids": ["W2", "W3"],
                             "assignee": "a2"},
                            {"batch_id": "B3", "order_ids": ["W3", "W1"],
                             "assignee": "a3"},
                        ],
                        "run_replay": True,
                    },
                },
                {
                    "summary": "同顺序安全批次（无死锁）",
                    "value": {
                        "batches": [
                            {"batch_id": "B1",
                             "order_ids": ["W1", "W2", "W3"],
                             "assignee": "alice"},
                            {"batch_id": "B2",
                             "order_ids": ["W9", "W1", "W2"],
                             "assignee": "bob"},
                        ],
                        "run_replay": True,
                    },
                },
            ]
        }
    }

    @model_validator(mode="after")
    def _check_batch_count(self) -> "PrecheckRequest":
        n = len(self.batches)
        if not (settings.min_batches <= n <= settings.max_batches):
            raise ValueError(
                f"批次数必须在 {settings.min_batches}~{settings.max_batches} 之间，收到 {n} 批"
            )
        ids = [b.batch_id for b in self.batches]
        if len(set(ids)) != len(ids):
            raise ValueError("batch_id 不允许重复")
        return self


class WaitEdge(BaseModel):
    """等待边：waiter 批次在执行 order_id 时等待 holder 批次释放行锁。"""

    waiter_batch: str
    holder_batch: str
    order_id: str
    waiter_step: int = Field(..., description="等待方在本批中的第几步（从 1 开始）")
    holder_step: int = Field(..., description="持有方在本批中的第几步（从 1 开始）")


class CycleReport(BaseModel):
    cycle_batches: List[str] = Field(..., description="构成循环的批次顺序，首尾相接")
    cycle_orders: List[str] = Field(
        ...,
        description="循环边上的工单号，与 cycle_batches 等长，"
        "cycle_batches[i] 在 cycle_orders[i] 上等待下一个批次",
    )
    edges: List[WaitEdge] = Field(..., description="参与循环的等待边")


class ReplayStep(BaseModel):
    seq: int = Field(..., description="全局执行序号，从 1 开始")
    batch_id: str
    step: int = Field(..., description="批次内第几步，从 1 开始")
    order_id: str
    action: str = Field(..., description="acquire(拿到锁)/block(阻塞等待)/abort(被死锁检测中止)")
    note: Optional[str] = None


class BatchWait(BaseModel):
    batch_id: str
    waits_for_order: str = Field(..., description="该批最终阻塞在哪张工单上")
    waits_for_holder: str = Field(..., description="当前持有该工单行锁的批次")
    last_acquired_step: int = Field(..., description="阻塞前已完成到本批第几步")


class ReplayReport(BaseModel):
    reproduced: bool
    sqlstate: Optional[str] = Field(None, description="复现到的 SQLSTATE，期望 40P01")
    victim_batch: Optional[str] = None
    detail: Optional[str] = None
    steps: List[ReplayStep] = Field(default_factory=list)
    batch_waits: List[BatchWait] = Field(default_factory=list)
    cleanup_ok: bool = Field(..., description="试跑结束后是否无残留事务、无残留改派结果")


class PrecheckResponse(BaseModel):
    has_deadlock: bool
    cycles: List[CycleReport] = Field(default_factory=list)
    wait_edges: List[WaitEdge] = Field(default_factory=list, description="完整等待图")
    replay: Optional[ReplayReport] = None
    message: str

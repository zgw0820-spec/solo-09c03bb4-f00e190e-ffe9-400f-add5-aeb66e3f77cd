"""运行配置，全部来自环境变量，便于容器化。"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # PostgreSQL 连接串（psycopg 关键字参数形式或 dsn 均可）
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/patrol",
    )
    # 试跑时使用的独立 schema，与业务数据隔离，试跑后整体清空
    trial_schema: str = os.getenv("TRIAL_SCHEMA", "deadlock_trial")
    # 单步锁等待上限，避免试跑线程被无限挂住
    lock_timeout_ms: int = int(os.getenv("LOCK_TIMEOUT_MS", "10000"))
    # 试跑整体超时
    trial_timeout_s: float = float(os.getenv("TRIAL_TIMEOUT_S", "30"))
    # 业务校验常量
    min_batches: int = 2
    max_batches: int = 3
    max_orders_per_batch: int = 6


settings = Settings()

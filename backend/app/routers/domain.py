"""序列索引与冷热分层的领域专用接口。

通用资源路由只承载登记/列表/状态流转；涉及原件核验、查询号快照与
冷区幂等写入的业务动作集中在本路由，路由层不做业务判断，全部委派
给领域服务。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from app.services.rollup_window import RollupWindowService
from app.services.series_index import SeriesIndexService
from app.services.tier_storage import TierStorageService

router = APIRouter(prefix="/api/domain", tags=["索引与迁冷领域"])


class PartitionQuery(BaseModel):
    point: str
    event_day: str
    product_type: str | None = None
    include_history: bool = False


class AdmitPayload(BaseModel):
    point: str
    event_days: list[str]
    query_no: str


class EvictPayload(BaseModel):
    point: str
    query_no: str | None = None
    event_days: list[str] | None = None


class CorrectionPayload(BaseModel):
    point: str
    event_day: str
    window_seq: int
    corrected_value: float


@router.post("/rollup/correct")
def append_correction(payload: CorrectionPayload) -> dict[str, Any]:
    return RollupWindowService().append_correction(
        payload.point, payload.event_day, payload.window_seq, payload.corrected_value)


@router.post("/series-index/{index_id}/build")
def build_index(index_id: int) -> dict[str, Any]:
    return SeriesIndexService().build_index(index_id)


@router.post("/series-index/{index_id}/rebuild")
def rebuild_index(index_id: int) -> dict[str, Any]:
    return SeriesIndexService().rebuild_index(index_id)


@router.post("/series-index/query-partition")
def query_partition(payload: PartitionQuery) -> dict[str, Any]:
    return SeriesIndexService().query_partition(
        payload.point, payload.event_day, payload.product_type, payload.include_history)


@router.get("/series-index/query/{query_no}")
def query_by_number(query_no: str) -> dict[str, Any]:
    return SeriesIndexService().query_by_number(query_no)


@router.post("/tier/admit")
def admit_for_cold(payload: AdmitPayload) -> dict[str, Any]:
    return TierStorageService().admit_for_cold(
        payload.point, payload.event_days, payload.query_no)


@router.post("/tier/evict")
def evict_to_cold(payload: EvictPayload) -> dict[str, Any]:
    return TierStorageService().evict_to_cold(
        payload.point, payload.query_no, payload.event_days)


@router.get("/tier/cold/{point}")
def read_cold(point: str, event_day: str, product_type: str | None = None) -> dict[str, Any]:
    return TierStorageService().read_cold(point, event_day, product_type)


@router.get("/tier/query/{query_no}")
def read_cold_by_query(query_no: str) -> dict[str, Any]:
    return TierStorageService().read_cold_by_query(query_no)

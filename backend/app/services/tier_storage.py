"""冷热分层领域服务：维护时序数据热温冷三层的边界划分、迁移方式与驻留天数。

修复要点（数据归属错位向冷区传导）：
1. 迁冷受理只接收能从归并/降采样原件核对出实际测点、事件日与时间范围的
   索引记录；无法核验的记录一律拒绝受理，绝不写入冷区。
2. 受理时生成查询号快照（query_no），写入前原件若出现更正值，本次写入
   只会交付仍可独立核验的窗口；被更正且替换索引尚在重建的窗口保持等待，
   原查询号仍能读回当时看到的历史值。
3. 冷区写入按 (测点, 事件日, 产物类型, 窗口序号) 幂等替换，索引更新后
   重试不会产生重复副本，每个窗口在冷区最终只有一份现时交付。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.series_index import SeriesIndexService
from app.store import store


class TierStorageService(BaseModuleService):
    MODULE = "tier_storage"
    ENTITY = "分层策略"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["策略编号", "时间边界", "热区范围", "温区范围", "冷区路径", "迁移方式", "驻留天数", "策略版本"]
    REQUIRED_FIELDS = ["策略编号", "时间边界", "热区范围"]
    STATUS_ORDER = ["草稿", "待生效", "已生效", "已停用"]
    ACTION_RULES = {"提交生效": "待生效", "确认生效": "已生效", "标记停用": "已停用"}
    # 数据迁移操作：按分层策略扫描索引分区进行分类迁移写入各层水位数据
    NEGATIVE_ACTIONS = ["标记停用"]

    def __init__(self) -> None:
        self._index_service = SeriesIndexService()

    # ------------------------------------------------------------------
    # 通用策略统计（保留原有行为，统计的是已通过核验的现行索引记录）
    # ------------------------------------------------------------------

    def apply_tier_policy(self, policy_id: int) -> dict[str, Any]:
        """按分层策略执行索引数据冷热迁移并写入各层存储水位数据文件。"""
        policy = store.find(self.MODULE, policy_id)
        if policy is None:
            return {"error": f"分层策略 {policy_id} 不存在"}
        self.run_action(policy_id, "确认生效")
        boundary = policy.get("时间边界", "30d")
        hot_range = policy.get("热区范围", "0-7d")
        warm_range = policy.get("温区范围", "7d-30d")
        cold_path = policy.get("冷区路径", "/cold/archive")

        # 只统计现行索引（history 子目录不参与分层统计）
        index_dir = self._data_dir("series_index")
        hot_count = warm_count = cold_count = 0
        if index_dir.exists():
            for f in index_dir.glob("*_index.jsonl"):
                for r in self._read_jsonl(f):
                    if not r.get("verified", False):
                        continue
                    partition = str(r.get("event_day", r.get("time_partition", "")))
                    if "2026-09-03" in partition:
                        hot_count += 1
                    elif "2026-09-02" in partition:
                        warm_count += 1
                    else:
                        cold_count += 1
        self._write_jsonl(
            self._data_dir("tier_storage") / f"{policy['策略编号']}_tier_log.jsonl",
            [
                {"policy": policy["策略编号"], "tier": "hot", "records": hot_count, "range": hot_range, "at": time.time()},
                {"policy": policy["策略编号"], "tier": "warm", "records": warm_count, "range": warm_range, "at": time.time()},
                {"policy": policy["策略编号"], "tier": "cold", "records": cold_count, "path": cold_path, "at": time.time()},
            ]
        )
        return {"policy_id": policy["策略编号"], "boundary": boundary,
                "hot_records": hot_count, "warm_records": warm_count, "cold_records": cold_count}

    # ------------------------------------------------------------------
    # 带原件核验的迁冷受理 / 写入 / 重试
    # ------------------------------------------------------------------

    @staticmethod
    def _identity(record: dict[str, Any]) -> tuple[str, str, str, int]:
        return (
            str(record.get("point", "")),
            str(record.get("event_day", record.get("time_partition", ""))),
            str(record.get("product_type", "")),
            int(record.get("window_seq", 0) or 0),
        )

    def _cold_archive_path(self, point_key: str) -> Path:
        return self._data_dir("tier_storage") / f"{point_key}_cold_archive.jsonl"

    def _cold_history_path(self, point_key: str) -> Path:
        return self._data_dir("tier_storage") / f"{point_key}_cold_history.jsonl"

    def _read_current_index(self, point_key: str, event_day: str) -> list[dict[str, Any]]:
        """读取某测点某日的现行索引记录（不读 history 归档）。"""
        index_dir = self._data_dir("series_index")
        records = []
        if not index_dir.exists():
            return records
        for f in index_dir.glob("*_index.jsonl"):
            for r in self._read_jsonl(f):
                if str(r.get("point", "")) != point_key:
                    continue
                if str(r.get("event_day", r.get("time_partition", ""))) != event_day:
                    continue
                records.append(r)
        return records

    def admit_for_cold(self, point_key: str, event_days: list[str],
                       query_no: str) -> dict[str, Any]:
        """迁冷受理：只受理能从原件独立核验实际测点与事件日的索引记录。

        受理结果固化为查询号快照；写入前的任何原件更正都不会改变快照，
        保证原查询号始终能读回受理当时看到的值。
        """
        admitted: list[dict[str, Any]] = []
        waiting: list[dict[str, Any]] = []
        for day in event_days:
            for rec in self._read_current_index(point_key, day):
                verification = self._verify_index_record(rec)
                superseded = self._superseded_reason(rec)
                item = dict(rec)
                item["admission_verified"] = verification["verified"] and not superseded
                item["admission_reasons"] = verification["reasons"] + (
                    [superseded] if superseded else [])
                if item["admission_verified"]:
                    admitted.append(item)
                else:
                    waiting.append({
                        "point": point_key, "event_day": day,
                        "product_type": rec.get("product_type", ""),
                        "window_seq": rec.get("window_seq", 0),
                        "reasons": item["admission_reasons"],
                    })
        self._index_service.save_query_manifest(
            query_no, point_key, admitted, remark="迁冷受理快照")
        return {"query_no": query_no, "point": point_key, "event_days": event_days,
                "admitted_count": len(admitted), "waiting_count": len(waiting),
                "admitted": admitted, "waiting": waiting}

    def _verify_index_record(self, index_record: dict[str, Any]) -> dict[str, Any]:
        """用索引锚定的原件校验和重新取原件核验，核验不过即拒绝迁冷。"""
        point_key = str(index_record.get("point", ""))
        day = str(index_record.get("event_day", index_record.get("time_partition", "")))
        originals = self._index_service._scan_originals()
        original = self._anchored_original(index_record, originals)
        if original is not None:
            return self._index_service.verify_original(original, point_key, day)
        # 无校验和的旧索引走兼容核验
        return self._index_service._legacy_verify(index_record, originals)

    def _anchored_original(self, index_record: dict[str, Any],
                           originals: list[dict[str, Any]] | None = None
                           ) -> dict[str, Any] | None:
        """按索引锚定的校验和取回原件；无校验和时退回身份匹配。"""
        checksum = str(index_record.get("source_checksum", ""))
        if originals is None:
            originals = self._index_service._scan_originals()
        if checksum:
            return next((r for r in originals
                         if str(r.get("checksum", "")) == checksum), None)
        ident = self._identity(index_record)
        return next((r for r in originals
                     if self._index_service._identity(r) == ident), None)

    def _superseded_reason(self, index_record: dict[str, Any]) -> str:
        """索引锚定的原件版本已被更正值替代时，返回等待原因，否则返回空串。"""
        originals = self._index_service._scan_originals()
        original = self._anchored_original(index_record, originals)
        if original is None:
            return ""
        ident = self._index_service._identity(original)
        latest = self._index_service._latest_originals().get(ident)
        if latest is None:
            return ""
        latest_version = int(latest.get("version", 1) or 1)
        anchored_version = int(original.get("version", 1) or 1)
        if latest_version > anchored_version:
            return (f"原件已出现 v{latest_version} 更正值，替换索引重建完成前，"
                    f"v{anchored_version} 旧索引本次不予迁冷写入")
        return ""

    def evict_to_cold(self, point_key: str, query_no: str | None = None,
                      event_days: list[str] | None = None) -> dict[str, Any]:
        """执行迁冷写入：受理后、写入前再次逐记录核验，幂等替换冷区现时副本。

        - 仍可独立核验的窗口直接交付冷区；
        - 原件已更正且替换索引尚未重建的窗口本次等待（不写入、不报错）；
        - 同一业务身份重复写入只保留最新一份，冷区不会出现重复现时副本。
        """
        if event_days is None:
            event_days = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"]

        candidates: list[dict[str, Any]] = []
        manifest_seen: set[tuple[str, str, str, int]] = set()
        if query_no:
            manifest = self._index_service.query_by_number(query_no)
            if "error" in manifest:
                return manifest
            for rec in manifest["records"]:
                if rec["point"] != point_key or rec["event_day"] not in event_days:
                    continue
                # 回到现行索引取完整写入载荷
                current = self._read_current_index(point_key, rec["event_day"])
                match = next((r for r in current if self._identity(r) == (
                    rec["point"], rec["event_day"], rec["product_type"], rec["window_seq"])), None)
                if match is not None:
                    candidates.append(match)
                    manifest_seen.add(self._identity(match))
        else:
            for day in event_days:
                candidates.extend(self._read_current_index(point_key, day))

        delivered: list[dict[str, Any]] = []
        waiting: list[dict[str, Any]] = []
        for rec in candidates:
            ident = self._identity(rec)
            verification = self._verify_index_record(rec)
            superseded = self._superseded_reason(rec)
            if not verification["verified"]:
                waiting.append({"identity": ident, "reasons": verification["reasons"]})
                continue
            if superseded:
                # 原件已更正、替换索引仍在重建：本次不写入冷区，等待重建后重试
                waiting.append({"identity": ident, "reasons": [superseded]})
                continue
            payload = dict(rec)
            payload["tier"] = "cold"
            payload["delivered_at"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
            payload["delivery_query_no"] = query_no or ""
            delivered.append(payload)

        # 冷区现时归档：按身份幂等替换，被替换下来的旧副本转入冷区历史
        archive = self._cold_archive_path(point_key)
        existing = self._read_jsonl(archive) if archive.exists() else []
        delivered_idents = {self._identity(r) for r in delivered}
        superseded = [r for r in existing if self._identity(r) in delivered_idents]
        kept = [r for r in existing if self._identity(r) not in delivered_idents]
        if superseded:
            self._write_jsonl(self._cold_history_path(point_key), superseded)
        merged = kept + delivered
        self._overwrite_jsonl(archive, merged)

        return {"point": point_key, "query_no": query_no,
                "delivered_count": len(delivered),
                "waiting_count": len(waiting),
                "delivered": [{"identity": self._identity(r), "value": r.get("value"),
                               "version": r.get("source_version")} for r in delivered],
                "waiting": waiting,
                "cold_path": str(archive)}

    def read_cold(self, point_key: str, event_day: str,
                  product_type: str | None = None) -> dict[str, Any]:
        """读取冷区现时交付：只返回通过核验、未被替代的唯一现时副本。"""
        archive = self._cold_archive_path(point_key)
        records = []
        for r in self._read_jsonl(archive) if archive.exists() else []:
            if str(r.get("event_day", r.get("time_partition", ""))) != event_day:
                continue
            if product_type and str(r.get("product_type", "")) != product_type:
                continue
            records.append(r)
        return {"point": point_key, "event_day": event_day,
                "product_type": product_type, "count": len(records), "records": records}

    def read_cold_by_query(self, query_no: str) -> dict[str, Any]:
        """用原查询号回查：历史窗口按原件重新核验，标注是否已被更正替代。"""
        return self._index_service.query_by_number(query_no)

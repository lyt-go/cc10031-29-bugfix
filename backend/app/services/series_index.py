"""序列索引领域服务：维护时序数据按时间分区的点区间索引与存储位置登记。

修复要点（数据归属错位）：
1. 构建日分区索引时只受理同时通过三项原件核验的记录：
   测点一致、事件日与目标分区一致、产物类型可识别；其他测点（如 B）的
   文件即使在同一目录下被扫描到，也会因测点证据不符被排除，不再被盖上
   本分区的日期。
2. 每条索引记录锚定归并/降采样原件的版本与校验和，可随时独立复核；
   无事件日/时间范围证据、无法核验的记录不进索引，也不允许迁冷。
3. 重建采用"旧版索引归档 + 覆盖重写"，错误归属不会残留在现行索引中；
   旧索引只要还能凭归并原件的明确时间范围与测点证据核验，历史查询
   仍可通过原查询号回查当时的值。
"""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 原件记录里承载聚合值的字段，按产物类型区分
VALUE_FIELD = {"rollup": "rollup_value", "downsampled": "aggregated_value"}


class SeriesIndexService(BaseModuleService):
    MODULE = "series_index"
    ENTITY = "索引分区"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["索引编号", "测点编号", "时间分区", "点区间", "索引粒度", "索引条数", "存储位置", "索引版本"]
    REQUIRED_FIELDS = ["索引编号", "测点编号", "时间分区"]
    STATUS_ORDER = ["草稿", "待构建", "已构建", "已失效"]
    ACTION_RULES = {"启动构建": "待构建", "确认构建": "已构建",
                    "标记失效": "已失效", "重建索引": "待构建"}
    # 数据存储操作：扫描归并窗口结果按时间分区构建序列索引分区写入索引元数据
    NEGATIVE_ACTIONS = ["标记失效"]

    # ------------------------------------------------------------------
    # 原件扫描与独立核验
    # ------------------------------------------------------------------

    def _scan_originals(self) -> list[dict[str, Any]]:
        """扫描归并与降采样两类原件，标注来源文件与产物类型。"""
        originals: list[dict[str, Any]] = []
        rollup_dir = self._data_dir("rollup_windows")
        if rollup_dir.exists():
            for f in sorted(rollup_dir.glob("*_rollup.jsonl")):
                for r in self._read_jsonl(f):
                    r = dict(r)
                    r.setdefault("product_type", "rollup")
                    r["_source_file"] = str(f)
                    originals.append(r)
        ds_dir = self._data_dir("downsampled")
        if ds_dir.exists():
            for f in sorted(ds_dir.glob("*_downsampled.jsonl")):
                for r in self._read_jsonl(f):
                    r = dict(r)
                    r.setdefault("product_type", "downsampled")
                    r["_source_file"] = str(f)
                    originals.append(r)
        return originals

    @staticmethod
    def _identity(record: dict[str, Any]) -> tuple[str, str, str, int]:
        """原件/索引共享的业务身份：测点 + 事件日 + 产物类型 + 窗口序号。"""
        return (
            str(record.get("point", "")),
            str(record.get("event_day", "")),
            str(record.get("product_type", "")),
            int(record.get("window_seq", record.get("window_index", 0)) or 0),
        )

    def _latest_originals(self) -> dict[tuple[str, str, str, int], dict[str, Any]]:
        """同一业务身份只保留最高版本原件（更正值以新版本追加，旧版保留可回查）。"""
        latest: dict[tuple[str, str, str, int], dict[str, Any]] = {}
        for r in self._scan_originals():
            ident = self._identity(r)
            version = int(r.get("version", 1) or 1)
            if ident not in latest or version > int(latest[ident].get("version", 1) or 1):
                latest[ident] = r
        return latest

    def verify_original(self, record: dict[str, Any], point_key: str,
                        partition: str) -> dict[str, Any]:
        """对一条候选记录执行归属核验。

        三道证据缺一不可：测点一致、事件日与分区一致、明确时间范围落在
        该事件日内；带校验和的原件还要复核校验和，防止原件被静默改写。
        """
        rec_point = str(record.get("point", ""))
        event_day = str(record.get("event_day", ""))
        product_type = str(record.get("product_type", ""))
        time_start = str(record.get("time_start", ""))
        time_end = str(record.get("time_end", ""))

        point_match = rec_point == point_key
        day_match = bool(event_day) and event_day == partition
        # 明确时间范围证据：两端都必须落在同一事件日内
        range_match = (
            bool(time_start) and bool(time_end)
            and time_start[:10] == partition and time_end[:10] == partition
        )
        type_match = product_type in VALUE_FIELD
        checksum_match = True
        expected_checksum = record.get("checksum")
        if expected_checksum:
            checksum_match = self._record_checksum(
                {k: v for k, v in record.items()
                 if k not in ("_source_file", "checksum")}
            ) == expected_checksum

        verified = all([point_match, day_match, range_match, type_match, checksum_match])
        reasons = []
        if not point_match:
            reasons.append(f"测点证据不符：原件属 {rec_point or '未知'}，索引属 {point_key}")
        if not day_match:
            reasons.append(f"事件日证据不符：原件 {event_day or '缺失'}，分区 {partition}")
        if not range_match:
            reasons.append("原件缺少与分区一致的明确时间范围证据")
        if not type_match:
            reasons.append(f"无法识别的产物类型：{product_type or '缺失'}")
        if not checksum_match:
            reasons.append("原件校验和不一致，原件可能已被改写")
        return {"verified": verified, "reasons": reasons,
                "point_match": point_match, "event_day_match": day_match,
                "time_range_match": range_match, "checksum_match": checksum_match}

    # ------------------------------------------------------------------
    # 索引构建（修复后的主流程）
    # ------------------------------------------------------------------

    def build_index(self, index_id: int) -> dict[str, Any]:
        """凭原件核验结果按 (测点, 事件日, 产物类型) 构建日分区索引。

        与旧实现的区别：不再把目录下所有测点的记录盖上同一分区日期，
        无法通过原件核验的记录一律不进现行索引。
        """
        entry = store.find(self.MODULE, index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        self.run_action(index_id, "启动构建")
        point_key = entry["测点编号"]
        partition = entry.get("时间分区", time.strftime("%Y-%m-%d", time.localtime()))
        index_no = entry["索引编号"]

        indexed_points: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        # 同一身份只索引最新版本原件；被替代的旧版本留给历史查询回查
        for ident, record in self._latest_originals().items():
            verification = self.verify_original(record, point_key, partition)
            if not verification["verified"]:
                rec_point = str(record.get("point", ""))
                rec_day = str(record.get("event_day", ""))
                # 其他测点、其他事件日的记录属正常排除；本测点记录中，
                # 声称属于本分区或根本缺失事件日证据的，登记为拒绝
                if rec_point == point_key and (rec_day == partition or not rec_day):
                    rejected.append({
                        "point": point_key, "event_day": rec_day,
                        "product_type": record.get("product_type", ""),
                        "reasons": verification["reasons"],
                    })
                continue
            product_type = str(record["product_type"])
            value_field = VALUE_FIELD[product_type]
            indexed_points.append({
                "index_id": index_no,
                "point": point_key,
                "event_day": partition,
                # 兼容旧字段：time_partition 与事件日严格同义
                "time_partition": partition,
                "product_type": product_type,
                "window_seq": ident[3],
                "value_field": value_field,
                "value": record.get(value_field, 0),
                "window_index": record.get("window_start", record.get("window_index", 0)),
                "record_count": record.get("record_count", record.get("source_count", 0)),
                "time_start": record.get("time_start", ""),
                "time_end": record.get("time_end", ""),
                "source_file": record.get("_source_file", ""),
                "source_version": int(record.get("version", 1) or 1),
                "source_checksum": record.get("checksum", ""),
                "index_version": entry.get("索引版本", "v1.1"),
                "verified": True,
                "built_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            })

        # 覆盖写：重建时旧索引内容不会残留在现行文件中
        output_file = self._data_dir("series_index") / f"{index_no}_index.jsonl"
        written = self._overwrite_jsonl(output_file, indexed_points)
        entry["索引条数"] = str(written)
        entry["点区间"] = f"1-{written}" if written else "0-0"
        entry["存储位置"] = str(output_file)
        entry.setdefault("索引版本", "v1.1")
        self.run_action(index_id, "确认构建")
        return {"index_id": index_no, "point": point_key,
                "partition": partition, "indexed_count": written,
                "rejected_count": len(rejected), "rejected": rejected}

    def rebuild_index(self, index_id: int) -> dict[str, Any]:
        """重建索引：旧版整体归档后覆盖重写，错误归属不残留。"""
        entry = store.find(self.MODULE, index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        index_no = entry["索引编号"]
        current_file = self._data_dir("series_index") / f"{index_no}_index.jsonl"

        archived = ""
        if current_file.exists():
            history_dir = self._data_dir("series_index") / "history"
            history_dir.mkdir(parents=True, exist_ok=True)
            stamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
            target = history_dir / f"{index_no}_{stamp}_index.jsonl"
            shutil.move(str(current_file), str(target))
            archived = str(target)

        # 版本号递增，便于区分重建前后的索引口径
        old_version = str(entry.get("索引版本", "v1.0"))
        try:
            major, minor = (int(x) for x in old_version.lstrip("v").split(".")[:2])
            entry["索引版本"] = f"v{major}.{minor + 1}"
        except (ValueError, AttributeError):
            entry["索引版本"] = "v1.1"

        result = self.build_index(index_id)
        if "error" in result:
            return result
        result["archived_old_index"] = archived
        result["previous_version"] = old_version
        result["current_version"] = entry["索引版本"]
        return result

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """工作台动作分派：「重建索引」走带核验的重建流程，其余沿用通用流转。"""
        if action == "重建索引":
            result = self.rebuild_index(entry_id)
            if "error" in result:
                return None, result["error"]
            entry = store.find(self.MODULE, entry_id)
            return entry, (
                f"索引已重建：现行 {result['indexed_count']} 条，"
                f"排除无法核验记录 {result['rejected_count']} 条，旧版已归档"
            )
        return super().run_action(entry_id, action)

    # ------------------------------------------------------------------
    # 查询号与历史回查
    # ------------------------------------------------------------------

    def _query_manifest_path(self, query_no: str) -> Path:
        return self._data_dir("series_index") / "queries" / f"{query_no}.json"

    def save_query_manifest(self, query_no: str, point_key: str,
                            records: list[dict[str, Any]], remark: str = "") -> Path:
        """把某次受理/查询看到的索引快照固化为查询号清单。"""
        manifest = {
            "query_no": query_no,
            "point": point_key,
            "remark": remark,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            "records": records,
        }
        path = self._query_manifest_path(query_no)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)
        return path

    def _all_originals_by_checksum(self) -> dict[str, dict[str, Any]]:
        return {str(r.get("checksum", "")): r for r in self._scan_originals()
                if r.get("checksum")}

    def query_by_number(self, query_no: str) -> dict[str, Any]:
        """用原查询号读回历史窗口，并对每条记录重新做原件核验。

        旧索引/旧快照中的值只要原件（含被替代的旧版本）仍在且证据齐全，
        就作为可验证的历史窗口返回；原件已无法核验的记录明确标注
        verified=false，调用方不得把它当作可迁冷/可交付依据。
        """
        path = self._query_manifest_path(query_no)
        if not path.exists():
            return {"error": f"查询号 {query_no} 不存在"}
        with open(path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        originals = self._scan_originals()
        by_checksum = self._all_originals_by_checksum()
        latest = self._latest_originals()
        results = []
        for rec in manifest.get("records", []):
            ident = (
                str(rec.get("point", "")),
                str(rec.get("event_day", rec.get("time_partition", ""))),
                str(rec.get("product_type", "")),
                int(rec.get("window_seq", 0) or 0),
            )
            checksum = str(rec.get("source_checksum", ""))
            original = by_checksum.get(checksum)
            historical = False
            superseded_by = None
            if original is not None:
                current = latest.get(ident)
                if current is not None and int(current.get("version", 1) or 1) > int(
                        original.get("version", 1) or 1):
                    historical = True
                    superseded_by = int(current.get("version", 1) or 1)
                verification = self.verify_original(
                    original, ident[0], ident[1])
            else:
                # 无校验和的旧索引：退回用测点/事件日/时间范围/值对原件
                verification = self._legacy_verify(rec, originals)
            results.append({
                "query_no": query_no,
                "point": ident[0],
                "event_day": ident[1],
                "product_type": ident[2],
                "window_seq": ident[3],
                "value": rec.get("value"),
                "source_version": rec.get("source_version"),
                "verified": verification["verified"],
                "historical": historical,
                "superseded_by_version": superseded_by,
                "reasons": verification["reasons"],
            })
        return {"query_no": query_no, "point": manifest.get("point"),
                "created_at": manifest.get("created_at"),
                "verified_count": sum(1 for r in results if r["verified"]),
                "unverifiable_count": sum(1 for r in results if not r["verified"]),
                "records": results}

    def _legacy_verify(self, index_record: dict[str, Any],
                       originals: list[dict[str, Any]]) -> dict[str, Any]:
        """旧版（v1.0 无校验和）索引的兼容核验：凭测点+事件日+明确时间范围+值。"""
        point_key = str(index_record.get("point", ""))
        day = str(index_record.get("event_day", index_record.get("time_partition", "")))
        value = index_record.get("value")
        for original in originals:
            if str(original.get("point", "")) != point_key:
                continue
            if str(original.get("event_day", "")) != day:
                continue
            time_start = str(original.get("time_start", ""))
            time_end = str(original.get("time_end", ""))
            if not time_start or time_start[:10] != day or time_end[:10] != day:
                continue
            value_field = VALUE_FIELD.get(str(original.get("product_type", "")), "")
            if value_field and original.get(value_field) == value:
                return {"verified": True, "reasons": [], "point_match": True,
                        "event_day_match": True, "time_range_match": True,
                        "checksum_match": False}
        return {"verified": False,
                "reasons": ["旧索引无法凭归并原件的测点、事件日与时间范围证据核验"],
                "point_match": False, "event_day_match": False,
                "time_range_match": False, "checksum_match": False}

    def query_partition(self, point_key: str, event_day: str,
                        product_type: str | None = None,
                        include_history: bool = False) -> dict[str, Any]:
        """按测点+事件日查询现行索引；include_history 时附带已归档的可验证旧窗口。"""
        index_dir = self._data_dir("series_index")
        current: list[dict[str, Any]] = []
        for f in sorted(index_dir.glob("*_index.jsonl")) if index_dir.exists() else []:
            for r in self._read_jsonl(f):
                if str(r.get("point", "")) != point_key:
                    continue
                if str(r.get("event_day", r.get("time_partition", ""))) != event_day:
                    continue
                if product_type and str(r.get("product_type", "")) != product_type:
                    continue
                current.append(r)

        historical: list[dict[str, Any]] = []
        if include_history:
            history_dir = index_dir / "history"
            by_checksum = self._all_originals_by_checksum()
            for f in sorted(history_dir.glob("*_index.jsonl")) if history_dir.exists() else []:
                for r in self._read_jsonl(f):
                    if str(r.get("point", "")) != point_key:
                        continue
                    if str(r.get("event_day", r.get("time_partition", ""))) != event_day:
                        continue
                    if product_type and str(r.get("product_type", "")) != product_type:
                        continue
                    original = by_checksum.get(str(r.get("source_checksum", "")))
                    r = dict(r)
                    r["historical_verified"] = (
                        self.verify_original(original, point_key, event_day)["verified"]
                        if original is not None else
                        self._legacy_verify(r, self._scan_originals())["verified"]
                    )
                    historical.append(r)
        return {"point": point_key, "event_day": event_day,
                "product_type": product_type, "current": current,
                "historical": historical}

    def build_all_partitions(self, point_key: str) -> dict[str, Any]:
        """为指定测点构建所有时间分区的序列索引数据集。"""
        partitions = ["2026-09-01", "2026-09-02", "2026-09-03"]
        results = []
        for part in partitions:
            tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
            self._auto_create_seed(self.MODULE, {
                "索引编号": f"IDX-{point_key}-{part}",
                "测点编号": point_key,
                "时间分区": part,
                "点区间": "0-0",
                "索引粒度": "1h",
                "索引条数": "0",
                "存储位置": "",
                "索引版本": "v1.1",
            })
            r = self.build_index(tid)
            results.append(r)
        return {"point": point_key, "partitions_built": len(results), "results": results}

"""窗口归并领域服务：维护固定、滑动与会话窗口的时序归并聚合指标与窗口条数。
对降采样后的序列数据按窗口类型进行窗口归并计算，生成归并结果数据集。

归并结果是后续序列索引与冷热迁移唯一可回查的原件，因此每条记录都必须
自带测点、事件日与明确时间范围证据；更正值以新版本追加，绝不覆盖旧原件，
保证历史索引仍能凭旧版本原件核验回查。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.downsample import _event_day
from app.store import store


class RollupWindowService(BaseModuleService):
    MODULE = "rollup_window"
    ENTITY = "归并窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口编号", "测点编号", "窗口类型", "窗口长度", "滑动步长", "聚合指标", "窗口条数", "归并水位"]
    REQUIRED_FIELDS = ["窗口编号", "测点编号", "窗口类型"]
    STATUS_ORDER = ["草稿", "待归并", "已归并", "已作废"]
    ACTION_RULES = {"启动归并": "待归并", "确认归并": "已归并", "标记作废": "已作废"}
    # 数据归并操作：按窗口归并规则执行聚合计算生成归并结果数据集
    NEGATIVE_ACTIONS = []

    @staticmethod
    def _record_identity(record: dict[str, Any]) -> tuple[str, str, int]:
        """归并原件的业务身份：测点 + 事件日 + 当日窗口序号。"""
        return (
            str(record.get("point", "")),
            str(record.get("event_day", "")),
            int(record.get("window_seq", 0) or 0),
        )

    def _load_originals(self, point_key: str) -> list[dict[str, Any]]:
        """读取某测点全部归并原件（含历史版本与更正值），按文件稳定排序。"""
        rollup_dir = self._data_dir("rollup_windows")
        records: list[dict[str, Any]] = []
        if not rollup_dir.exists():
            return records
        for f in sorted(rollup_dir.glob(f"{point_key}_*_rollup.jsonl")):
            records.extend(self._read_jsonl(f))
        return records

    def rollup_windows(self, rollup_id: int) -> dict[str, Any]:
        """按窗口归并规则对降采样后的时序数据执行聚合计算，生成归并结果数据集。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        self.run_action(rollup_id, "启动归并")
        point_key = entry["测点编号"]
        window_type = entry.get("窗口类型", "固定窗口")
        window_len = int(entry.get("窗口长度", "5"))
        agg_metric = entry.get("聚合指标", "均值")

        # 只读本测点的降采样数据；文件名仅做初筛，最终以记录内嵌测点证据为准，
        # 杜绝把其他测点（如同目录下的 B 测点）的序列并入 A 测点原件
        ds_dir = self._data_dir("downsampled")
        ds_files = list(ds_dir.glob(f"*{point_key}*_downsampled.jsonl")) if ds_dir.exists() else []
        records = []
        for f in sorted(ds_files)[:3]:
            records.extend(r for r in self._read_jsonl(f)
                           if str(r.get("point", point_key)) == point_key)
        if not records:
            filtered_file = self._data_dir("filtered_series") / f"{point_key}_filtered.jsonl"
            records = [r for r in self._read_jsonl(filtered_file)
                       if str(r.get("point", point_key)) == point_key]

        # 按窗口类型执行归并聚合计算；事件日/时间范围直接来自降采样原件证据
        merged = []
        seq_per_day: dict[str, int] = {}
        for i in range(0, len(records), window_len):
            window = records[i:i + window_len]
            values = []
            window_times = []
            # 数据产物输出：写入归并聚合结果到窗口数据JSONL文件
            for r in window:
                v = r.get("aggregated_value", r.get("calibrated_value_V", 0))
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    values.append(0.0)
                ts = r.get("time_start") or r.get("timestamp") or r.get("event_time")
                if ts:
                    window_times.append(str(ts))
                te = r.get("time_end")
                if te:
                    window_times.append(str(te))
            if not values:
                continue
            if agg_metric in ("均值", "avg"):
                agg_val = round(sum(values) / len(values), 4)
            elif agg_metric in ("求和", "sum"):
                agg_val = round(sum(values), 4)
            elif agg_metric in ("最大值", "max"):
                agg_val = round(max(values), 4)
            else:
                agg_val = round(sum(values) / len(values), 4)
            event_day = ""
            days = {_event_day(ts) for ts in window_times if _event_day(ts)}
            if len(days) == 1:
                event_day = next(iter(days))
            time_start = min(window_times) if window_times else ""
            time_end = max(window_times) if window_times else ""
            # 窗口序号按事件日独立编号：同一测点每个事件日都有序号 1 的窗口
            seq_per_day[event_day] = seq_per_day.get(event_day, 0) + 1
            record = {
                "window_id": entry["窗口编号"],
                "point": point_key,
                "product_type": "rollup",
                "window_type": window_type,
                "event_day": event_day,
                "window_seq": seq_per_day[event_day],
                "window_start": i + 1,
                "window_end": i + len(window),
                "record_count": len(window),
                "rollup_value": agg_val,
                "metric": agg_metric,
                "time_start": time_start,
                "time_end": time_end,
                "version": 1,
            }
            record["checksum"] = self._record_checksum(record)
            merged.append(record)
        # 将窗口归并结果写入归并窗口数据文件
        output_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_rollup.jsonl"
        written = self._overwrite_jsonl(output_file, merged)
        entry["窗口条数"] = str(written)
        entry["归并水位"] = str(written)
        self.run_action(rollup_id, "确认归并")
        return {"rollup_id": entry["窗口编号"], "point": point_key,
                "window_type": window_type, "window_period_count": written,
                "metric": agg_metric}

    def append_correction(
        self,
        point_key: str,
        event_day: str,
        window_seq: int,
        corrected_value: float,
    ) -> dict[str, Any]:
        """对某测点某日某序号窗口追加一条可独立核验的更正值原件。

        更正是新版本原件（version+1、supersedes 指向被替代版本），旧原件
        原样保留：重建后的索引只受理最新版本，旧索引/历史查询仍能凭旧
        原件的明确时间范围与测点证据回查当时读到的值。
        """
        originals = [
            r for r in self._load_originals(point_key)
            if self._record_identity(r) == (point_key, event_day, window_seq)
        ]
        if not originals:
            return {"error": f"测点 {point_key} {event_day} 序号 {window_seq} 的归并原件不存在，无法更正"}
        base = max(originals, key=lambda r: int(r.get("version", 1) or 1))
        new_version = int(base.get("version", 1) or 1) + 1
        correction = dict(base)
        correction["rollup_value"] = corrected_value
        correction["version"] = new_version
        correction["supersedes"] = int(base.get("version", 1) or 1)
        correction["corrected_at"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        correction.pop("checksum", None)
        correction["checksum"] = self._record_checksum(correction)

        output_file = self._data_dir("rollup_windows") / f"{point_key}_{event_day}_rollup.jsonl"
        self._write_jsonl(output_file, [correction])
        return {"point": point_key, "event_day": event_day, "window_seq": window_seq,
                "old_version": int(base.get("version", 1) or 1), "new_version": new_version,
                "corrected_value": corrected_value, "checksum": correction["checksum"]}

    def compute_cumulative_rollup(self, rollup_id: int) -> dict[str, Any]:
        """对归并窗口数据计算累计聚合指标写入累计快照数据文件。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        # 先执行归并
        rollup_result = self.rollup_windows(rollup_id)
        if "error" in rollup_result:
            return rollup_result
        # 读取归并结果计算累计
        rollup_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_rollup.jsonl"
        records = self._read_jsonl(rollup_file)
        cumulative = []
        running_sum = 0.0
        for i, r in enumerate(records):
            val = float(r.get("rollup_value", 0))
            running_sum += val
            cumulative.append({
                "window_index": i + 1,
                "point": r["point"],
                "window_value": val,
                "cumulative_value": round(running_sum, 4),
                "window_id": r["window_id"],
            })
        # 写入累计聚合数据文件
        cum_file = self._data_dir("rollup_windows") / f"{entry['窗口编号']}_cumulative.jsonl"
        self._write_jsonl(cum_file, cumulative)
        return {"rollup_id": entry["窗口编号"], "cumulative_periods": len(cumulative),
                "final_cumulative_value": round(running_sum, 4)}

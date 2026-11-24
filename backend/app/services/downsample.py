"""降采样领域服务：维护高频时序到目标频率的降采样聚合方式与压缩比产出。
按聚合方式对时序数据进行降采样计算，写入降采样结果序列数据集。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _event_day(timestamp: str) -> str:
    """从 ISO 时间字符串取事件日（YYYY-MM-DD）；无法解析时返回空串。"""
    if not timestamp:
        return ""
    head = str(timestamp).strip().replace(" ", "T")
    if len(head) >= 10 and head[4] == "-" and head[7] == "-":
        day = head[:10]
        try:
            time.strptime(day, "%Y-%m-%d")
            return day
        except ValueError:
            return ""
    return ""


class DownsampleService(BaseModuleService):
    MODULE = "downsample"
    ENTITY = "降采样任务"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["任务编号", "源序列", "源采样频率", "目标频率", "聚合方式", "输出点数", "压缩比", "任务水位"]
    REQUIRED_FIELDS = ["任务编号", "源序列", "源采样频率"]
    STATUS_ORDER = ["草稿", "待执行", "执行中", "已完成"]
    ACTION_RULES = {"启动执行": "执行中", "标记完成": "已完成", "标记失败": "已完成"}
    NEGATIVE_ACTIONS = []

    @staticmethod
    def _event_day_from_times(timestamps: list[str]) -> str:
        """窗口内时间戳同属一个事件日时返回该日；跨日或缺证据时返回空串。"""
        days = {_event_day(ts) for ts in timestamps if _event_day(ts)}
        return next(iter(days)) if len(days) == 1 else ""

    # 数据聚合操作：按窗口执行降采样聚合计算生成降采样结果序列数据集
    def downsample_series(self, task_id: int, source_data_file: str | None = None) -> dict[str, Any]:
        """按聚合方式对高频时序数据进行降采样计算生成降采样结果数据集。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"error": f"降采样任务 {task_id} 不存在"}
        entry, _ = self.run_action(task_id, "启动执行")
        point_key = entry.get("源序列", entry["任务编号"])
        agg_method = entry.get("聚合方式", "均值")

        # 读取过滤后的序列数据
        filtered_file = self._data_dir("filtered_series") / f"{point_key}_filtered.jsonl"
        records = self._read_jsonl(filtered_file)
        if not records:
            cal_file = self._data_dir("calibrated") / f"{point_key}_calibrated.jsonl"
            records = self._read_jsonl(cal_file)
        if not records:
            edge_file = self._data_dir("edge_samples") / f"{point_key}.jsonl"
            records = self._read_jsonl(edge_file)

        # 按窗口执行降采样聚合计算
        source_freq = float(entry.get("源采样频率", "10").replace("Hz", ""))
        target_freq = float(entry.get("目标频率", "1").replace("Hz", ""))
        window_size = max(1, int(source_freq / target_freq))
        downsampled = []
        for i in range(0, len(records), window_size):
            window = records[i:i + window_size]
            values = []
            window_times = []
            for r in window:
                # 数据产物输出：将降采样结果写入降采样序列数据JSONL文件
                v = r.get("calibrated_value_V", r.get("raw_value_mV", r.get("raw_value", 0)))
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    values.append(0.0)
                ts = r.get("timestamp") or r.get("event_time")
                if ts:
                    window_times.append(str(ts))
            if not values:
                continue
            if agg_method in ("均值", "avg"):
                agg_val = round(sum(values) / len(values), 4)
            elif agg_method in ("最大值", "max"):
                agg_val = round(max(values), 4)
            elif agg_method in ("最小值", "min"):
                agg_val = round(min(values), 4)
            else:
                agg_val = round(sum(values) / len(values), 4)
            # 事件日与明确时间范围是后续序列索引核验测点/事件日归属的原件证据，
            # 没有时间证据的窗口不能被任何日分区索引受理。
            event_day = self._event_day_from_times(window_times)
            time_start = min(window_times) if window_times else ""
            time_end = max(window_times) if window_times else ""
            downsampled.append({
                "task_id": entry["任务编号"],
                "point": point_key,
                "product_type": "downsampled",
                "window_index": len(downsampled) + 1,
                "source_count": len(window),
                "aggregated_value": agg_val,
                "method": agg_method,
                "target_freq": target_freq,
                "event_day": event_day,
                "time_start": time_start,
                "time_end": time_end,
            })
        # 将降采样结果写入降采样序列数据文件
        output_file = self._data_dir("downsampled") / f"{entry['任务编号']}_downsampled.jsonl"
        written = self._write_jsonl(output_file, downsampled)
        compression_ratio = round(len(records) / max(written, 1), 2)
        entry["输出点数"] = str(written)
        entry["压缩比"] = str(compression_ratio)
        entry["任务水位"] = str(written)
        self.run_action(task_id, "标记完成")
        return {"task_id": entry["任务编号"], "point": point_key,
                "input_records": len(records), "output_points": written,
                "compression_ratio": compression_ratio, "method": agg_method}

    def batch_downsample(self, point_keys: list[str], agg_method: str = "均值") -> dict[str, Any]:
        """批量对多个测点的时序数据执行降采样聚合转换。"""
        results = []
        for pk in point_keys:
            tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
            self._auto_create_seed(self.MODULE, {
                "任务编号": f"DOWN-{pk}-{tid}",
                "源序列": pk,
                "源采样频率": "10Hz",
                "目标频率": "1Hz",
                "聚合方式": agg_method,
                "输出点数": "0",
                "压缩比": "0",
                "任务水位": "0",
            })
            r = self.downsample_series(tid)
            results.append(r)
        return {"total_points": len(point_keys), "executed": len(results), "results": results}
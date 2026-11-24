"""序列索引归属错位修复的端到端场景测试。

叙事主线（A/B 两个测点）：
- A 的 3 日、4 日各有一个序号 1 的归并窗口，原件值分别为 20、30；
- B 的记录保留在 B 自己的原件里，A 的日分区索引不得扫描受理；
- 迁冷受理后写入前，A 的 4 日原件出现更正值 31，替换索引仍在重建：
  3 日独立交付、4 日等待重建，原查询号仍能读回 30；
- 索引更新后重试，冷区最终只有一份 A 的 4 日现时交付 31。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from app.services.rollup_window import RollupWindowService
from app.services.series_index import SeriesIndexService
from app.services.tier_storage import TierStorageService
from app.store import store

DAY3 = "2026-09-03"
DAY4 = "2026-09-04"


@pytest.fixture(autouse=True)
def setup_data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            import app.services.rollup_window as rw
            import app.services.series_index as si
            import app.services.tier_storage as ts
            for mod in (rw, si, ts):
                mod.DATA_ROOT = Path(tmp)
            yield Path(tmp)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _rollup_record(svc, point, day, seq, value, version=1, **extra):
    record = {
        "window_id": f"ROLL-{point}",
        "point": point,
        "product_type": "rollup",
        "window_type": "固定窗口",
        "event_day": day,
        "window_seq": seq,
        "window_start": seq,
        "window_end": seq,
        "record_count": 1,
        "rollup_value": value,
        "metric": "均值",
        "time_start": f"{day}T00:00:00",
        "time_end": f"{day}T23:59:59",
        "version": version,
    }
    record.update(extra)
    record["checksum"] = svc._record_checksum(record)
    return record


def _downsample_record(svc, point, day, seq, value):
    record = {
        "task_id": f"DOWN-{point}",
        "point": point,
        "product_type": "downsampled",
        "window_index": seq,
        "source_count": 10,
        "aggregated_value": value,
        "method": "均值",
        "target_freq": 1.0,
        "event_day": day,
        "time_start": f"{day}T00:00:00",
        "time_end": f"{day}T00:59:59",
        "version": 1,
    }
    record["checksum"] = svc._record_checksum(record)
    return record


def _seed_index(index_id, index_no, point, day):
    store.rows("series_index").append({
        "id": index_id, "索引编号": index_no, "测点编号": point,
        "时间分区": day, "点区间": "0-0", "索引粒度": "1h",
        "索引条数": "0", "存储位置": "", "索引版本": "v1.1",
        "status": "待构建", "pending": True, "abnormal": False,
    })


def _prepare_ab_originals(data_root: Path):
    """A、B 两个测点各一份归并与降采样原件。"""
    rw_svc = RollupWindowService()
    si_svc = SeriesIndexService()
    # A：3 日值 20、4 日值 30，均为当日序号 1 的窗口
    a_rollup = [
        _rollup_record(rw_svc, "A", DAY3, 1, 20),
        _rollup_record(rw_svc, "A", DAY4, 1, 30),
    ]
    b_rollup = [
        _rollup_record(rw_svc, "B", DAY3, 1, 200),
        _rollup_record(rw_svc, "B", DAY4, 1, 300),
    ]
    _write_jsonl(data_root / "rollup_windows" / "A_main_rollup.jsonl", a_rollup)
    _write_jsonl(data_root / "rollup_windows" / "B_main_rollup.jsonl", b_rollup)
    # 降采样产物各一份
    _write_jsonl(data_root / "downsampled" / "DOWN-A_downsampled.jsonl", [
        _downsample_record(si_svc, "A", DAY3, 1, 20.0),
        _downsample_record(si_svc, "A", DAY4, 1, 30.0),
    ])
    _write_jsonl(data_root / "downsampled" / "DOWN-B_downsampled.jsonl", [
        _downsample_record(si_svc, "B", DAY4, 1, 300.0),
    ])
    return a_rollup, b_rollup


class TestIndexOwnershipCorrection:
    """新索引必须区分测点、事件日和产物类型。"""

    def test_partition_scan_does_not_absorb_other_point(self, setup_data_dir):
        data_root = setup_data_dir
        _prepare_ab_originals(data_root)
        si_svc = SeriesIndexService()
        _seed_index(301, "IDX-A-0904", "A", DAY4)

        result = si_svc.build_index(301)
        # A 的 4 日：归并 1 条 + 降采样 1 条；B 的任何记录都不得混入
        assert result["indexed_count"] == 2
        assert result["rejected_count"] == 0
        idx_file = data_root / "series_index" / "IDX-A-0904_index.jsonl"
        rows = si_svc._read_jsonl(idx_file)
        assert {r["point"] for r in rows} == {"A"}
        assert {r["event_day"] for r in rows} == {DAY4}
        assert {r["product_type"] for r in rows} == {"rollup", "downsampled"}
        assert all(r["verified"] is True for r in rows)
        # 每条索引都锚定了可独立复核的原件版本与校验和
        assert all(r["source_checksum"] and r["source_version"] == 1 for r in rows)

    def test_each_day_has_its_own_seq_one_window(self, setup_data_dir):
        data_root = setup_data_dir
        _prepare_ab_originals(data_root)
        si_svc = SeriesIndexService()
        _seed_index(302, "IDX-A-0903", "A", DAY3)
        _seed_index(303, "IDX-A-0904", "A", DAY4)

        r3 = si_svc.build_index(302)
        r4 = si_svc.build_index(303)
        assert r3["indexed_count"] == 2
        assert r4["indexed_count"] == 2

        q3 = si_svc.query_partition("A", DAY3, "rollup")
        q4 = si_svc.query_partition("A", DAY4, "rollup")
        w3, w4 = q3["current"][0], q4["current"][0]
        # 两个事件日各自有序号 1 的窗口，值分别可核对为 20 和 30
        assert w3["window_seq"] == 1 and w3["value"] == 20
        assert w4["window_seq"] == 1 and w4["value"] == 30

    def test_b_point_keeps_its_own_index(self, setup_data_dir):
        data_root = setup_data_dir
        _prepare_ab_originals(data_root)
        si_svc = SeriesIndexService()
        _seed_index(304, "IDX-B-0904", "B", DAY4)

        result = si_svc.build_index(304)
        rows = si_svc._read_jsonl(
            data_root / "series_index" / "IDX-B-0904_index.jsonl")
        assert result["indexed_count"] == 2  # B 的归并 + B 的降采样
        assert {r["point"] for r in rows} == {"B"}
        assert {r["value"] for r in rows} == {300, 300.0}

    def test_unverifiable_record_is_rejected_and_not_cold_eligible(self, setup_data_dir):
        data_root = setup_data_dir
        rw_svc = RollupWindowService()
        # A 的 4 日多一条缺少事件日与时间范围证据的原件
        good = _rollup_record(rw_svc, "A", DAY4, 1, 30)
        bad = _rollup_record(rw_svc, "A", DAY4, 2, 99, event_day="", time_start="", time_end="")
        _write_jsonl(data_root / "rollup_windows" / "A_main_rollup.jsonl", [good, bad])

        si_svc = SeriesIndexService()
        _seed_index(305, "IDX-A-BAD", "A", DAY4)
        result = si_svc.build_index(305)
        assert result["indexed_count"] == 1
        assert result["rejected_count"] == 1
        assert "事件日证据不符" in result["rejected"][0]["reasons"][0]

        # 无法核验的记录不进索引，自然也不会被当作允许迁冷的记录
        tier_svc = TierStorageService()
        admit = tier_svc.admit_for_cold("A", [DAY4], "Q-BAD")
        assert admit["admitted_count"] == 1
        assert admit["waiting_count"] == 0
        assert all(r.get("admission_verified") for r in admit["admitted"])


class TestRebuildAndHistoricalLookup:
    """重建不残留错误归属；旧窗口可凭原件证据回查。"""

    def test_rebuild_overwrites_without_residue_and_archives_old(self, setup_data_dir):
        data_root = setup_data_dir
        _prepare_ab_originals(data_root)
        si_svc = SeriesIndexService()
        _seed_index(310, "IDX-A-0904", "A", DAY4)
        si_svc.build_index(310)
        idx_file = data_root / "series_index" / "IDX-A-0904_index.jsonl"

        # 模拟旧索引里残留的错误归属（B 的记录被错标成 A 的 4 日）
        with open(idx_file, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "index_id": "IDX-A-0904", "point": "B", "event_day": DAY4,
                "time_partition": DAY4, "product_type": "rollup",
                "window_seq": 1, "value": 300, "verified": False,
            }) + "\n")

        result = si_svc.rebuild_index(310)
        rows = si_svc._read_jsonl(idx_file)
        # 现行索引里错误归属不再残留
        assert {r["point"] for r in rows} == {"A"}
        assert result["indexed_count"] == 2
        assert result["archived_old_index"]
        assert Path(result["archived_old_index"]).exists()
        assert result["current_version"] == "v1.2"

    def test_legacy_index_without_checksum_can_still_be_verified(self, setup_data_dir):
        data_root = setup_data_dir
        _prepare_ab_originals(data_root)
        si_svc = SeriesIndexService()
        # 旧版 v1.0 索引：无校验和，但测点/事件日/时间范围/值仍能与原件对上
        legacy = [{
            "index_id": "IDX-OLD", "point": "A", "event_day": DAY3,
            "time_partition": DAY3, "product_type": "rollup",
            "window_seq": 1, "value": 20, "index_version": "v1.0",
        }]
        _write_jsonl(data_root / "series_index" / "IDX-OLD_index.jsonl", legacy)
        q = si_svc.query_partition("A", DAY3, include_history=False)
        assert q["current"][0]["value"] == 20

        originals = si_svc._scan_originals()
        ok = si_svc._legacy_verify(legacy[0], originals)
        assert ok["verified"] is True
        # 原件缺失时，旧索引无法核验
        gone = dict(legacy[0], value=404)
        assert si_svc._legacy_verify(gone, originals)["verified"] is False


class TestColdMigrationCorrectionScenario:
    """完整叙事：受理后更正、等待重建、查询号回查、重试幂等。"""

    def test_full_correction_during_migration(self, setup_data_dir):
        data_root = setup_data_dir
        rw_svc = RollupWindowService()
        # 本场景聚焦归并原件：A 的 3 日 20、4 日 30；B 独立保留
        a_rollup = [
            _rollup_record(rw_svc, "A", DAY3, 1, 20),
            _rollup_record(rw_svc, "A", DAY4, 1, 30),
        ]
        b_rollup = [
            _rollup_record(rw_svc, "B", DAY3, 1, 200),
            _rollup_record(rw_svc, "B", DAY4, 1, 300),
        ]
        _write_jsonl(data_root / "rollup_windows" / "A_main_rollup.jsonl", a_rollup)
        _write_jsonl(data_root / "rollup_windows" / "B_main_rollup.jsonl", b_rollup)

        si_svc = SeriesIndexService()
        tier_svc = TierStorageService()
        _seed_index(320, "IDX-A-0903", "A", DAY3)
        _seed_index(321, "IDX-A-0904", "A", DAY4)
        si_svc.build_index(320)
        si_svc.build_index(321)

        # 1) 迁冷受理：3 日、4 日各一条归并窗口，凭原件全部核验通过
        admit = tier_svc.admit_for_cold("A", [DAY3, DAY4], "Q-1")
        assert admit["admitted_count"] == 2
        assert admit["waiting_count"] == 0

        # 2) 受理后、写入前：A 的 4 日原件出现可独立核验的更正值 31
        correction = rw_svc.append_correction("A", DAY4, 1, 31)
        assert correction["new_version"] == 2
        assert correction["checksum"]
        # 旧原件（30）原样保留，B 的原件不受影响
        originals = rw_svc._load_originals("A")
        day4 = [r for r in originals if r["event_day"] == DAY4 and r["product_type"] == "rollup"]
        assert {int(r["version"]) for r in day4} == {1, 2}
        assert max(day4, key=lambda r: r["version"])["rollup_value"] == 31

        # 3) 完成本次迁冷写入：3 日独立交付，4 日因替换索引重建中而等待
        evict = tier_svc.evict_to_cold("A", query_no="Q-1", event_days=[DAY3, DAY4])
        delivered_idents = [d["identity"] for d in evict["delivered"]]
        assert delivered_idents == [("A", DAY3, "rollup", 1)]
        waiting = evict["waiting"]
        assert len(waiting) == 1
        assert waiting[0]["identity"] == ("A", DAY4, "rollup", 1)
        assert any("更正值" in reason for reason in waiting[0]["reasons"])

        cold3 = tier_svc.read_cold("A", DAY3)
        assert cold3["count"] == 1 and cold3["records"][0]["value"] == 20
        cold4 = tier_svc.read_cold("A", DAY4)
        assert cold4["count"] == 0  # 等待重建期间冷区没有 4 日现时交付

        # 4) 用原查询号读回 4 日的 30：历史值仍可凭旧版原件核验
        lookback = tier_svc.read_cold_by_query("Q-1")
        day4_hist = [r for r in lookback["records"] if r["event_day"] == DAY4][0]
        assert day4_hist["value"] == 30
        assert day4_hist["verified"] is True
        assert day4_hist["historical"] is True
        assert day4_hist["superseded_by_version"] == 2

        # 5) 替换索引重建：现行 4 日索引锚定 v2 原件，值为 31，旧 30 索引归档
        rebuilt = si_svc.rebuild_index(321)
        assert rebuilt["indexed_count"] == 1
        current4 = si_svc.query_partition("A", DAY4, "rollup")["current"][0]
        assert current4["value"] == 31
        assert current4["source_version"] == 2

        # 6) 重试迁冷：冷区最终只有一份 A 的 4 日归并现时交付 31
        retry = tier_svc.evict_to_cold("A", query_no="Q-1", event_days=[DAY3, DAY4])
        assert retry["waiting_count"] == 0
        cold4 = tier_svc.read_cold("A", DAY4)
        rollup4 = [r for r in cold4["records"] if r["product_type"] == "rollup"]
        assert len(rollup4) == 1
        assert rollup4[0]["value"] == 31
        assert rollup4[0]["source_version"] == 2

        # 7) 再次重试仍然只有一份（幂等替换，不产生重复副本）
        tier_svc.evict_to_cold("A", query_no="Q-1", event_days=[DAY3, DAY4])
        cold4 = tier_svc.read_cold("A", DAY4)
        rollup4 = [r for r in cold4["records"] if r["product_type"] == "rollup"]
        assert len(rollup4) == 1
        assert rollup4[0]["value"] == 31

        # 8) B 的记录自始至终没有进入 A 的冷区
        archive = si_svc._read_jsonl(
            data_root / "tier_storage" / "A_cold_archive.jsonl")
        assert {r["point"] for r in archive} == {"A"}
        assert all(r["event_day"] in (DAY3, DAY4) for r in archive)

        # 9) 原查询号回查结果不被重建改写：仍读回受理当时的 30
        lookback_again = si_svc.query_by_number("Q-1")
        day4_hist = [r for r in lookback_again["records"] if r["event_day"] == DAY4][0]
        assert day4_hist["value"] == 30 and day4_hist["verified"] is True

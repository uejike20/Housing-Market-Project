from datetime import date

from housing_pipeline.ingest import normalize, read_checkpoint, record_key, window, write_checkpoint


def test_stable_key():
    assert record_key("MORTGAGE30US", "2026-01-01") == record_key("MORTGAGE30US", "2026-01-01")
    assert record_key("MORTGAGE30US", "2026-01-01") != record_key("MORTGAGE30US", "2026-01-08")


def test_normalize_skips_missing():
    rows = normalize([{"date": "2026-01-01", "value": "6.12"},
                      {"date": "2026-01-08", "value": "."}])
    assert len(rows) == 1 and rows[0]["value"] == 6.12


def test_window_overlaps_for_revisions():
    assert window(date(2026, 2, 1), date(2026, 2, 10), 7) == (date(2026, 1, 25), date(2026, 2, 10))


def test_checkpoint_round_trip(tmp_path):
    path = tmp_path / "state.json"
    write_checkpoint(path, date(2026, 2, 10))
    assert read_checkpoint(path) == date(2026, 2, 10)

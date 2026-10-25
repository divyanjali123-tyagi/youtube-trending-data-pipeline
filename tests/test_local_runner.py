"""The offline runner executes the whole pipeline end to end."""

import run_local


def test_run_local_end_to_end(tmp_path, spark, capsys, monkeypatch):
    monkeypatch.setattr(run_local, "make_spark", lambda: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    lake = tmp_path / "lake"
    assert run_local.main(["--lake", str(lake), "--runs", "2"]) == 0
    out = capsys.readouterr().out
    assert out.count("DQ silver -> PASSED") == 2
    assert out.count("DQ gold   -> PASSED") == 2
    assert (lake / "gold" / "region_daily_kpis").exists()

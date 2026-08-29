from pathlib import Path

from config.settings import Settings
from src.create_small_sample import create_small_sample
from src.file_router import choose_engine, inspect_file


def test_router_uses_threshold_and_force_override(tmp_path: Path) -> None:
    source = tmp_path / "orders.csv"
    source.write_text("order_id\n1\n", encoding="utf-8")
    info = inspect_file(source)
    settings = Settings(small_file_threshold_mb=0.000001)
    assert choose_engine(info, settings).engine == "pyspark"
    assert choose_engine(info, settings, "python_batch").engine == "python_batch"


def test_sample_creation_is_header_preserving_and_configurable(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    target = tmp_path / "sample.csv"
    source.write_text("a,b\n1,2\n3,4\n5,6\n", encoding="utf-8")
    assert create_small_sample(source, target, 2) == 2
    assert target.read_text(encoding="utf-8") == "a,b\n1,2\n3,4\n"

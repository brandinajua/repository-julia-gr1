import pandas as pd
from eda_cli.core import summarize_dataset, missing_table, compute_quality_flags


def test_quality_flags_basic():
    df = pd.DataFrame({
        "a": [1, 1, 1],
        "b": [1, None, 2],
    })

    summary = summarize_dataset(df)
    missing = missing_table(df)
    flags = compute_quality_flags(df, summary, missing)

    assert "quality_score" in flags
    assert isinstance(flags["quality_score"], float)

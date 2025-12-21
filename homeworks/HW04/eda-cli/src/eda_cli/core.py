from __future__ import annotations

import pandas as pd


def summarize_dataset(df: pd.DataFrame) -> pd.DataFrame:
    summary = pd.DataFrame({
        "dtype": df.dtypes.astype(str),
        "non_null": df.notnull().sum(),
        "missing": df.isnull().sum(),
        "missing_share": df.isnull().mean(),
        "unique": df.nunique(),
        "is_numeric": df.dtypes.apply(lambda x: x.kind in "if"),
    })

    if summary["is_numeric"].any():
        numeric = df.select_dtypes(include="number")
        summary.loc[numeric.columns, "min"] = numeric.min()
        summary.loc[numeric.columns, "max"] = numeric.max()
        summary.loc[numeric.columns, "mean"] = numeric.mean()
        summary.loc[numeric.columns, "std"] = numeric.std()

    return summary.reset_index(names="name")


def missing_table(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "column": df.columns,
        "missing": df.isnull().sum(),
        "missing_share": df.isnull().mean(),
    })


def compute_quality_flags(
    df: pd.DataFrame,
    summary: pd.DataFrame,
    missing_df: pd.DataFrame,
) -> dict:
    flags = {}

    flags["too_few_rows"] = len(df) < 100
    flags["too_many_columns"] = df.shape[1] > 100
    flags["too_many_missing"] = missing_df["missing_share"].max() > 0.3

    constant_cols = summary.query("unique <= 1")["name"].tolist()
    flags["has_constant_columns"] = len(constant_cols) > 0
    flags["constant_columns"] = constant_cols

    quality_score = 1.0
    for v in flags.values():
        if v is True:
            quality_score -= 0.1

    flags["quality_score"] = max(0.0, quality_score)

    return flags

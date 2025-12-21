from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path


def plot_histograms_per_column(df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    for col in df.select_dtypes(include="number").columns:
        plt.figure()
        df[col].hist(bins=30)
        plt.title(col)
        plt.savefig(out_dir / f"hist_{col}.png")
        plt.close()

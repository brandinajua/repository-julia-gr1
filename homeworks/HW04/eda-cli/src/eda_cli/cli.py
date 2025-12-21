from __future__ import annotations

from pathlib import Path
import pandas as pd
import typer

from .core import summarize_dataset, missing_table, compute_quality_flags
from .viz import plot_histograms_per_column

app = typer.Typer(help="Mini CLI for EDA CSV files")


@app.command()
def overview(path: Path):
    df = pd.read_csv(path)
    summary = summarize_dataset(df)

    typer.echo(f"Строк: {df.shape[0]}")
    typer.echo(f"Столбцов: {df.shape[1]}")
    typer.echo(summary.to_string(index=False))


@app.command()
def report(path: Path, out_dir: Path = Path("reports")):
    df = pd.read_csv(path)

    summary = summarize_dataset(df)
    missing = missing_table(df)
    flags = compute_quality_flags(df, summary, missing)

    out_dir.mkdir(parents=True, exist_ok=True)

    summary.to_csv(out_dir / "summary.csv", index=False)
    missing.to_csv(out_dir / "missing.csv", index=False)

    plot_histograms_per_column(df, out_dir)

    typer.echo("Отчёт готов")
    typer.echo(flags)

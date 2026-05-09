"""
store.py — Persist enriched data as partitioned Parquet files.

Demonstrates: pyarrow.parquet, partitioned dataset writes,
              pathlib for lake path management.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


LAKE_SCHEMA = pa.schema([
    pa.field("match_id",       pa.string()),
    pa.field("date",           pa.timestamp("ms")),
    pa.field("home_team",      pa.string()),
    pa.field("away_team",      pa.string()),
    pa.field("home_goals",     pa.int32()),
    pa.field("away_goals",     pa.int32()),
    pa.field("home_shots",     pa.int32()),
    pa.field("away_shots",     pa.int32()),
    pa.field("home_possession",pa.float64()),
    pa.field("away_possession",pa.float64()),
    pa.field("stadium",        pa.string()),
    pa.field("referee",        pa.string()),
    pa.field("result",         pa.string()),
    pa.field("total_goals",    pa.int32()),
    pa.field("goal_diff",      pa.int32()),
    pa.field("high_scoring",   pa.bool_()),
    pa.field("home_shot_acc",  pa.float64()),
    pa.field("away_shot_acc",  pa.float64()),
    pa.field("dominant_team",  pa.string()),
    pa.field("match_label",    pa.string()),
    pa.field("season",         pa.int32()),
])


def write_matches(df: pd.DataFrame, lake_dir: str | Path) -> Path:
    """
    Write enriched match DataFrame to a partitioned Parquet dataset.

    Partition layout: lake_dir/season=YYYY/part-0.parquet

    Args:
        df:       Enriched DataFrame from transform.enrich().
        lake_dir: Root directory for the data lake.

    Returns:
        Path to the lake root.
    """
    lake_dir = Path(lake_dir)
    lake_dir.mkdir(parents=True, exist_ok=True)

    # Convert pandas → Arrow with explicit schema
    table = pa.Table.from_pandas(df, schema=LAKE_SCHEMA, preserve_index=False)

    pq.write_to_dataset(
        table,
        root_path=str(lake_dir),
        partition_cols=["season"],
        compression="snappy",
        existing_data_behavior="overwrite_or_ignore",
    )

    # Report what was written
    partitions = sorted(lake_dir.glob("season=*"))
    for p in partitions:
        files = list(p.glob("*.parquet"))
        print(f"[store] {p.name}/ → {len(files)} file(s)")

    print(f"[store] Lake written to {lake_dir} ({table.num_rows} rows)")
    return lake_dir


def write_standings(df: pd.DataFrame, out_path: str | Path) -> Path:
    """
    Write team standings summary to a single Parquet file.

    Args:
        df:       Standings DataFrame from transform.summarise_by_team().
        out_path: Destination .parquet file path.

    Returns:
        Path to the written file.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(out_path), compression="snappy")

    print(f"[store] Standings saved → {out_path} ({len(df)} teams)")
    return out_path

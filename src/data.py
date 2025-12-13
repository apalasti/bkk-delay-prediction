import os
import pathlib
import logging

import duckdb
import numpy as np
import pandas as pd
import torch
from torch_geometric.data import HeteroData
from torch.utils.data import Dataset


logger = logging.getLogger(__name__)


def load_data(data_dir, database: str):
    conn = duckdb.connect(database)
    for table_name in ["positions", "stop_times", "hops", "trips", "stops"]:
        create_table_from_files(conn, data_dir, table_name)

    conn.install_extension("spatial")
    conn.load_extension("spatial")
    conn.execute("""
CREATE OR REPLACE TABLE positions AS
    SELECT * EXCLUDE (pos),
        ST_GeomFromWKB(pos)::POINT_2D AS pos,
    FROM positions;

CREATE OR REPLACE TABLE stops AS
    SELECT * EXCLUDE (stop_pos),
        ST_GeomFromWKB(stop_pos)::POINT_2D AS stop_pos,
    FROM stops;
    
-- Custom function for calculating the shortest distance between two time points
-- STRICTLY time points
CREATE OR REPLACE MACRO timediff(part, start_t, end_t) AS
CASE
    WHEN 12 < datediff('hour', start_t, end_t) 
        THEN -(datediff(part, end_t, TIME '23:59:59') + datediff(part, TIME '00:00:00', start_t))
    WHEN datediff('hour', start_t, end_t) < - 12
        THEN datediff(part, start_t, TIME '23:59:59') + datediff(part, TIME '00:00:00', end_t)
    ELSE 
        datediff(part, start_t, end_t)
END;""")

    return conn


def run_sql_file(conn: duckdb.DuckDBPyConnection, file_path: os.PathLike):
    with open(file_path) as f:
        conn.execute(f.read())


def create_table_from_files(conn: duckdb.DuckDBPyConnection, data_dir, table_name: str):
    data_dir = pathlib.Path(data_dir)
    pattern = f"**/*{table_name}*.parquet"

    num_files = 0
    for _ in data_dir.glob(pattern):
        num_files += 1

    if num_files == 0:
        raise FileNotFoundError(
            f"No parquet files found for table '{table_name}' in directory {data_dir} with pattern {pattern}"
        )

    pattern = data_dir.absolute() / pattern
    conn.execute(
        f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet($pattern)",
        parameters={"pattern": str(pattern)},
    )
    logger.info(f"Created table '{table_name}' with {num_files} parquet files matching pattern: {pattern}")


def time_to_sin_cos(time_obj):
    seconds = time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second
    angle = 2 * np.pi * seconds / 86400  # 86400 seconds in a day
    return np.sin(angle), np.cos(angle)


class DelayPredictionDataset(Dataset):
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.stops_df = conn.execute("""
            SELECT 
                h.global_trip_id, s.stop_id, s.stop_lat, s.stop_lon, 
                h.actual_arrival, st.arrival_time AS scheduled_arrival,
                h.current_stop_sequence
            FROM hops h
            JOIN stops s ON h.to_stop_id = s.stop_id
            JOIN stop_times st ON h.trip_id = st.trip_id AND h.to_stop_id = st.stop_id
            ORDER BY h.global_trip_id, h.current_stop_sequence
        """).fetchdf()
        self.stops_df["actual_arrival"] = self.stops_df["actual_arrival"]
        self.stops_df.set_index(["global_trip_id", "current_stop_sequence"], inplace=True)

        stop_id_counts = self.stops_df["stop_id"].value_counts()
        self.stop_id_mapping = {
            stop_id: i + 1
            for i, stop_id in enumerate(
                stop_id_counts[
                    stop_id_counts > 30
                ].index  # pyright: ignore[reportAttributeAccessIssue]
            )
        }
        self.stops_df["stop_id_int"] = self.stops_df["stop_id"].map(
            lambda x: self.stop_id_mapping.get(x, 0)
        )

        self.stops_df["actual_arrival_sin"], self.stops_df["actual_arrival_cos"] = zip(
            *self.stops_df["actual_arrival"].dt.time.apply(time_to_sin_cos)
        )
        self.stops_df["scheduled_arrival_sin"], self.stops_df["scheduled_arrival_cos"] = zip(
            *self.stops_df["scheduled_arrival"].apply(time_to_sin_cos)
        )

        self.stops_df["delay"] = (
            pd.to_timedelta(self.stops_df["actual_arrival"].dt.time.astype("string"))
            - pd.to_timedelta(self.stops_df["scheduled_arrival"].astype("string"))  # pyright: ignore[reportCallIssue, reportArgumentType]
        ).dt.total_seconds()

        self.pos_df = conn.execute("""
            SELECT p.global_trip_id, p.latitude, p.longitude, p.bearing, p.speed, p.timestamp
            FROM positions p
                JOIN hops h ON p.global_trip_id = h.global_trip_id AND p.current_stop_sequence = h.current_stop_sequence
            WHERE p.timestamp < h.actual_arrival
            ORDER BY p.global_trip_id, p.timestamp
        """).fetchdf()
        self.pos_df["timestamp_sin"], self.pos_df["timestamp_cos"] = zip(
            *self.pos_df["timestamp"].dt.time.apply(time_to_sin_cos)
        )

    def __len__(self):
        return len(self.pos_df)

    def __getitem__(self, index: int):
        position_row = self.pos_df.iloc[index]
        stops_slice = self.stops_df.loc[(position_row["global_trip_id"], slice(None))]
        return self.build_graph(position_row, stops_slice)

    def build_graph(self, position: pd.Series, stops_df: pd.DataFrame):
        data = HeteroData()

        is_past = stops_df["actual_arrival"].values <= position["timestamp"]
        is_future = ~is_past

        past_indices = np.flatnonzero(is_past)
        future_indices = np.flatnonzero(is_future)
        num_stops = len(stops_df)

        # Stop features
        x_stop = stops_df[[
            "scheduled_arrival_sin", 
            "scheduled_arrival_cos", 
            "delay",
            "delay", # MASK flag
        ]].to_numpy(dtype=np.float32)
        x_stop[is_future, -2] = 0.0 # Remove delay for future stops
        x_stop[:, -1] = is_past.astype(np.float32) # MASK flag

        data["stop"].x = torch.from_numpy(x_stop)

        # Static Inputs for Embeddings (Physical ID and GPS)
        data["stop"].physical_id = torch.tensor(stops_df["stop_id_int"].astype(int).values, dtype=torch.long)
        data["stop"].pos = torch.tensor(stops_df[["stop_lat", "stop_lon"]].values, dtype=torch.float)

        # Ground Truth (Target) for ALL nodes
        data["stop"].y = torch.tensor(stops_df["delay"].values, dtype=torch.float)

        # Bus features
        data["bus"].x = torch.tensor([[position["speed"], position["bearing"]]], dtype=torch.float)

        if num_stops > 1:
            # Connect stop -> stop in arrival order
            u = torch.arange(0, num_stops - 1, dtype=torch.long)
            v = torch.arange(1, num_stops, dtype=torch.long)
            data["stop", "next", "stop"].edge_index = torch.stack([u, v], dim=0)
        else:
            data["stop", "next", "stop"].edge_index = torch.empty((2,0), dtype=torch.long)

        if len(past_indices) > 0:
            # Connect all past stops TO the bus (index 0)
            src = torch.from_numpy(past_indices).long()
            dst = torch.zeros(len(past_indices), dtype=torch.long)
            data["stop", "history", "bus"].edge_index = torch.stack([src, dst], dim=0)
        else:
            data["stop", "history", "bus"].edge_index = torch.empty((2, 0), dtype=torch.long)

        if len(future_indices) > 0:
            # Connect Bus (index 0) TO all future stops
            src = torch.zeros(len(future_indices), dtype=torch.long)
            dst = torch.from_numpy(future_indices).long()
            data["bus", "predict", "stop"].edge_index = torch.stack([src, dst], dim=0)
        else:
            data["bus", "predict", "stop"].edge_index = torch.empty((2, 0), dtype=torch.long)

        return data


if __name__ == "__main__":
    with load_data("data/processed/", ":memory:") as conn:
        dataset = DelayPredictionDataset(conn)
    print(f"Loaded {len(dataset)} samples.")
    sample = dataset[0]
    print("Sample's type:", type(sample))
    print("Sample:", sample)

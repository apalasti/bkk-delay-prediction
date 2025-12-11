import plotly.express as px
from duckdb import DuckDBPyConnection


def plot_trip(conn: DuckDBPyConnection, global_trip_id: str):
    positions_query = """
SELECT * FROM positions 
WHERE global_trip_id = $global_trip_id 
ORDER BY timestamp ASC"""
    positions = conn.sql(
        positions_query, params={"global_trip_id": global_trip_id}
    ).to_df()

    assert (
        len(positions) > 0
    ), f"No positions found for the specified global_trip_id: {global_trip_id}"
    assert positions["trip_id"].nunique() == 1
    trip_id: str = positions.at[0, "trip_id"]

    stops_query = """
SELECT 
    s.stop_id, 
    s.stop_name, 
    s.stop_lat, 
    s.stop_lon, 
    array_agg(st.arrival_time) AS time
FROM stops s 
    JOIN stop_times st ON s.stop_id = st.stop_id AND $trip_id = st.trip_id 
GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon"""
    stops = conn.sql(stops_query, params={"trip_id": trip_id}).to_df()
    stops["text"] = (
        stops["stop_id"].astype(str)
        + "<br>"
        + stops["stop_name"].astype(str)
        + "<br>"
        + stops["time"].apply(lambda x: ", ".join([t.strftime("%H:%M") for t in x]))
    )

    fig = px.line_map(
        positions,
        lat="latitude", lon="longitude",
        zoom=12, map_style="outdoors",
        hover_data=[
            "timestamp",
            "current_stop_sequence",
            "speed",
            "bearing",
            "trip_id",
            "vehicle_id",
        ],
    )
    fig.update_traces(mode="markers+lines", marker={"size": 10}, line={"width": 3})
    fig.add_scattermap(
        lon=stops["stop_lon"], lat=stops["stop_lat"],
        mode="markers", marker={"size": 12},
        text=stops["text"],
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
    return fig


def plot_positions(conn: DuckDBPyConnection, vehicle_id: str, from_t: str, to_t: str):
    positions_query = """
SELECT * FROM positions 
WHERE vehicle_id = $vehicle_id AND timestamp BETWEEN $from AND $to 
ORDER BY timestamp"""
    positions = conn.sql(
        positions_query,
        params={"vehicle_id": vehicle_id, "from": from_t, "to": to_t},
    ).to_df()

    assert (
        len(positions) > 0
    ), f"No positions found for the specified vehicle_id: {vehicle_id}"

    stops_query = """
SELECT 
    s.stop_id,
    s.stop_name, 
    s.stop_lat, 
    s.stop_lon, 
    array_agg(st.arrival_time) AS time
FROM stops s 
JOIN stop_times st ON s.stop_id = st.stop_id 
WHERE list_contains($stops, s.stop_id) AND list_contains($trips, st.trip_id)
GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon"""
    stops = conn.sql(
        stops_query,
        params={"stops": positions["stop_id"].unique(), "trips": positions["trip_id"].unique()},
    ).to_df()
    stops["text"] = (
        stops["stop_id"].astype(str)
        + "<br>"
        + stops["stop_name"].astype(str)
        + "<br>"
        + stops["time"].apply(lambda x: ", ".join([t.strftime("%H:%M") for t in x]))
    )

    fig = px.line_map(
        positions, lat="latitude", lon="longitude", zoom=12, map_style="outdoors", color="trip_id",
        hover_data=["timestamp", "current_stop_sequence", "trip_id", "vehicle_id", "speed", "bearing"]
    )
    fig.update_traces(mode="markers+lines", marker={"size": 10}, line={"width": 3})
    fig.add_scattermap(
        lon=stops["stop_lon"], lat=stops["stop_lat"], 
        mode="markers", marker={"size": 12}, 
        text=stops["text"]
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
    return fig

import pandas as pd

from .transit_feed import fetch_trainsit_feed


def parse_alert_entity(entity):
    alert = entity.alert
    return {
        "id": entity.id,
        "active_periods": [
            {
                "start": period.start if period.HasField("start") else None,
                "end": period.end if period.HasField("end") else None,
            }
            for period in alert.active_period
        ],
        "cause": alert.Cause.Name(alert.cause) if alert.HasField("cause") else None,
        "effect": alert.Effect.Name(alert.effect) if alert.HasField("effect") else None,
        "informed_entity": [
            {
                "agency_id": ie.agency_id if ie.HasField("agency_id") else None,
                "route_id": ie.route_id if ie.HasField("route_id") else None,
                "trip_id": ie.trip.trip_id if ie.HasField("trip") and ie.trip.HasField("trip_id") else None,
                "stop_id": ie.stop_id if ie.HasField("stop_id") else None,
            }
            for ie in alert.informed_entity
        ],
        "severity_level": alert.severity_level if alert.HasField("severity_level") else None,
    }


def fetch_alerts(api_key=None, timeout=10):
    feed = fetch_trainsit_feed(
        feed_type="alerts", api_key=api_key, timeout=timeout
    )
    if feed is None:
        return pd.DataFrame()

    df = pd.DataFrame([
        parse_alert_entity(entity)
        for entity in feed.entity
        if entity.HasField("alert")
    ])

    def unwrap_col(df: pd.DataFrame, col: str):
        expanded = pd.json_normalize(df[col].explode())
        expanded.columns = [f"{col}_{subcol}" for subcol in expanded.columns]
        df.drop(columns=[col], inplace=True)
        return df.join(expanded)

    df = unwrap_col(df, "active_periods")
    df = unwrap_col(df, "informed_entity")
    df.drop(columns=["informed_entity_agency_id"], inplace=True)

    # Map columns to correct types
    df["active_periods_start"] = pd.to_datetime(
        df["active_periods_start"], unit="s", errors="coerce", utc=True
    )
    df["active_periods_end"] = pd.to_datetime(
        df["active_periods_end"], unit="s", errors="coerce", utc=True
    )
    df = df.astype({
        "id": "string",
        "cause": "category",
        "effect": "category",
        "severity_level": "category",
        "informed_entity_route_id": "string",
        "informed_entity_trip_id": "string",
        "informed_entity_stop_id": "string",
    }, copy=False)
    return df


if __name__ == "__main__":
    df = fetch_alerts()
    print(df.info(verbose=True))
    print(df.sample(n=10))

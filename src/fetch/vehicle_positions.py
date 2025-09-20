import pandas as pd

from .transit_feed import fetch_trainsit_feed


def parse_vehicle_entity(entity):
    vehicle = entity.vehicle
    return {
        "id": entity.id,
        "trip_id": vehicle.trip.trip_id if vehicle.trip.HasField("trip_id") else None,
        "route_id": vehicle.trip.route_id if vehicle.trip.HasField("route_id") else None,
        "vehicle_id": vehicle.vehicle.id if vehicle.vehicle.HasField("id") else None,
        "vehicle_label": vehicle.vehicle.label if vehicle.vehicle.HasField("label") else None,
        "vehicle_license_plate": vehicle.vehicle.license_plate if vehicle.vehicle.HasField("license_plate") else None,
        "latitude": vehicle.position.latitude if vehicle.position.HasField("latitude") else None,
        "longitude": vehicle.position.longitude if vehicle.position.HasField("longitude") else None,
        "bearing": vehicle.position.bearing if vehicle.position.HasField("bearing") else None,
        "speed": vehicle.position.speed if vehicle.position.HasField("speed") else None,
        "timestamp": vehicle.timestamp if vehicle.HasField("timestamp") else None,
        "current_stop_sequence": vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None,
        "current_status": vehicle.current_status if vehicle.HasField("current_status") else None,
        "stop_id": vehicle.stop_id if vehicle.HasField("stop_id") else None,
    }


def fetch_vehicle_positions(api_key=None, timeout=10):
    feed = fetch_trainsit_feed(
        feed_type="vehicle_pos", api_key=api_key, timeout=timeout
    )
    if feed is None:
        return pd.DataFrame()

    return pd.DataFrame([
        parse_vehicle_entity(entity)
        for entity in feed.entity
        if entity.HasField("vehicle")
    ])


if __name__ == "__main__":
    df = fetch_vehicle_positions()
    print(df.sample(n=10))

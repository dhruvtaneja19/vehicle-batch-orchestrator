from schemas.vehicle import VehicleRequest

def process_fastag(vehicle: VehicleRequest):
    return {
        "vehicle_id": vehicle.vehicle_id,
        "fastag_status": "ACTIVE",
        "bank": "HDFC"
    }

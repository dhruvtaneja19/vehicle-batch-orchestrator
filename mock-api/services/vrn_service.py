from schemas.vehicle import VehicleRequest

def process_vrn(vehicle: VehicleRequest):
    return {
        "vehicle_id": vehicle.vehicle_id,
        "blacklisted": False
    }

from schemas.vehicle import VehicleRequest

def process_valuation(vehicle: VehicleRequest):
    age = 2026 - vehicle.year
    value = max(300000, 1500000 - age * 100000)
    return {
        "vehicle_id": vehicle.vehicle_id,
        "market_price": value
    }

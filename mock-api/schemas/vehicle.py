from pydantic import BaseModel

class VehicleRequest(BaseModel):
    vehicle_id: str
    brand: str
    model: str
    year: int
    fuel_type: str

class RCResponse(BaseModel):
    vehicle_id: str
    owner: str
    registration_status: str

class FastagResponse(BaseModel):
    vehicle_id: str
    fastag_status: str
    bank: str

class VRNResponse(BaseModel):
    vehicle_id: str
    blacklisted: bool

class ValuationResponse(BaseModel):
    vehicle_id: str
    market_price: int

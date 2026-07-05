from fastapi import APIRouter
from schemas.vehicle import VehicleRequest, ValuationResponse
from services.valuation_service import process_valuation

router = APIRouter()

@router.post("/valuation", response_model=ValuationResponse)
def get_valuation(request: VehicleRequest):
    return process_valuation(request)

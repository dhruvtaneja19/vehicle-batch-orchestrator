import time
from concurrent.futures import ThreadPoolExecutor

from scripts.api_client import (
    get_rc,
    get_fastag,
    get_vrn,
    get_valuation,
)

from utils.logger import get_logger

logger = get_logger(__name__)

def enrich_vehicle(vehicle: dict, environment: str, enable_valuation: bool):
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=4) as executor:
        rc_future = executor.submit(get_rc, vehicle, environment)
        fastag_future = executor.submit(get_fastag, vehicle, environment)
        vrn_future = executor.submit(get_vrn, vehicle, environment)
        if enable_valuation:
            valuation_future = executor.submit(get_valuation, vehicle, environment)

        try:
            rc = rc_future.result()
        except Exception as e:
            from utils.error_handler import handle_api_error
            rc = handle_api_error("RC", vehicle["vehicle_id"], e)
            
        try:
            fastag = fastag_future.result()
        except Exception as e:
            from utils.error_handler import handle_api_error
            fastag = handle_api_error("FASTag", vehicle["vehicle_id"], e)

        try:
            vrn = vrn_future.result()
        except Exception as e:
            from utils.error_handler import handle_api_error
            vrn = handle_api_error("VRN", vehicle["vehicle_id"], e)

        try:
            if enable_valuation:
                valuation = valuation_future.result()
            else:
                logger.info(
                    "Skipping Valuation API"
                )
            
                valuation = {
                    "market_value": None,
                    "valuation_status": "SKIPPED"
                }
        except Exception as e:
            from utils.error_handler import handle_api_error
            valuation = handle_api_error("Valuation", vehicle["vehicle_id"], e)

    elapsed = time.perf_counter() - start
    print(f"{vehicle['vehicle_id']} processed in {elapsed:.2f} sec")

    result = vehicle.copy()
    result.update(rc)
    result.update(fastag)
    result.update(vrn)
    result.update(valuation)

    return result

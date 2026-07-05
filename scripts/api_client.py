import logging
import requests
from scripts.environment_selector import get_environment
from utils.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

logger = get_logger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1),
    reraise=True,
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def post(endpoint, payload, environment):
    base_url = get_environment(environment)
    url = f"{base_url}/{endpoint}"
    
    logger.info(f"Using environment: {environment}")
    logger.info(f"Calling URL: {url}")

    response = requests.post(
        url,
        json=payload,
        timeout=10,
    )

    response.raise_for_status()

    return response.json()

def get_rc(vehicle, environment):
    return post("rc", vehicle, environment)

def get_fastag(vehicle, environment):
    return post("fastag", vehicle, environment)

def get_vrn(vehicle, environment):
    return post("vrn", vehicle, environment)

def get_valuation(vehicle, environment):
    return post("valuation", vehicle, environment)
from configs.environment import (
    STAGING,
    PRODUCTION,
    STAGING_URL,
    PRODUCTION_URL,
)

def get_environment(run_environment: str):
    if run_environment == STAGING:
        return STAGING_URL

    if run_environment == PRODUCTION:
        return PRODUCTION_URL

    raise ValueError(f"Unknown environment: {run_environment}")

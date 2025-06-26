# my_speed_camera_project/resources.py

from dagster import resource, get_dagster_logger
from dotenv import load_dotenv
import os

logger = get_dagster_logger()

# Load environment variables from a .env file
load_dotenv()

@resource
def nyc_open_data_api_key_resource():
    """
    A resource to provide an API key for NYC Open Data (if required).
    It retrieves the key from an environment variable.
    """
    api_key = os.getenv("myappsec")
    load_dotenv()

    if not api_key:
        logger.warning(
            "NYC_OPEN_DATA_API_KEY environment variable not set. "
            "Proceeding without API key. Note: Some endpoints might require it."
        )
    return api_key

# You can also create a resource for a geocoding API key if you switch from Nominatim
@resource
def geocoding_api_key_resource():
    """
    A resource to provide an API key for a geocoding service (e.g., Google, Geoapify).
    """
    api_key = os.getenv("GEOCODING_API_KEY")
    if not api_key:
        logger.warning(
            "GEOCODING_API_KEY environment variable not set. Geocoding may fail "
            "or be rate-limited if a key is required for your chosen service."
        )
    return api_key
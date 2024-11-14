"""
Module implementing a client for fetching crop prices.
"""

import logging
from dataclasses import dataclass
from datetime import date
from io import StringIO
from typing import Dict, List, Optional, Union
import time

import pandas as pd
import requests
import tqdm

# API URL endpoint
URL = "http://18.233.138.7/api/v1/crop-prices"

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)

# Constants
MAX_RETRIES = 5  # Maximum number of retry attempts
BACKOFF_FACTOR = 2  # Backoff multiplier for exponential delay


@dataclass
class CropPriceFilterParams:
    """Encapsulates filtering parameters for crop data requests."""

    crops: Optional[List[str]] = None
    regions: Optional[List[str]] = None
    districts: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    ordering: Optional[List[str]] = None


class CropPriceDataFrameBuilder:
    """
    Builder class for constructing a DataFrame from crop price data with configurable filtering parameters.
    Supports chainable methods for specifying filtering options and initiates the data fetch process.
    """

    def __init__(self):
        self._params = CropPriceFilterParams()

    @classmethod
    def of(cls, *crops: str) -> "CropPriceDataFrameBuilder":
        """
        Initialize the builder with a list of crops.

        Args:
            *crops: Names of crops to filter.

        Returns:
            CropPriceDataFrameBuilder: The builder instance for chaining.
        """
        builder = cls()
        builder._params.crops = list(crops)
        return builder

    def in_regions(self, *region_district_pairs: str) -> "CropPriceDataFrameBuilder":
        """
        Specify regions or region/district pairs for filtering.

        Args:
            *region_district_pairs: Strings in "Region" or "Region/District" format.

        Returns:
            CropPriceDataFrameBuilder: The builder instance for chaining.
        """
        regions, districts = [], []
        for pair in region_district_pairs:
            parts = pair.split("/")
            regions.append(parts[0].strip())
            if len(parts) == 2:
                districts.append(parts[1].strip())
        self._params.regions = regions
        self._params.districts = districts if districts else None
        return self

    def from_date(self, start_date: Union[str, date]) -> "CropPriceDataFrameBuilder":
        """
        Set the start date for filtering.

        Args:
            start_date: Start date as a date object or ISO-format string.

        Returns:
            CropPriceDataFrameBuilder: The builder instance for chaining.
        """
        self._params.start_date = self._parse_date(start_date)
        return self

    def to_date(self, end_date: Union[str, date]) -> "CropPriceDataFrameBuilder":
        """
        Set the end date for filtering.

        Args:
            end_date: End date as a date object or ISO-format string.

        Returns:
            CropPriceDataFrameBuilder: The builder instance for chaining.
        """
        self._params.end_date = self._parse_date(end_date)
        return self

    def order_by(self, *ordering: str) -> "CropPriceDataFrameBuilder":
        """
        Specify ordering criteria for the data.

        Args:
            *ordering: Fields for sorting prefixed by '+' (ascending) or '-' (descending).

        Returns:
            CropPriceDataFrameBuilder: The builder instance for chaining.
        """
        self._params.ordering = list(ordering)
        return self

    def build(self) -> pd.DataFrame:
        """
        Execute the data fetch with the configured parameters and build the DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the filtered crop price data.
        """
        params = self._construct_query_params()
        return _fetch_crop_data(params)

    def _parse_date(self, dt: Union[str, date]) -> date:
        """Convert a date string to a date object if necessary."""
        if isinstance(dt, str):
            return date.fromisoformat(dt)
        if isinstance(dt, date):
            return dt
        raise ValueError("Invalid date format")

    def _construct_query_params(self) -> Dict[str, str]:
        """
        Construct the dictionary of query parameters for the API request based on the builder
        settings.

        Returns:
            Dict[str, str]: Dictionary of API parameters.
        """
        params = {
            "crop_prices__in": (
                ",".join(self._params.crops) if self._params.crops else None
            ),
            "region__in": (
                ",".join(self._params.regions) if self._params.regions else None
            ),
            "district__in": (
                ",".join(self._params.districts) if self._params.districts else None
            ),
            "ts__gte": (
                self._params.start_date.isoformat() if self._params.start_date else None
            ),
            "ts__lte": (
                self._params.end_date.isoformat() if self._params.end_date else None
            ),
            "ordering": ",".join(self._params.ordering or ["+ts"]),
        }
        return {k: v for k, v in params.items() if v is not None}


def _fetch_crop_data(params: Dict[str, str]) -> pd.DataFrame:
    """
    Attempt to retrieve crop data from the API with retry logic.

    Args:
        params: Query parameters for the API request.

    Returns:
        pd.DataFrame: DataFrame containing the crop price data.
    """
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            with requests.get(URL, params=params, timeout=30, stream=True) as response:
                if response.status_code == 429:
                    logger.warning("Rate limit exceeded (429); retrying...")
                    time.sleep(BACKOFF_FACTOR**attempt)
                    attempt += 1
                    continue

                response.raise_for_status()

                # Progress bar setup
                total_size = int(response.headers.get("content-length", 0))
                block_size = 1024
                progress_bar = tqdm.tqdm(
                    total=total_size,
                    unit="iB",
                    unit_scale=True,
                    desc="Downloading data",
                )
                bytes_data = bytearray()

                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    bytes_data.extend(data)
                progress_bar.close()

                csv_content = bytes_data.decode("utf-8")
                return _convert_csv_to_dataframe(csv_content)

        except requests.RequestException as e:
            logger.exception("Error fetching crop data: %s", e)
            if attempt >= MAX_RETRIES - 1:
                raise RuntimeError(
                    "Failed to fetch crop data after multiple attempts"
                ) from e
            logger.info("Retrying fetch... attempt %d", attempt + 1)
            time.sleep(BACKOFF_FACTOR**attempt)
            attempt += 1


def _convert_csv_to_dataframe(csv_content: str) -> pd.DataFrame:
    """
    Convert raw CSV content to a structured DataFrame with formatted fields.

    Args:
        csv_content: CSV data as a string.

    Returns:
        pd.DataFrame: DataFrame with cleaned and formatted crop price data.
    """
    df = pd.read_csv(StringIO(csv_content))
    return _clean_dataframe(df)


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the DataFrame fields, formatting region, district, and date columns.

    Args:
        df: Raw DataFrame.

    Returns:
        pd.DataFrame: Cleaned DataFrame ready for analysis.
    """
    df["region"] = df["region"].str.replace(
        "dar es saalam", "Dar-es-Salaam", case=False
    )
    df["region"] = df["region"].str.title()
    df["district"] = df["district"].str.title()
    df["crop"] = df["crop"].str.title()
    df["ts"] = pd.to_datetime(df["ts"], format="%Y-%m-%d")
    return df

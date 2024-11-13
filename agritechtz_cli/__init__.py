"""
Module implementing a client for fetching crop prices.
"""

import logging
from dataclasses import dataclass
from datetime import date
from io import StringIO
from typing import Dict, List, Optional, Union

import pandas as pd
import requests
import tqdm

# API URL endpoint
URL = "http://http://18.233.138.7/api/v1/crop-prices"

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)


@dataclass
class CropPriceFilterParams:
    """Data class to encapsulate parameters for crop data filtering."""

    crops: Optional[List[str]] = None
    regions: Optional[List[str]] = None
    districts: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    ordering: Optional[List[str]] = None


class CropPriceDataFrameBuilder:
    """
    A builder class to construct pandas DataFrames from crop price
    data with chainable configuration methods.
    """

    def __init__(self):
        self._params = CropPriceFilterParams()

    @classmethod
    def of(cls, *crops: str) -> "CropPriceDataFrameBuilder":
        """Initialize builder with a list of crops."""
        builder = cls()
        builder._params.crops = list(crops)
        return builder

    def _handle_date(self, dt: Union[str, date]) -> date:
        """Parse date parameter."""
        if isinstance(dt, str):
            dt = date.fromisoformat(dt)
        if not isinstance(dt, date):
            raise ValueError("Invalid date format.")
        return dt

    def in_regions(self, *region_district_pairs: str) -> "CropPriceDataFrameBuilder":
        """Specify region/district pairs for filtering.

        Args:
            *region_district_pairs: Strings
            representing region or region/district pairs separated by '/'.

        Returns:
            CropPriceDataFrameBuilder: Enables method chaining.
        """
        regions, districts = [], []
        for pair in region_district_pairs:
            parts = pair.split("/")
            if len(parts) == 1:
                regions.append(parts[0].strip())
            elif len(parts) == 2:
                regions.append(parts[0].strip())
                districts.append(parts[1].strip())
            else:
                logger.error("Invalid region/district pair format: %s", pair)
                raise ValueError(f"Invalid region/district format: '{pair}'")
        self._params.regions = regions
        if districts:
            self._params.districts = districts
        return self

    def from_date(self, start_date: Union[str, date]) -> "CropPriceDataFrameBuilder":
        """Set the start date for data filtering.

        Args:
            start_date: The start date as a string in ISO format or a date object.

        Returns:
            CropPriceDataFrameBuilder: Enables method chaining.
        """
        self._params.start_date = self._handle_date(start_date)
        return self

    def to_date(self, end_date: Union[str, date]) -> "CropPriceDataFrameBuilder":
        """Set the end date for data filtering.

        Args:
            end_date: The end date as a string in ISO format or a date object.

        Returns:
            CropPriceDataFrameBuilder: Enables method chaining.
        """
        self._params.end_date = self._handle_date(end_date)
        return self

    def order_by(self, *ordering: str) -> "CropPriceDataFrameBuilder":
        """Specify ordering criteria.

        Args:
            *ordering: Fields to order by, prefixed with '+' (ascending) or '-' (descending).

        Returns:
            CropPriceDataFrameBuilder: Enables method chaining.
        """
        self._params.ordering = list(ordering)
        return self

    def build(self) -> pd.DataFrame:
        """Fetch and construct the DataFrame based on set parameters.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the crop price data.
        """
        params = self._create_params_dict()
        return _fetch_crop_data(params)

    def _create_params_dict(self) -> Dict[str, str]:
        """Assemble request parameters from the set configuration.

        Returns:
            Dict[str, str]: A dictionary of parameters suitable for the API request.
        """
        params = {}
        if self._params.crops:
            params["crop_prices__in"] = ",".join(self._params.crops)
        if self._params.regions:
            params["region__in"] = ",".join(self._params.regions)
        if self._params.districts:
            params["district__in"] = ",".join(self._params.districts)
        if self._params.start_date:
            params["ts__gte"] = self._params.start_date.isoformat()
        if self._params.end_date:
            params["ts__lte"] = self._params.end_date.isoformat()
        params["ordering"] = ",".join(self._params.ordering or ["+ts"])
        return params


def _convert_csv_to_dataframe(csv_content: str) -> pd.DataFrame:
    """
    Convert CSV content to a pandas DataFrame.

    Args:
        csv_content: CSV data as a string.

    Returns:
        pd.DataFrame: A pandas DataFrame with formatted crop price data.
    """
    df = pd.read_csv(StringIO(csv_content))
    df["region"] = df["region"].str.replace(
        "dar es saalam", "Dar-es-Salaam", case=False
    )
    df["region"] = df["region"].str.title()
    df["district"] = df["district"].str.title()
    df["crop"] = df["crop"].str.title()
    df["ts"] = pd.to_datetime(df["ts"], format="%Y-%m-%d")
    return df


def _fetch_crop_data(params: Dict[str, str]) -> pd.DataFrame:
    """
    Retrieve crop data from the API and convert it to a DataFrame.

    Args:
        params: Parameters for the API request.

    Returns:
        pd.DataFrame: A pandas DataFrame with crop prices.
    """
    try:
        with requests.get(URL, params=params, timeout=30, stream=True) as response:
            response.raise_for_status()

            # Get total size from headers
            total_size_in_bytes = int(response.headers.get("content-length", 0))
            if total_size_in_bytes == 0:
                logger.warning(
                    "Unable to determine total content size for progressbar."
                )
            block_size = 1024
            progressbar = tqdm.tqdm(
                total=total_size_in_bytes,
                unit="iB",
                unit_scale=True,
                desc="Downloading data...",
            )
            bytes_data = bytearray()

            for data in response.iter_content(block_size):
                progressbar.update(len(data))
                bytes_data.extend(data)
            progressbar.close()

    except requests.RequestException as e:
        logger.exception("Error fetching crop data: %s", e)
        raise RuntimeError("Failed to fetch crop data") from e

    csv_content = bytes_data.decode("utf-8")
    return _convert_csv_to_dataframe(csv_content)

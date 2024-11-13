"""Unit test module for the Crop price dataframe"""

# pylint: disable=protected-access

import unittest
from unittest.mock import patch, MagicMock
from datetime import date


import requests
import pandas as pd

from agritechtz_cli import (
    CropPriceDataFrameBuilder,
    _convert_csv_to_dataframe,
    _fetch_crop_data,
    CropPriceFilterParams,
    URL,
)


class TestCropPriceDataFrameBuilder(unittest.TestCase):
    """Unit test the `CropPriceDataFrameBuilder` class"""

    def setUp(self):
        self.builder = CropPriceDataFrameBuilder()

    def test_initialization(self):
        """Test that the builder initializes correctly."""
        self.assertIsInstance(self.builder, CropPriceDataFrameBuilder)
        self.assertIsInstance(self.builder._params, CropPriceFilterParams)

    def test_of_method(self):
        """Test the 'of' class method."""
        builder = CropPriceDataFrameBuilder.of("maize", "rice")
        self.assertEqual(builder._params.crops, ["maize", "rice"])

    def test_in_regions_single(self):
        """Test 'in_regions' with a single region."""
        self.builder.in_regions("Region1")
        self.assertEqual(self.builder._params.regions, ["Region1"])
        self.assertIsNone(self.builder._params.districts)

    def test_in_regions_with_districts(self):
        """Test 'in_regions' with regions and districts."""
        self.builder.in_regions("Region1/District1", "Region2/District2")
        self.assertEqual(self.builder._params.regions, ["Region1", "Region2"])
        self.assertEqual(self.builder._params.districts, ["District1", "District2"])

    def test_in_regions_invalid_format(self):
        """Test 'in_regions' with invalid input format."""
        with self.assertRaises(ValueError):
            self.builder.in_regions("Invalid/Format/Extra")

    def test_handle_date_with_string(self):
        """Test '_handle_date' with a string input."""
        result = self.builder._handle_date("2023-10-01")
        self.assertEqual(result, date(2023, 10, 1))

    def test_handle_date_with_date_object(self):
        """Test '_handle_date' with a date object."""
        input_date = date(2023, 10, 1)
        result = self.builder._handle_date(input_date)
        self.assertEqual(result, input_date)

    def test_handle_date_invalid_input(self):
        """Test '_handle_date' with invalid input."""
        with self.assertRaises(ValueError):
            self.builder._handle_date(12345)

    def test_from_date(self):
        """Test 'from_date' method."""
        self.builder.from_date("2023-10-01")
        self.assertEqual(self.builder._params.start_date, date(2023, 10, 1))

    def test_to_date(self):
        """Test 'to_date' method."""
        self.builder.to_date("2023-10-31")
        self.assertEqual(self.builder._params.end_date, date(2023, 10, 31))

    def test_order_by(self):
        """Test 'order_by' method."""
        self.builder.order_by("+ts", "-crop")
        self.assertEqual(self.builder._params.ordering, ["+ts", "-crop"])

    def test_create_params_dict(self):
        """Test internal method '_create_params_dict'."""
        self.builder._params = CropPriceFilterParams(
            crops=["maize", "rice"],
            regions=["Region1"],
            districts=["District1"],
            start_date=date(2023, 10, 1),
            end_date=date(2023, 10, 31),
            ordering=["+ts", "-crop"],
        )
        expected_params = {
            "crop_prices__in": "maize,rice",
            "region__in": "Region1",
            "district__in": "District1",
            "ts__gte": "2023-10-01",
            "ts__lte": "2023-10-31",
            "ordering": "+ts,-crop",
        }
        params = self.builder._create_params_dict()
        self.assertEqual(params, expected_params)

    @patch("agritechtz_cli.requests.get")
    def test_fetch_crop_data(self, mock_get):
        """Test '_fetch_crop_data' with mocked requests."""
        # Mock response content
        mock_response = MagicMock()
        csv_data = "crop,region,district,ts\nMaize,Region1,District1,2023-10-01"
        mock_response.content.decode.return_value = csv_data
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        params = {"crop_prices__in": "maize", "ordering": "+ts"}
        df = _fetch_crop_data(params)

        # Verify requests.get was called correctly
        mock_get.assert_called_with(URL, params=params, timeout=30)
        # Verify DataFrame contents
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["crop"], "Maize")
        self.assertEqual(df.iloc[0]["region"], "Region1")
        self.assertEqual(df.iloc[0]["district"], "District1")
        self.assertEqual(df.iloc[0]["ts"], pd.Timestamp("2023-10-01"))

    def test_convert_csv_to_dataframe(self):
        """Test '_convert_csv_to_dataframe' function."""
        csv_content = (
            "crop,region,district,ts\nMaize,dar es saalam,District1,2023-10-01"
        )
        df = _convert_csv_to_dataframe(csv_content)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["region"], "Dar-Es-Salaam")
        self.assertEqual(df.iloc[0]["crop"], "Maize")
        self.assertEqual(df.iloc[0]["ts"], pd.Timestamp("2023-10-01"))

    @patch("agritechtz_cli._fetch_crop_data")
    def test_build_method(self, mock_fetch_crop_data):
        """Test 'build' method of the builder."""
        # Mock DataFrame to be returned by _fetch_crop_data
        mock_df = pd.DataFrame(
            {
                "crop": ["Maize"],
                "region": ["Region1"],
                "district": ["District1"],
                "ts": [pd.Timestamp("2023-10-01")],
            }
        )
        mock_fetch_crop_data.return_value = mock_df

        # Build DataFrame using the builder
        df = (
            CropPriceDataFrameBuilder.of("maize")
            .in_regions("Region1/District1")
            .from_date("2023-10-01")
            .to_date("2023-10-31")
            .order_by("+ts")
            .build()
        )

        # Verify that _fetch_crop_data was called with correct parameters
        expected_params = {
            "crop_prices__in": "maize",
            "region__in": "Region1",
            "district__in": "District1",
            "ts__gte": "2023-10-01",
            "ts__lte": "2023-10-31",
            "ordering": "+ts",
        }
        mock_fetch_crop_data.assert_called_with(expected_params)

        # Verify the DataFrame returned
        self.assertTrue(df.equals(mock_df))

    @patch("agritechtz_cli.requests.get")
    def test_fetch_crop_data_http_error(self, mock_get):
        """Test '_fetch_crop_data' handles HTTP errors."""
        mock_get.side_effect = requests.RequestException("HTTP Error")
        with self.assertRaises(RuntimeError):
            _fetch_crop_data({})

    def test_invalid_date_format(self):
        """Test handling of invalid date formats."""
        with self.assertRaises(ValueError):
            self.builder.from_date("invalid-date")

    def test_invalid_order_by(self):
        """Test 'order_by' with invalid input."""
        self.builder.order_by("+invalid_field")
        params = self.builder._create_params_dict()
        self.assertIn("+invalid_field", params["ordering"])


if __name__ == "__main__":
    unittest.main()

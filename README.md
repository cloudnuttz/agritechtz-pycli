# Crop Price Client

This module implements a client for fetching and processing crop price data from a REST API. It allows filtering by crop types, regions, districts, dates, and ordering. The data is fetched as a CSV, converted into a pandas DataFrame, and preprocessed for analysis.


# Table of Contents

  1. [Features](#features)
  2. [Requirements](#requirements)
  3. [Installation](#installation)
  4. [Usage](#usage)
  5. [Classes](#classes)
  6. [Error Handling](#error-handling)
  7. [Logging](#logging)
  8. [Testing](#testing)
  9. [License](#license)
  10. [Contributing](#contributing)

# Features

  * Flexible Filtering: Filter crop price data by crops, regions, districts, date range, and ordering.
  * Chained Configuration: Use a chainable builder pattern for constructing DataFrames with specific parameters.
  * Progress Tracking: Displays a download progress bar.
  * Data Cleanup: Preprocesses and formats data for consistency.

# Requirements

  * Python 3.11+
  * Required Packages:
      * pandas
      * requests
      * tqdm

# Installation

To install this library, use pip:

```sh
pip install agritechtz-pycli
```

Or, if you’re installing from source:

```sh
git https://github.com/cloudnuttz/agritechtz-pycli.git
cd agritechtz-pycli
pip install .
```

# Usage

## Initialize the Builder:
The CropPriceDataFrameBuilder class allows you to configure and retrieve crop price data as a pandas DataFrame. Here’s an example of how to use it:

```python
from agritechtz import CropPriceDataFrameBuilder

df = CropPriceDataFrameBuilder.of("Maize", "Rice") \
    .in_regions("Dar es saalam/Kinondoni", "Mbeya/Sido") \
    .from_date("2023-01-01") \
    .to_date("2023-12-31") \
    .order_by("-ts") # Order data by date in descending order\
    .build()

print(df.head())
```

# Classes and Methods

## `CropPriceFilterParams`

 A data class for setting up filter parameters.

Parameters include:

|Attribute|Type|Description|
|---------|----|-----------|
|`crops`|`List[str]`|A list of crop names|
|`regions`|`List[str]`|A list of regions|
|`districts`|`List[str]`|A list of districts|
|`start_date`|`Union[date,str]`|Specify start date (ISO8601 format) of our time series data|
|`end_date`|`Union[date,str]`|Specify end date (ISO8601 format) of our time series data|
|`ordering`|`List[str]`|Specify sort priority. + or - before the parameter name where `+=ascending` `-=descending`|



## `CropPriceDataFrameBuilder`

A builder class for constructing crop price DataFrames with chainable configuration methods.

Methods:
|Method|Parameters|Return Type|Description|
|------|----------|-----------|-----------|
|`of`|Comma separated list of crops e.g., `"Maize","Rice"`|An instance `CropPriceDataFrameBuilder`|Crops to retrieve from the API|
|`in_regions`|Comma separated list of regions| The current instance created by the `of` method|Specify regions to include in your API query
|`from_date`|Date object or String representation of the date in ISO8601 format|The current instance created by the `of` method|Specify the starting date to include in your API query
|`end_date`|Date object or String representation of the date in ISO8601 format|The current instance create by the `of` method|Specify the end date to include in your API query. If specified togehter with the `start_date`, they will create time horizon for your time series data.
|`ordering`|Comma separated list of sort specifications. e.g., `"+ts"`|The current instance created by the `of` method.|Specify the order parameter to use in sorting. The sign before parameter indicates sorting direction `+/asc`, `-/desc`
|`build`|N/A|`DataFrame`|Builds the dataframe using the data from the API you've specified in the parameters above.

# Error Handling

The library raises informative errors in specific cases:

  * Date Format Errors: Raises ValueError for invalid date formats.
  * API Errors: Raises RuntimeError if network or request errors occur during data fetching.

> Note: Since this is a public API, we employed rate limit techniques in order to safeguard against Denial of service (DoD) attacks. Therefore, once you exceeds 5 req/minute you'll be locked.


# Logging

Logs messages at the CRITICAL level by default and outputs errors and warnings in case of invalid data or network issues.

# Testing

Run tests by typing `python -m unittest tests/test_crop_price_data_frame_builder.py`

# License

This project is licensed under the MIT License - see the LICENSE file for details.

---

# Contributing

Contributions are welcome! To contribute:

* Fork the repository.
* Create a new branch (`git checkout -b feature-branch`).
* Make your changes.
* Commit your changes (`git commit -m 'Add new feature'`).
* Push to the branch (`git push origin feature-branch`).
* Open a pull request.
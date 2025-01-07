###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

__version__ = "0.7.0"

from collections import OrderedDict
from copy import deepcopy
import csv
from datetime import datetime, timedelta
import hashlib
from io import BytesIO
import json
import logging
import os
import os.path
from pathlib import Path
import re
import tempfile
from typing import Iterator, Union

from cfunits import Units
from eccodes import (codes_bufr_new_from_file, codes_clone,
                     codes_get_array, codes_set, codes_write,
                     codes_release, codes_get,
                     CODES_MISSING_LONG, CODES_MISSING_DOUBLE,
                     codes_bufr_keys_iterator_new,
                     codes_bufr_keys_iterator_next,
                     codes_bufr_keys_iterator_delete, codes_definition_path,
                     codes_bufr_keys_iterator_get_name)

import numpy as np

LOGGER = logging.getLogger(__name__)

# some 'constants' / env variables
SUCCESS = True
NUMBERS = (float, int, complex)
MISSING = ("NA", "NaN", "NAN", "None")
NULLIFY_INVALID = os.environ.get("BUFR2GEOJSON_NULLIFY_INVALID", True)
THISDIR = os.path.dirname(os.path.realpath(__file__))
RESOURCES = f"{THISDIR}{os.sep}resources"
ASSOCIATED_FIELDS_FILE = f"{RESOURCES}{os.sep}031021.json"
CODETABLES = {}
FLAGTABLES = {}
ECCODES_DEFINITION_PATH = codes_definition_path()
if not os.path.exists(ECCODES_DEFINITION_PATH):
    LOGGER.debug('ecCodes definition path does not exist, trying environment')
    ECCODES_DEFINITION_PATH = os.environ.get('ECCODES_DEFINITION_PATH')
    LOGGER.debug(f'ECCODES_DEFINITION_PATH: {ECCODES_DEFINITION_PATH}')
    if ECCODES_DEFINITION_PATH is None:
        raise EnvironmentError('Cannot find ecCodes definition path')
TABLEDIR = Path(ECCODES_DEFINITION_PATH) / 'bufr' / 'tables' / '0' / 'wmo'

# TODO - read preferred units from config file
# PREFERRED UNITS
PREFERRED_UNITS = {
    "K": "Celsius",
    "Pa": "hPa"
}

# The following is required as the code table from ECMWF is incomplete
# and that from github/wmo-im not very usable.
try:
    with open(ASSOCIATED_FIELDS_FILE) as fh:
        ASSOCIATED_FIELDS = json.load(fh)
except Exception as e:
    LOGGER.error(f"Error loading associated field table (031021) - {e}")
    raise e

# list of BUFR attributes
ATTRIBUTES = ['code', 'units', 'scale', 'reference', 'width']

# Dictionary to store attributes for each element, caching is more
# efficient
_ATTRIBUTES_ = {}

# list of ecCodes keys for BUFR headers
HEADERS = ["edition", "masterTableNumber", "bufrHeaderCentre",
           "bufrHeaderSubCentre", "updateSequenceNumber", "dataCategory",
           "internationalDataSubCategory", "dataSubCategory",
           "masterTablesVersionNumber", "localTablesVersionNumber",
           "typicalYear", "typicalMonth", "typicalDay", "typicalHour",
           "typicalMinute", "typicalSecond", "typicalDate", "typicalTime",
           "numberOfSubsets", "observedData", "compressedData",
           "subsetNumber"]

UNEXPANDED_DESCRIPTORS = ["unexpandedDescriptors"]

# list of headers added by ECMWF and ecCodes
ECMWF_HEADERS = ["rdb", "rdbType", "oldSubtype", "localYear", "localMonth",
                 "localDay", "localHour", "localMinute", "localSecond",
                 "rdbtimeDay", "rdbtimeHour", "rdbtimeMinute",
                 "rdbtimeSecond", "rdbtimeTime", "rectimeDay", "rectimeHour",
                 "rectimeMinute", "rectimeSecond", "restricted",
                 "correction1", "correction1Part", "correction2",
                 "correction2Part", "correction3", "correction3Part",
                 "correction4", "correction4Part", "qualityControl",
                 "newSubtype", "localLongitude1", "localLatitude1",
                 "localLongitude2", "localLatitude2",
                 "localNumberOfObservations", "satelliteID"]

LOCATION_DESCRIPTORS = ["latitude", "latitude_increment",
                        "latitude_displacement", "longitude",
                        "longitude_increment", "longitude_displacement"]

ZLOCATION_DESCRIPTORS = ["height", "flight_level", "grid_point_altitude"]

RELATIVE_OBS_HEIGHT = ["height_above_station",
                       "height_of_sensor_above_local_ground_or_deck_of_marine_platform",  # noqa, land only
                       "height_of_sensor_above_water_surface",  # noqa, marine only
                       "depth_below_land_surface",
                       "depth_below_water_surface"
                       ]

OTHER_Z_DESCRIPTORS = ["geopotential", "pressure", "geopotential_height",
                       "water_pressure"]

TIME_DESCRIPTORS = ["year", "month", "day", "hour", "minute",
                    "second", "time_increment", "time_period"]

ID_DESCRIPTORS = ["block_number", "station_number",
                  "ship_or_mobile_land_station_identifier",
                  "wmo_region_sub_area", "region_number",
                  "buoy_or_platform_identifier",
                  "stationary_buoy_platform_identifier_e_g_c_man_buoys",
                  "marine_observing_platform_identifier",
                  "wigos_identifier_series", "wigos_issuer_of_identifier",
                  "wigos_issue_number", "wigos_local_identifier_character"]

WSI_DESCRIPTORS = ["wigos_identifier_series", "wigos_issuer_of_identifier",
                   "wigos_issue_number", "wigos_local_identifier_character"]

IDENTIFIERS_BY_TYPE = {
    # 0 surface data (land)
    "0": {
        "0": ["block_number", "station_number"],
        "1": ["block_number", "station_number"],
        "2": ["block_number", "station_number"],
        "3": ["ship_or_mobile_land_station_identifier"],
        "4": ["ship_or_mobile_land_station_identifier"],
        "5": ["ship_or_mobile_land_station_identifier"],
        "default": ["block_number", "station_number"]
    },

    # 1 surface data (sea)
    "1": {
        "0": ["ship_or_mobile_land_station_identifier"],
        "6": ["ship_or_mobile_land_station_identifier"],
        "7": ["ship_or_mobile_land_station_identifier"],
        "15": ["ship_or_mobile_land_station_identifier"],
        "25": [],
        "ship": ["ship_or_mobile_land_station_identifier"],
        "buoy_5digit": ["region_number", "wmo_region_sub_area", "buoy_or_platform_identifier"],  # noqa
        "buoy_7digit": ["stationary_buoy_platform_identifier_e_g_c_man_buoys"]
        # 7 digit id, 5 digit id (region, subarea, buoy id)
    },
    # 2 vertical sounding (other than satellite)
    "2": {
        "default": ["block_number", "station_number"]
    },
    # 31 oceanographic
    "31": {
        "default": ""
    }
}


# dictionary to store jsonpath parsers, these are compiled the first time that
# they are used.
jsonpath_parsers = dict()


# class to act as parser for BUFR data
class BUFRParser:
    def __init__(self, raise_on_error=False):

        self.raise_on_error = raise_on_error

        # dict to store qualifiers in force and for accounting, strictly only
        # those < 9 remain in force but some others in practice are assumed to
        self.qualifiers = {
            "01": {},  # identification
            "02": {},  # instrumentation
            "03": {},  # instrumenation
            "04": {},  # location (time)
            "05": {},  # location (horizontal 1)
            "06": {},  # location (horizontal 2)
            "07": {},  # location (vertical)
            "08": {},  # significance qualifiers
            "09": {},  # reserved
            "22": {},  # some sst sensors in class 22
            "25": {},  # processing information
            "31": {},  # associated field significance
            "33": {},  # BUFR/CREX quality information
            "35": {}  # data monitoring information
        }

    def set_qualifier(self, fxxyyy: str, key: str, value: Union[NUMBERS],
                      description: str, attributes: any, append: bool = False) -> None:  # noqa
        """
        Sets qualifier specified.

        :param fxxyyy: BUFR element descriptor of qualifier being set
        :param key: Plain text key of fxxyyy qualifier based on ecCodes library
        :param value: Numeric value of the qualifier
        :param description: Character value of the qualifier
        :param attributes: BUFR attributes (scale, reference value, width,
                           units etc) associated with element)
        :param append: Flag to indicate whether to append qualifier on to list
                       of values. Only valid for coordinates

        :returns: None
        """
        try:
            # get class of descriptor
            xx = fxxyyy[1:3]
            # first check whether the value is None, if so remove and exit
            if [value, description] == [None, None]:
                if key in self.qualifiers[xx]:
                    del self.qualifiers[xx][key]
            else:
                if key in self.qualifiers[xx] and append:
                    self.qualifiers[xx][key]["value"] = \
                        [self.qualifiers[xx][key]["value"], value]
                else:
                    self.qualifiers[xx][key] = {
                        "code": fxxyyy,
                        "key": key,
                        "value": value,
                        "attributes": attributes,
                        "description": description
                    }
        except Exception as e:
            LOGGER.error(f"Error in BUFRParser.set_qualifier: {e}")
            if self.raise_on_error:
                raise e

    def get_qualifier(self, xx: str, key: str, default=None) -> Union[NUMBERS]:
        """
        Function to get specified qualifier

        :param xx: class of the element to get
        :param key: textual key of element
        :param default: default value to use if qualifier is not set

        :returns: numeric value of qualifier
        """

        if key in self.qualifiers[xx]:
            if self.qualifiers[xx][key]["attributes"]["units"] == "CCITT IA5":
                value = self.qualifiers[xx][key]["description"]
            else:
                value = self.qualifiers[xx][key]["value"]
        else:
            LOGGER.debug(f"No value found for requested qualifier ({key}), setting to default ({default})")  # noqa
            value = default

        return value

    def get_qualifiers(self) -> dict:
        """
        Function to return all qualifiers set (excluding special qualifiers
        such as date and time)

        :returns: Dictionary containing qualifiers, their values and units -
                  grouped by class.
        """

        classes = list(self.qualifiers.keys())

        identification = {}
        wigos_md = {}
        qualifiers = {}
        processing = {}
        monitoring = {}
        quality = {}
        associated_field = {}

        # name, value, units
        for c in classes:
            for k in self.qualifiers[c]:
                #  skip special qualifiers handled elsewhere
                if k in LOCATION_DESCRIPTORS:
                    continue
                if k in TIME_DESCRIPTORS:
                    continue
                if k in ID_DESCRIPTORS:
                    continue
                if c in ("04", "05", "06"):  # , "07"):
                    LOGGER.warning(f"Unhandled location information {k}")
                # now remaining qualifiers
                value = self.qualifiers[c][k]["value"]
                units = self.qualifiers[c][k]["attributes"]["units"]
                description = self.qualifiers[c][k]["description"]
                try:
                    description = strip2(description)
                except AttributeError:
                    pass
                except Exception as e:
                    LOGGER.error(f"{e}")

                # set the qualifier value, result depends on type
                if units in ("CODE TABLE", "FLAG TABLE"):
                    q = {
                        "value": value.copy()
                    }
                elif units == "CCITT IA5":
                    q = {"value": description}
                else:
                    q = {
                        "value": value,
                        "units": units,
                        "description": description
                    }

                # now assign to type of qualifier
                if c == "01":
                    identification[k] = q.copy()
                if c in ("02", "03", "07", "22"):
                    wigos_md[k] = q.copy()
                if c in ("08", "09"):
                    qualifiers[k] = q.copy()
                if c == "25":
                    processing[k] = q.copy()
                if c == "31":
                    associated_field[k] = q.copy()
                if c == "33":
                    quality[k] = q.copy()
                if c == "35":
                    monitoring[k] = q.copy()

        result = {
            "identification": identification,
            "instrumentation": wigos_md,
            "qualifiers": qualifiers,
            "processing": processing,
            "monitoring": monitoring,
            "quality": quality,
            "associated_field": associated_field
        }

        return result

    def get_location(self, bufr_class: int = None) -> Union[dict, None]:
        """
        Function to get location from qualifiers and to apply any displacements
        or increments

        :returns: dictionary containing GeoJSON geometry or None
                  (if geometry contains null values/cannot be derived)
                  example: `{"type":"", "coordinates": [x,y,z?]}`
        """

        # first get latitude
        if "latitude" not in self.qualifiers["05"]:
            LOGGER.warning("Invalid location in BUFR message, no latitude")
            LOGGER.warning(self.qualifiers["05"])
            LOGGER.warning("latitude set to None")
            latitude = None
        else:
            latitude = deepcopy(self.qualifiers["05"]["latitude"])

        if latitude is not None:
            # check if we need to add a displacement
            if "latitude_displacement" in self.qualifiers["05"]:  # noqa
                y_displacement = deepcopy(self.qualifiers["05"]["latitude_displacement"])  # noqa
                latitude["value"] += y_displacement["value"]
            latitude = round(latitude["value"], latitude["attributes"]["scale"])  # noqa

        # now get longitude
        if "longitude" not in self.qualifiers["06"]:
            LOGGER.warning("Invalid location in BUFR message, no longitude")
            LOGGER.warning(self.qualifiers["06"])
            LOGGER.warning("longitude set to None")
            longitude = None
        else:
            longitude = deepcopy(self.qualifiers["06"]["longitude"])

        if longitude is not None:
            # check if we need to add a displacement
            if "longitude_displacement" in self.qualifiers["06"]:
                x_displacement = deepcopy(self.qualifiers["06"]["longitude_displacement"])  # noqa
                longitude["value"] += x_displacement["value"]
            # round to avoid extraneous digits
            longitude = round(longitude["value"], longitude["attributes"]["scale"])  # noqa

        z = self.get_zcoordinate(bufr_class)
        height = z.get('z_amsl', {}).get('value')

        # check for increments, not yet implemented
        if "005011" in self.qualifiers["05"] or \
                "005012" in self.qualifiers["05"] or \
                "006011" in self.qualifiers["06"] or \
                "006012" in self.qualifiers["06"]:
            raise NotImplementedError

        location = [longitude, latitude]

        if height is not None:
            location.append(height)

        if None in location:
            LOGGER.debug('geometry contains null values; setting to None')
            return None
        return {
            "type": "Point",
            "coordinates": location
        }

    def get_zcoordinate(self, bufr_class: int = None) -> Union[dict, None]:
        # class 07 gives vertical coordinate
        result = {}

        # 1) Height of sensor above local ground + height of station AMSL
        # 2) Height of barometer AMSL
        # 3) Height or altitude
        # 4) Geopotential
        # 5) Pressure
        # 6) Height above station + height of station AMSL
        # 7) Height
        # 8) Geopotential height


        station_ground = self.qualifiers["07"].get("height_of_station_ground_above_mean_sea_level",None)  # noqa

        abs_height = []
        if bufr_class == 10:
            if "height_of_barometer_above_mean_sea_level" in self.qualifiers["07"]:  # noqa
                abs_height.append("height_of_barometer_above_mean_sea_level")
        else:
            for k in ZLOCATION_DESCRIPTORS:
                if k in self.qualifiers["07"]:
                    abs_height.append(k)

        rel_height = []
        for k in RELATIVE_OBS_HEIGHT:
            if k in self.qualifiers["07"]:
                rel_height.append(k)

        other_height = []
        for k in OTHER_Z_DESCRIPTORS:
            if k in self.qualifiers["07"]:
                other_height.append(k)

        # if we have other heights we want to nullify abs and rel
        if len(other_height) == 1:
            abs_height = []
            rel_height = []

        # check we have as many heights as expected
        if len(abs_height) > 1:
            LOGGER.warning("Multiple absolute heights found, setting to None. See metadata")  # noqa
            abs_height = []

        if len(rel_height) > 1:
            LOGGER.warning("Multiple relative heights found, setting to None. See metadata")  # noqa
            rel_height = []

        if len(other_height) > 1:
            LOGGER.warning("Multiple other heights found, setting to None. See metadata")  # noqa
            other_height = []

        z_amsl = None
        z_alg = None
        z_other = None

        if len(rel_height) == 1 and station_ground is not None:
            assert station_ground.get('attributes').get('units') == self.qualifiers["07"].get(rel_height[0]).get('attributes').get('units')  # noqa
            z_amsl = station_ground.get('value') + self.qualifiers["07"].get(rel_height[0], {}).get('value')  # noqa
            z_alg = self.qualifiers["07"].get(rel_height[0], {}).get('value')
            if 'depth' in rel_height[0]:
                z_alg = -1 * z_alg
        elif len(abs_height) == 1 and station_ground is not None:
            z_amsl = self.qualifiers["07"].get(abs_height[0], {}).get('value')
            z_alg = z_amsl - station_ground.get('value')
        else:
            if len(abs_height) == 1:
                z_amsl = self.qualifiers["07"].get(abs_height[0], {}).get('value')  # noqa
            if len(rel_height) == 1:
                z_alg = self.qualifiers["07"].get(rel_height[0], {}).get('value')  # noqa

        if len(other_height) == 1:
            z_other = self.qualifiers["07"].get(other_height[0], {})

        if z_amsl is not None:
            result['z_amsl'] = {
                'name': 'height_above_mean_sea_level',
                'value': z_amsl,
                'units': 'm'
            }

        if z_other is not None:
            result['z'] = {
                'name': z_other.get('key'),
                'value': z_other.get('value'),
                'units': z_other.get('attributes').get('units')
            }
        elif z_alg is not None:
            result['z'] = {
                'name': 'height_above_local_ground',
                'value': z_alg,
                'units': 'm'
            }

        return result

    def get_time(self) -> str:
        """
        Function to get time from qualifiers and to apply any displacements or
        increments.

        :returns: ISO 8601 formatted date/time string
        """

        # class is always 04
        xx = "04"
        # get year
        year = self.get_qualifier(xx, "year")
        month = self.get_qualifier(xx, "month")
        day = self.get_qualifier(xx, "day", 1)
        hour = self.get_qualifier(xx, "hour", 0)
        minute = self.get_qualifier(xx, "minute", 0)
        second = self.get_qualifier(xx, "second", 0)
        # check we have valid date
        if None in [year, month, day, hour, minute, second]:
            msg = f"Invalid date ({year}-{month}-{day} {hour}:{minute}:{second}) in BUFR data"  # noqa
            LOGGER.error(msg)
            if self.raise_on_error:
                raise ValueError(msg)
            else:
                return msg

        if hour == 24:
            hour = 0
            offset = 1
            LOGGER.debug("Hour == 24 found in get time, increment day by 1")
        else:
            offset = 0
        time_ = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"  # noqa

        try:
            time_ = datetime.strptime(time_, "%Y-%m-%d %H:%M:%S")
            time_ = time_ + timedelta(days=offset)
        except Exception as e:
            LOGGER.error(e)
            LOGGER.debug(time_)
            raise e

        time_list = None

        # check if we have any increment descriptors, not yet supported
        # for date
        yyy = ("004011", "004012", "004013", "004014", "004015", "004016")
        for qualifier in yyy:
            if qualifier in self.qualifiers["04"]:
                LOGGER.error(qualifier)
                raise NotImplementedError

        # check if we have any displacement descriptors, years and months
        time_units = {
            "a": "years",
            "mon": "months",
            "d": "days",
            "h": "hours",
            "min": "minutes",
            "s": "seconds"
        }
        if "time_period" in self.qualifiers["04"]:
            displacement = self.qualifiers["04"]["time_period"]
            value = displacement["value"]
            units = displacement["attributes"]["units"]  # noqa
            units = time_units[units]
            if not isinstance(value, int):
                LOGGER.debug(f"DISPLACEMENT: {value}")
                LOGGER.debug(len(value))
                if len(value) > 2:
                    LOGGER.error("More than two time displacements")
                    raise NotImplementedError
            else:
                # many sequences only specify a single displacement when
                # they should indicate two. For example, average wind speed
                # over proceeding 10 minutes. If only negative single
                # displacement assume time period up to current time.
                if value < 0:
                    value = [value, 0]
                else:
                    value = [0, value]

            time_list = [None] * len(value)

            for tidx in range(len(value)):
                time_list[tidx] = deepcopy(time_)
                if units not in ("years", "months"):
                    kwargs = dict()
                    kwargs[units] = value[tidx]
                    time_list[tidx] = time_list[tidx] + timedelta(**kwargs)
                elif units == "years":
                    time_list[tidx].year += value[tidx]
                elif units == "months":
                    time_list[tidx].month += value[tidx]

        if time_list:
            if len(time_list) > 2:
                LOGGER.error("More than two times")
                raise NotImplementedError
            time_list[0] = time_list[0].strftime("%Y-%m-%dT%H:%M:%SZ")
            time_list[1] = time_list[1].strftime("%Y-%m-%dT%H:%M:%SZ")
            time_ = f"{time_list[0]}/{time_list[1]}"
        else:
            # finally convert datetime to string
            time_ = time_.strftime("%Y-%m-%dT%H:%M:%SZ")

        return time_

    def get_wsi(self, guess_wsi: bool = False) -> str:
        """
        Function returns WIGOS station ID as string

        :returns: WIGOS station ID.
        """

        return self.get_identification(guess_wsi)["wsi"]

    def get_tsi(self) -> str:
        """
        Function returns Traditional station identifier as string

        :returns: Traditional station ID.
        """

        return self.get_identification()["tsi"]

    def get_identification(self, guess_wsi: bool = False) -> dict:
        """
        Function extracts identification information from qualifiers.

        :returns: dictionary containing any class 01 qualifiers and WSI as dict.  # noqa
        """

        # default WSI value
        wsi = None

        # see https://library.wmo.int/doc_num.php?explnum_id=11021
        # page 19 for allocation of WSI if not set
        # check to see what identification we have
        # WIGOS id
        # 001125, 001126, 001127, 001128
        if all(x in self.qualifiers["01"] for x in ("wigos_identifier_series",
                                                    "wigos_issuer_of_identifier",  # noqa
                                                    "wigos_issue_number", "wigos_local_identifier_character")):  # noqa
            wsi_series = self.get_qualifier("01", "wigos_identifier_series")
            wsi_issuer = self.get_qualifier("01", "wigos_issuer_of_identifier")
            wsi_number = self.get_qualifier("01", "wigos_issue_number")
            #wsi_local = self.qualifiers["01"]["wigos_local_identifier_character"]["description"]  # noqa
            wsi_local = strip2(self.get_qualifier("01", "wigos_local_identifier_character"))  # noqa
            return {
                "wsi": f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}",
                "tsi": wsi_local,
                "type": "wigos_station_identifier"
            }

        # block number and station number
        # 001001, 001002
        _types = ("block_number", "station_number")
        if all(x in self.qualifiers["01"] for x in _types):  # noqa
            block = self.get_qualifier("01", "block_number")
            station = self.get_qualifier("01", "station_number")
            tsi = strip2(f"{block:02d}{station:03d}")
            if guess_wsi:
                wsi_series = 0
                wsi_issuer = 20000
                wsi_number = 0
                wsi_local = tsi
                wsi = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"

            return {
                "wsi": wsi,
                "tsi": tsi,
                "type": "{}_and_{}".format(*_types)
            }

        # ship or mobile land station identifier (001011)
        _type = "ship_or_mobile_land_station_identifier"
        if _type in self.qualifiers["01"]:
            callsign = self.get_qualifier("01", _type)
            tsi = strip2(callsign)
            if guess_wsi:
                wsi_series = 0
                wsi_issuer = 20004
                wsi_number = 0
                wsi_local = tsi
                wsi = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"

            return {
                "wsi": wsi,
                "tsi": tsi,
                "type": _type
            }

        # 5 digit buoy number
        # 001003, 001020, 001005
        _types = ("region_number", "wmo_region_sub_area",
                  "buoy_or_platform_identifier")
        if all(x in self.qualifiers["01"] for x in _types):
            wmo_region = self.get_qualifier("region_number")
            wmo_subregion = self.get_qualifier("wmo_region_sub_area")
            wmo_number = self.get_qualifier("buoy_or_platform_identifier")
            tsi = strip2(f"{wmo_region:01d}{wmo_subregion:01d}{wmo_number:05d}")  # noqa
            if guess_wsi:
                wsi_series = 0
                wsi_issuer = 20002
                wsi_number = 0
                wsi_local = tsi  # noqa
                wsi = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"

            return {
                "wsi": wsi,
                "tsi": tsi,
                "type": "5_digit_marine_observing_platform_identifier"
            }

        # station buoy identifier
        # 001010
        _type = "stationary_buoy_platform_identifier_e_g_c_man_buoys"
        if _type in self.qualifiers["01"]:
            id_ = self.get_qualifier("01", _type)
            tsi = strip2(id_)
            if guess_wsi:
                wsi_series = 0
                wsi_issuer = 20002
                wsi_number = 0
                wsi_local = tsi
                wsi = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"

            return {
                "wsi": wsi,
                "tsi": tsi,
                "type": _type
            }

        # 7 digit buoy number
        # 001087
        _type = "7_digit_marine_observing_platform_identifier"
        if _type in self.qualifiers["01"]:
            id_ = self.get_qualifier("01", _type)
            tsi = strip2(id_)
            if guess_wsi:
                wsi_series = 0
                wsi_issuer = 20002
                wsi_number = 0
                wsi_local = tsi
                wsi = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"

            return {
                "wsi": wsi,
                "tsi": tsi,
                "type": _type
            }

        # flag if we do not have WSI
        LOGGER.debug(self.qualifiers["01"])
        return {"wsi": None, "tsi": None, "type": None}

    def get_code_value(self, fxxyyy: str, code: int) -> str:
        """
        Gets decoded value for BUFR element

        :param fxxyyy: FXXYYY BUFR descriptor
        :param code: value to decode and convert to string representation

        :returns: string representation of coded value
        """
        if code is None:
            return None
        table = int(fxxyyy)

        if self.table_version not in CODETABLES:
            CODETABLES[self.table_version] = {}

        if fxxyyy not in CODETABLES[self.table_version]:
            CODETABLES[self.table_version][fxxyyy] = {}
            tablefile = TABLEDIR / str(self.table_version) / 'codetables' / f'{table}.table'  # noqa
            with tablefile.open() as csvfile:
                reader = csv.reader(csvfile, delimiter=" ")
                for row in reader:
                    CODETABLES[self.table_version][fxxyyy][int(row[0])] = " ".join(row[2:])  # noqa

        if code not in CODETABLES[self.table_version][fxxyyy]:
            LOGGER.warning(f"Invalid entry for value {code} in code table {fxxyyy}, table version {self.table_version}")  # noqa
            decoded = "Invalid"
        else:
            decoded = CODETABLES[self.table_version][fxxyyy][code]

        return decoded

    def get_flag_value(self, fxxyyy: str, flags: str) -> str:
        if flags is None:
            return None
        table = int(fxxyyy)
        if self.table_version not in FLAGTABLES:
            FLAGTABLES[self.table_version] = {}

        if fxxyyy not in FLAGTABLES[self.table_version]:
            FLAGTABLES[self.table_version][fxxyyy] = {}
            tablefile = TABLEDIR / str(self.table_version) / 'codetables' / f'{table}.table'  # noqa
            with tablefile.open() as csvfile:
                reader = csv.reader(csvfile, delimiter=" ")
                for row in reader:
                    FLAGTABLES[self.table_version][fxxyyy][int(row[0])] = " ".join(row[2:])  # noqa

        flag_table = FLAGTABLES[self.table_version][fxxyyy]

        bits = [int(flag) for flag in flags]
        nbits = len(bits)
        values = []
        for idx in range(nbits):
            if bits[idx]:
                key = idx+1
                value = flag_table.get(key)
                if value is not None:
                    values.append(value)

        return values

    def as_geojson(self, bufr_handle: int, id: str,
                   guess_wsi: bool = False) -> dict:
        """
        Function to return GeoJSON representation of BUFR message

        :param bufr_handle: integer handle for BUFR data (used by eccodes)
        :param id: id to assign to feature collection
        :param serialize: whether to return as JSON string (default is False)

        :returns: dictionary containing GeoJSON feature collection
        """

        # check we have data
        if not bufr_handle:
            LOGGER.warning("Empty BUFR")
            return {}

        LOGGER.debug(f"Processing {id}")

        # unpack the message
        codes_set(bufr_handle, "unpack", True)

        # get table version
        try:
            self.table_version = codes_get(bufr_handle,
                                           "masterTablesVersionNumber")
        except Exception as e:
            LOGGER.error("Unable to read table version number")
            LOGGER.error(e)
            raise e

        # get number of subsets
        nsubsets = codes_get(bufr_handle, "numberOfSubsets")
        LOGGER.debug(f"as_geojson.nsubsets: {nsubsets}")
        try:
            assert nsubsets == 1
        except Exception:
            LOGGER.error(f"Too many subsets in call to as_geojson ({nsubsets})")  # noqa

        # Load headers
        headers = OrderedDict()
        for header in HEADERS:
            try:
                headers[header] = codes_get(bufr_handle, header)
            except Exception as e:
                if header == "subsetNumber":
                    continue
                LOGGER.error(f"Error reading {header}")
                raise e

        self.reportType = headers.get('dataCategory')

        # characteristic_date = headers["typicalDate"]
        # characteristic_time = headers["typicalTime"]

        try:
            sequence = codes_get_array(bufr_handle, UNEXPANDED_DESCRIPTORS[0])
            sequence = sequence.tolist()
        except Exception as e:
            LOGGER.error(f"Error reading {UNEXPANDED_DESCRIPTORS}")
            raise e
        # convert to string
        sequence = [f"{descriptor}" for descriptor in sequence]
        sequence = ",".join(sequence)
        headers["sequence"] = sequence
        LOGGER.debug(sequence)

        # now get key iterator
        key_iterator = codes_bufr_keys_iterator_new(bufr_handle)

        # set up data structures
        last_key = None
        index = 0

        # iterate over keys and add to dict
        while codes_bufr_keys_iterator_next(key_iterator):
            # get key
            key = codes_bufr_keys_iterator_get_name(key_iterator)
            if "associatedField" in key:  # we've already processed, skip
                last_key = key
                continue

            # identify what we are processing
            if key in (HEADERS + ECMWF_HEADERS + UNEXPANDED_DESCRIPTORS):
                continue
            else:  # data descriptor
                try:
                    fxxyyy = codes_get(bufr_handle, f"{key}->code")
                except Exception as e:
                    LOGGER.warning(f"Error reading {key}->code, skipping element: {e}")  # noqa
                    continue

            # get class
            # get class etc
            f = int(fxxyyy[0:1])
            xx = int(fxxyyy[1:3])
            yyy = int(fxxyyy[3:6])

            # because of the way eccode works we need to check for associated
            # fields. These are returned after
            associated_field = None
            try:
                associated_field_value = codes_get(bufr_handle, f"{key}->associatedField")  # noqa
                associated_field = codes_get(bufr_handle, f"{key}->associatedField->associatedFieldSignificance")  # noqa
                associated_field = f"{associated_field}"
                associated_field = ASSOCIATED_FIELDS.get(associated_field)
            except Exception:
                pass

            if associated_field is not None:
                flabel = associated_field.get('label', '')
                ftype = associated_field.get('type', '')
                if ftype == 'int':
                    associated_field_value = f"{int(associated_field_value)}"
                    associated_field_value = \
                        associated_field.get('values',{}).get(associated_field_value, '')  # noqa
                else:
                    funits = associated_field.get('units', '')
                    associated_field_value = f"{associated_field_value} {funits}"  # noqa
                quality_flag = {
                    'inScheme': "https://codes.wmo.int/bufr4/codeflag/0-31-021",  # noqa
                    'flag': flabel,
                    'flagValue': associated_field_value
                }
            else:
                quality_flag = {
                    'inScheme': None,
                    'flag': None,
                    'flagValue': None
                }

            assert f == 0
            # get value and attributes
            # get as array and convert to scalar if required
            value = codes_get_array(bufr_handle, key)
            _value = None
            if (len(value) == 1) and (not isinstance(value, str)):
                value = value[0]
                if value in (CODES_MISSING_DOUBLE, CODES_MISSING_LONG):
                    value = None
                # now convert to regular python types as json.dumps doesn't
                # like numpy
                if isinstance(value, np.float64):
                    value = float(value)
                elif isinstance(value, np.int64):
                    value = int(value)
            else:
                assert False

            # get attributes
            attributes = {}
            if fxxyyy in _ATTRIBUTES_:
                attributes = _ATTRIBUTES_[fxxyyy]
                attributes = attributes.copy()
            else:
                for attribute in ATTRIBUTES:
                    attribute_key = f"{key}->{attribute}"
                    try:
                        attribute_value = codes_get(bufr_handle, attribute_key)
                    except Exception as e:
                        LOGGER.warning(f"Error reading {attribute_key}: {e}")
                        attribute_value = None
                    if attribute_value is not None:
                        attributes[attribute] = attribute_value
                _ATTRIBUTES_[fxxyyy] = attributes.copy()

            units = attributes["units"]
            # scale = attributes["scale"]

            # next decoded value if from code table
            description = None
            observation_type = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"  # noqa default type
            if attributes["units"] == "CODE TABLE" and value is not None:
                description = self.get_code_value(attributes["code"], value)
                observation_type = "http//www.opengis.net/def/observationType/OGC-OM/2.0/OM_CategoryObservation"  # noqa
                _value = {
                    'codetable': f"http://codes.wmo.int/bufr4/codeflag/{f:1}-{xx:02}-{yyy:03}",  # noqa
                    'entry': f"{value}",  # noqa
                    'description': description
                }
            elif attributes["units"] == "FLAG TABLE" and value is not None:
                observation_type = "http//www.opengis.net/def/observationType/OGC-OM/2.0/OM_CategoryObservation"  # noqa
                nbits = attributes['width']
                description = self.get_flag_value(attributes["code"], "{0:0{1}b}".format(value, nbits))  # noqa
                _value = {
                    'flagtable': f"http://codes.wmo.int/bufr4/codeflag/{f:1}-{xx:02}-{yyy:03}",  # noqa
                    'entry': "{0:0{1}b}".format(value, nbits),
                    'description': description
                }
            elif attributes["units"] == "CCITT IA5":
                description = value
                value = None
                observation_type = "http//www.opengis.net/def/observationType/OGC-OM/2.0/OM_Observation"  # noqa

            if (units in PREFERRED_UNITS) and (value is not None):
                value = Units.conform(value, Units(units),
                                      Units(PREFERRED_UNITS[units]))
                # round to 6 d.p. to remove any erroneous digits
                # due to IEEE arithmetic
                value = round(value, 6)
                units = PREFERRED_UNITS[units]
                attributes["units"] = units

            if _value is not None:
                value = _value.copy()

            # now process, convert key to snake case
            key = re.sub("#[0-9]+#", "", key)
            key = re.sub("([a-z])([A-Z])", r"\1_\2", key)
            key = key.lower()

            # determine whether we have data or metadata
            append = False
            if xx < 9 and fxxyyy != '004053':  # noqa - metadata / significance qualifiers. 0040552 is misplaced, it is not a time coordinate!
                if ((xx >= 4) and (xx < 8)) and (key == last_key):
                    append = True

                if fxxyyy == "004023" and sequence == "307075":  # noqa fix for broken DAYCLI sequence
                    self.set_qualifier(fxxyyy, key, value, description,
                                       attributes, append)
                    self.set_qualifier(fxxyyy, key, value+1, description,
                                       attributes, append)
                else:
                    self.set_qualifier(fxxyyy, key, value, description,
                                       attributes, append)
                last_key = key
                continue
            elif xx == 31:
                if yyy in (12, 31):
                    raise NotImplementedError
                last_key = key
                continue
            elif xx in (25, 33, 35):
                self.set_qualifier(fxxyyy, key, value, description,
                                   attributes, append)
                last_key = key
                continue

            if fxxyyy == ("022067", "022055", "022056", "022060",
                          "022068", "022080", "022081", "022078",
                          "022094", "022096"):
                append = False
                self.set_qualifier(fxxyyy, key, value, description,
                                   attributes, append)
                last_key = key
                continue

            if value is not None:
                # self.get_identification()
                metadata = self.get_qualifiers()
                metadata["BUFR_element"] = fxxyyy
                z = self.get_zcoordinate(bufr_class=xx)
                if z is not None:
                    metadata["zCoordinate"] = z.get('z')
                metadata['BUFRheaders'] = headers
                observing_procedure = "http://codes.wmo.int/wmdr/SourceOfObservation/unknown"  # noqa

                wsi = self.get_wsi(guess_wsi)
                host_id = wsi
                if wsi is None:
                    wsi = "UNKNOWN"  #
                    host_id = self.get_tsi()
                feature_id = f"{index}"

                try:
                    phenomenon_time = self.get_time()
                except Exception as e:
                    LOGGER.warning(
                        f"Error getting phenomenon time, skipping ({e})")
                    continue

                result_time = datetime.now().strftime('%Y-%m-%d %H:%M')

                # check if we have statistic, if so modify observed_property
                fos = self.get_qualifier("08", "first_order_statistics", None)
                observed_property = f"{key}"
                if fos is not None:
                    fos = fos.get("description", "")
                    observed_property = f"{key} ({fos.lower()})"

                data = {
                    "geojson": {
                        "id": feature_id,
                        "conformsTo": ["https://wis.wmo.int/spec/wccdm-obs/1/conf/observation"],  # noqa
                        "type": "Feature",
                        "geometry": self.get_location(bufr_class=xx),
                        "properties": {
                            "host": host_id,  # noqa
                            "observer": None,
                            "observationType": observation_type,  # noqa
                            "observedProperty": observed_property,
                            "observingProcedure": observing_procedure,
                            "phenomenonTime": phenomenon_time,
                            "resultTime": result_time,
                            "validTime": None,
                            "result": {
                                "value": value,
                                "units": attributes["units"],
                                "standardUncertainty": None
                            },
                            "resultQuality": [
                                quality_flag
                            ],
                            "parameter": {
                                "hasProvenance": None,
                                "status": None,
                                "version": 0,
                                "comment": None,
                                "reportType": f"{headers['dataCategory']:03}{headers['internationalDataSubCategory']:03}",  # noqa
                                "reportIdentifier": f"{id}",
                                "isMemberOf": None,
                                "additionalProperties": metadata
                            },
                            "featureOfInterest": [
                                {
                                    "id": None,
                                    "label": None,
                                    "relation": None
                                }
                            ],
                            "index": index,
                        }
                    },
                    "_meta": {
                        "data_date": self.get_time(),
                        "identifier": feature_id,
                        "geometry": self.get_location()
                    },
                    "_headers": headers
                }
                yield data
                last_key = key
                index += 1
        codes_bufr_keys_iterator_delete(key_iterator)


def transform(data: bytes, guess_wsi: bool = False,
              source_identifier: str = "") -> Iterator[dict]:
    """
    Main transformation

    :param data: byte string of BUFR data
    :param guess_wsi: whether to 'guess' WSI based on TSI and allocation rules
    :param source_identifier: identifier of the source (eg. filename ( file ID)

    :returns: `generator` of GeoJSON features
    """

    error = False

    # eccodes needs to read from a file, create a temporary fiole
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'wb') as f:
        f.write(data)

    # check data type, only in situ supported (not yet implemented)
    # split subsets into individual messages and process
    imsg = 0
    messages_remaining = True
    with open(tmp.name, 'rb') as fh:
        # get first message
        bufr_handle = codes_bufr_new_from_file(fh)
        if bufr_handle is None:
            LOGGER.warning("No messages in file")
            messages_remaining = False
        while messages_remaining:
            messages_remaining = False  # noqa set to false to prevent infinite loop by accident
            imsg += 1
            LOGGER.info(f"Processing message {imsg} from file")

            try:
                codes_set(bufr_handle, "unpack", True)
            except Exception as e:
                LOGGER.error("Error unpacking message")
                LOGGER.error(e)
                error = True

            if not error:
                nsubsets = codes_get(bufr_handle, "numberOfSubsets")
                LOGGER.info(f"{nsubsets} subsets")

                for idx in range(nsubsets):
                    # reportIdentifier = None
                    if nsubsets > 1:  # noqa this is only required if more than one subset (and will crash if only 1)
                        LOGGER.debug(f"Extracting subset {idx+1} of {nsubsets}")  # noqa
                        codes_set(bufr_handle, "extractSubset", idx+1)
                        codes_set(bufr_handle, "doExtractSubsets", 1)
                        LOGGER.debug("Cloning subset to new message")

                    single_subset = codes_clone(bufr_handle)

                    with BytesIO() as bufr_bytes:
                        codes_write(single_subset, bufr_bytes)
                        bufr_bytes.seek(0)
                        bhash = hashlib.md5()
                        bhash.update(bufr_bytes.getvalue())
                        reportIdentifier = bhash.hexdigest()

                    LOGGER.debug("Unpacking")
                    codes_set(single_subset, "unpack", True)

                    parser = BUFRParser()

                    tag = reportIdentifier
                    try:
                        data = parser.as_geojson(single_subset, id=tag,
                                                 guess_wsi=guess_wsi)  # noqa

                    except Exception as e:
                        LOGGER.error("Error parsing BUFR to GeoJSON, no data written")  # noqa
                        LOGGER.error(e)
                        data = {}

                    for obs in data:
                        # noqa set identifier, and report id (prepending file and subset numbers)
                        id = obs.get('geojson', {}).get('id', {})
                        if source_identifier in ("", None):
                            source_identifier = obs.get('geojson', {}).get('properties',{}).get('host', "")  # noqa
                        obs['geojson']['id'] = f"{reportIdentifier}-{id}"  # noqa update feature id to include report id
                        # now set prov data
                        prov = {
                            "prefix": {
                                "prov": "http://www.w3.org/ns/prov#",
                                "schema": "https://schema.org/"
                            },
                            "entity": {
                                f"{source_identifier}": {
                                    "prov:type": "schema:DigitalDocument",
                                    "prov:label": "Input data file",
                                    "schema:encodingFormat": "application/bufr"
                                },
                                f"{obs['geojson']['id']}": {
                                    "prov:type": "observation",
                                    "prov:label": f"Observation {id} from subset {idx} of message {imsg}"  # noqa
                                }
                            },
                            "wasDerivedFrom": {
                                "_:wdf": {
                                    "prov:generatedEntity": f"{obs['geojson']['id']}",  # noqa
                                    "prov:usedEntity": f"{source_identifier}",
                                    "prov:activity": "_:bufr2geojson"
                                }
                            },
                            "activity": {
                                "_:bufr2geojson": {
                                    "prov:type": "prov:Activity",
                                    "prov:label": f"Data transformation using version {__version__} of bufr2geojson",  # noqa
                                    "prov:endTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # noqa
                                }
                            }
                        }
                        obs['geojson']['properties']['parameter']['hasProvenance'] = prov.copy()  # noqa
                        yield obs

                    del parser
                    codes_release(single_subset)
            else:
                yield {}

            if not error:
                codes_release(bufr_handle)

            bufr_handle = codes_bufr_new_from_file(fh)

            if bufr_handle is not None:
                messages_remaining = True

        LOGGER.info(f"{imsg} messages processed from file")


def strip2(value) -> str:
    """
    Strip string and throw warning if space padded

    :returns: `str` of stripped value
    """

    if value is None:
        return None

    if isinstance(value, str):
        pass  # space = ' '
    elif isinstance(value, bytes):
        #  space = b' '
        pass
    else:  # make sure we have a string
        #  space = ' '
        value = f"{value}"

    return value.strip()

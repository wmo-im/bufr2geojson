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

__version__ = "0.0.1"

from collections import OrderedDict
from copy import deepcopy
import csv
from datetime import timezone, datetime, timedelta
import gc as gc
import hashlib
from io import StringIO, BytesIO
import json
import logging
import os.path
import re
from typing import Any, Iterator, Union
from uuid import uuid4
from pathlib import Path

import sys

from cfunits import Units
from eccodes import (codes_bufr_new_from_file, codes_clone,
                     codes_get_array, codes_set, codes_get_native_type,
                     codes_write, codes_release, codes_get,
                     CODES_MISSING_LONG, CODES_MISSING_DOUBLE,
                     codes_bufr_keys_iterator_new,
                     codes_bufr_keys_iterator_next,
                     codes_bufr_keys_iterator_delete,
                     codes_bufr_keys_iterator_get_name, CodesInternalError)
from jsonpath_ng.ext import parser
from jsonschema import validate
import numpy as np
import pandas as pd

# only used in dev version
# pandas config
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

# some 'constants'
SUCCESS = True
NUMBERS = (float, int, complex)
MISSING = ("NA", "NaN", "NAN", "None")
FAIL_ON_ERROR = False
NULLIFY_INVALID = True  # TODO: move to env. variable

LOGGER = logging.getLogger(__name__)

BUFR_TABLE_VERSION = 37  # default BUFR table version
THISDIR = os.path.dirname(os.path.realpath(__file__))
TABLES = f"{THISDIR}{os.sep}resources{os.sep}bufr{os.sep}{BUFR_TABLE_VERSION}"  # noqa

# PREFERRED UNITS
PREFERRED_UNITS = {
    "K": "Celsius",
    "Pa": "hPa"
}

# list of BUFR attributes
ATTRIBUTES = ['code', 'units', 'scale', 'reference', 'width']

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

LOCATION_DESCRIPTORS = ["latitude", "latitude_increment", "latitude_displacement",  # noqa
                        "longitude", "longitude_increment", "longitude_displacement",  # noqa
                        "height_of_station_ground_above_mean_sea_level"]

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

# dictionary to store jsonpath parsers, these are compiled the first time that
# they are used.
jsonpath_parsers = dict()


# class to act as parser for BUFR data
class BUFRParser:
    def __init__(self):
        # load BUFR tables
        self.code_table = pd.read_csv(f"{TABLES}{os.sep}BUFRCREX_CodeFlag_en.txt", dtype="object")  # noqa
        # strip out non numeric rows, these typically give a range
        numeric_rows = self.code_table["CodeFigure"].apply(
            lambda x: (not np.isnan(x)) if isinstance(x, (int, float)) else x.isnumeric() )  # noqa
        self.code_table = self.code_table.loc[numeric_rows, ]
        # now convert to integers for matching
        self.code_table = self.code_table.astype({"CodeFigure": "int"})
        # dict to store qualifiers in force and for accounting
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
            "22": {}  # some sst sensors in class 22
        }

    def set_qualifier(self, fxxyyy: str, key: str, value: Union[NUMBERS],
                      description: str, attributes: any, append: bool = False) -> None:  # noqa
        """
        Sets qualifier specified.

        :param fxxyyy: BUFR element descriptor of qualifier being set
        :param key: Plain text key of fxxyyy qualifier based on ecCodes library  # noqa
        :param value: Numeric value of the qualifier
        :param description: Character value of the qualifier
        :param attributes: BUFR attributes (scale, reference value, width, units etc) associated with element  # noqa
        :param append: Flag to indicate whether to append qualifier on to list of values. Only valid for coordinates.  # noqa

        :returns: None
        """
        # get class of descriptor
        xx = fxxyyy[1:3]
        # first check whether the value is None, if so remove and exit
        if (value is None) and (description is None):
            if key in self.qualifiers[xx]:
                del self.qualifiers[xx][key]
        else:
            if key in self.qualifiers[xx] and append:
                self.qualifiers[xx][key]["value"] = \
                    [self.qualifiers[xx][key]["value"], value]  # noqa
            else:
                self.qualifiers[xx][key] = {
                    "code": fxxyyy,
                    "key": key,
                    "value": value,
                    "attributes": attributes,
                    "description": description
                }

    def set_time_displacement(self, key, value, append=False):
        raise NotImplementedError

    def get_qualifer(self, xx: str, key: str, default=None) -> Union[NUMBERS]:
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

    def get_qualifiers(self) -> list:
        """
        Function to return all qualifiers set (excluding special qualifiers
        such as date and time)

        :returns: List containing qualifiers, their values and units
        """

        classes = ("01", "02", "03", "04", "05", "06", "07", "08", "22")
        result = list()
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
                # now remaining qualifiers
                name = k
                value = self.qualifiers[c][k]["value"]
                units = self.qualifiers[c][k]["attributes"]["units"]
                description = self.qualifiers[c][k]["description"]
                q = {
                    "name": name,
                    "value": value,
                    "units": units,
                    "description": description
                }
                result.append(q)
        return result

    def get_location(self) -> dict:  # special rules for the definition of the location
        """
        Function to get location from qualifiers and to apply any displacements
        or increments

        :returns: dictionary containing geosjon geom ({"type":"", "coordinates": [x,y,z?]})  # noqa
        """
        # first get latitude
        #if not (("005001" in self.qualifiers["05"]) ^ ("005002" in self.qualifiers["05"])):  # noqa
        if "latitude" not in self.qualifiers["05"]:
            LOGGER.warn("Invalid location in BUFR message, no latitude")
            LOGGER.warn(self.qualifiers["05"])
            LOGGER.warn("latitude set to None")
            latitude = None
            #raise
        else:
            latitude = deepcopy(self.qualifiers["05"]["latitude"])

        if latitude is not None:
            # check if we need to add a displacement
            if "latitude_displacement" in self.qualifiers["05"]:  # noqa
                y_displacement = deepcopy(self.qualifiers["05"]["latitude_displacement"])  # noqa
                latitude["value"] += y_displacement["value"]
            latitude = round(latitude["value"], latitude["attributes"]["scale"])

        # now get longitude
        if "longitude" not in self.qualifiers["06"]:
            LOGGER.warn("Invalid location in BUFR message, no longitude")
            LOGGER.warn(self.qualifiers["06"])
            LOGGER.warn("longitude set to None")
            longitude = None
        else:
            longitude = self.qualifiers["06"]["longitude"]

        if longitude is not None:
            # check if we need to add a displacement
            if "longitude_displacement" in self.qualifiers["06"]:
                x_displacement = deepcopy(self.qualifiers["06"]["longitude_displacement"])  # noqa
                longitude["value"] += x_displacement["value"]
            # round to avoid extraneous digits
            longitude = round(longitude["value"], longitude["attributes"]["scale"])  # noqa

        # now station elevation
        if "height_of_station_ground_above_mean_sea_level" in self.qualifiers["07"]:  # noqa
            elevation = self.qualifiers["07"]["height_of_station_ground_above_mean_sea_level"]  # noqa
            elevation = round(elevation["value"], elevation["attributes"]["scale"])  # noqa
        else:
            elevation = None
        # no elevation displacement in BUFR

        # check for increments, not yet implemented
        if "005011" in self.qualifiers["05"] or \
                "005012" in self.qualifiers["05"] or \
                "006011" in self.qualifiers["06"] or \
                "006012" in self.qualifiers["06"]:
            raise NotImplementedError

        if elevation is not None:
            location = [longitude, latitude, elevation]
        else:
            location = [longitude, latitude]

        geom = {
            "type": "Point",
            "coordinates": location
        }

        return geom

    def get_time(self) -> str:
        """
        Function to get time from qualifiers and to apply any displacements or
        increments.

        :returns: ISO 8601 formatted date/time string
        """
        # class is always 04
        xx = "04"
        # get year
        year = self.get_qualifer(xx, "year")
        month = self.get_qualifer(xx, "month")
        day = self.get_qualifer(xx, "day", 1)
        hour = self.get_qualifer(xx, "hour", 0)
        minute = self.get_qualifer(xx, "minute", 0)
        second = self.get_qualifer(xx, "second", 0)
        if hour == 24:
            hour = 0
            offset = 1
            LOGGER.debug("Hour == 24 found in get time, increment day by 1")
        else:
            offset = 0
        time = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"  # noqa
        time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        time = time + timedelta(days=offset)
        time_list = None
        # check if we have any increment descriptors, not yet supported for date  # noqa
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
                time_list[tidx] = deepcopy(time)
                if units not in ("years", "months"):
                    kwargs = dict()
                    kwargs[units] = value[tidx]
                    LOGGER.debug(kwargs)
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
            time = f"{time_list[0]}/{time_list[1]}"
        else:
            # finally convert datetime to string
            time = time.strftime("%Y-%m-%dT%H:%M:%SZ")

        return time

    def get_wsi(self) -> str:
        """
        Function returns WIGOS station ID as string

        :returns: WIGOS station ID.
        """

        return self.get_identification()["wsi"]

    def get_identification(self) -> dict:
        """
        Function extracts identification information from qualifiers.

        :returns: dictionary containing any class 01 qualifiers and WSI as dict.  # noqa
        """
        # see https://library.wmo.int/doc_num.php?explnum_id=11021
        # page 19 for allocation of WSI if not set
        # check to see what identification we have
        # WIGOS id
        # 001125, 001126, 001127, 001128
        station_id = dict()
        if all(x in self.qualifiers["01"] for x in ("wigos_identifier_series",
                                                    "wigos_issuer_of_identifier",  # noqa
                                                    "wigos_issue_number", "wigos_local_identifier_character")):  # noqa
            wsi_series = self.get_qualifer("01", "wigos_identifier_series")
            wsi_issuer = self.get_qualifer("01", "wigos_issuer_of_identifier")
            wsi_number = self.get_qualifer("01", "wigos_issue_number")
            #wsi_local = self.qualifiers["01"]["wigos_local_identifier_character"]["description"]  # noqa
            wsi_local = self.get_qualifer("01", "wigos_local_identifier_character")  # noqa
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
        else:
            wigosID = None
        # block number and station number
        # 001001, 001002
        if all(x in self.qualifiers["01"] for x in ("block_number", "station_number")):  # noqa
            block = self.get_qualifer("01", "block_number")
            station = self.get_qualifer("01", "station_number")
            wsi_series = 0
            wsi_issuer = 20000
            wsi_number = 0
            wsi_local = f"{block:02d}{station:03d}"
            if wigosID is None:
                wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"  # noqa

            station_id["tsi"] = {
                "block": block,
                "station": station
            }
        # ship or mobile land station identifier (001011)
        if "ship_or_mobile_land_station_identifier" in self.qualifiers["01"]:
            callsign = self.get_qualifer("01", "ship_or_mobile_land_station_identifier")  # noqa
            wsi_series = 0
            wsi_issuer = 20004
            wsi_number = 0
            wsi_local = callsign.strip()
            if wigosID is None:
                wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"  # noqa

            station_id["tsi"] = {
                "station_id": wsi_local
            }

        # 5 digit buoy number
        # 001003, 001020, 001005
        if all(x in self.qualifiers["01"] for x in ("region_number",
                                                    "wmo_region_sub_area",
                                                    "buoy_or_platform_identifier")):  # noqa
            wmo_region = self.get_qualifer("region_number")
            wmo_subregion = self.get_qualifer("wmo_region_sub_area")
            wmo_number = self.get_qualifer("buoy_or_platform_identifier")
            wsi_series = 0
            wsi_issuer = 20002
            wsi_number = 0
            wsi_local = f"{wmo_region:01d}{wmo_subregion:01d}{wmo_number:05d}"
            if wigosID is None:
                wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"  # noqa

            station_id["tsi"] = {
                "buoy_number": wsi_local
            }

        # station buoy identifier
        # 001010
        if "stationary_buoy_platform_identifier_e_g_c_man_buoys" in self.qualifiers["01"]:  # noqa
            id = self.get_qualifer("01", "stationary_buoy_platform_identifier_e_g_c_man_buoys")  # noqa
            wsi_series = 0
            wsi_issuer = 20002
            wsi_number = 0
            wsi_local = id.strip()
            if wigosID is None:
                wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"  # noqa

            station_id["tsi"] = {
                "station_id": wsi_local
            }

        # 7 digit buoy number
        # 001087
        if "marine_observing_platform_identifier" in self.qualifiers["01"]:
            id = self.get_qualifer("01","marine_observing_platform_identifier")  # noqa
            wsi_series = 0
            wsi_issuer = 20002
            wsi_number = 0
            wsi_local = id

            if wigosID is None:
                wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"  # noqa

            station_id["tsi"] = {
                "buoy_number": wsi_local
            }

        # flag if we do not have WSI
        if wigosID is None:
            LOGGER.debug(self.qualifiers["01"])

        # now set wsi in return value
        station_id["wsi"] = wigosID

        return station_id

    def get_code_value(self, fxxyyy: str, code: int) -> str:
        """
        Gets decoded value for BUFR element

        :param fxxyyy: FXXYYY BUFR descriptor
        :param code: value to decode and convert to string representation

        :returns: string representation of coded value
        """
        table = self.code_table.loc[(self.code_table["FXY"] == fxxyyy), ]
        decoded = table.loc[table["CodeFigure"] == code, ]
        decoded.reset_index(drop=True, inplace=True)
        if len(decoded) == 1:
            decoded = decoded.EntryName_en[0]
        else:
            assert len(decoded) == 0
            decoded = None
        return decoded

    def as_geojson(self, bufr_handle: int, id: str, serialize: bool = False) -> dict:  # noqa
        """
        Function to return geoJSON representation of BUFR message

        :param bufr_handle: integer handle for BUFR data (used by eccodes)
        :param id: id to assign to feature collection

        :returns: dictionary containing feature collection
        """
        # return data as geojson

        # check we have data
        if not bufr_handle:
            LOGGER.warn("Empty BUFR")
            return {}

        LOGGER.info(f"Processing {id}")

        # unpack the message
        codes_set(bufr_handle, "unpack", True)

        # get number of subsets
        nsubsets = codes_get(bufr_handle, "numberOfSubsets")
        LOGGER.debug(f"as_geojson.nsubsets: {nsubsets}")
        try:
            assert nsubsets == 1
        except:
            LOGGER.error(f"Too many subsets in call to as_geojson ({nsubsets})")  # noqa

        # Load headers
        headers = OrderedDict()
        for header in HEADERS:
            try:
                headers[header] = codes_get(bufr_handle, header)
            except Exception as e:
                if header == "subsetNumber":
                    LOGGER.warning("subsetNumber not found, continuing")
                    continue
                LOGGER.error(f"Error reading {header}")
                raise e

        characteristic_date = headers["typicalDate"]
        characteristic_time = headers["typicalTime"]

        try:
            sequence = codes_get_array(bufr_handle, UNEXPANDED_DESCRIPTORS[0])
        except Exception as e:
            LOGGER.error(f"Error reading {UNEXPANDED_DESCRIPTORS}")
            raise e

        sequence = sequence.tolist()
        sequence = [f"{descriptor}" for descriptor in sequence]
        sequence = ",".join(sequence)
        headers["sequence"] = sequence
        LOGGER.info(sequence)

        # now get key iterator
        key_iterator = codes_bufr_keys_iterator_new(bufr_handle)

        # set up data structures
        data = {}
        last_key = None
        index = 0

        # iterate over keys and add to dict
        while codes_bufr_keys_iterator_next(key_iterator):
            # get key
            key = codes_bufr_keys_iterator_get_name(key_iterator)

            LOGGER.debug(key)
            # identify what we are processing
            if key in (HEADERS + ECMWF_HEADERS + UNEXPANDED_DESCRIPTORS):
                continue
            else:  # data descriptor
                try:
                    fxxyyy = codes_get(bufr_handle, f"{key}->code")
                except Exception as e:
                    LOGGER.error(f"Error reading {key}->code")
                    raise e
            LOGGER.debug(key)

            # get class
            xx = int(fxxyyy[1:3])
            # get value and attributes
            value = codes_get_array(bufr_handle, key)  # noqa, get as array and convert to scalar if required
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
            for attribute in ATTRIBUTES:
                attribute_key = f"{key}->{attribute}"
                try:
                    attributes[attribute] = codes_get(bufr_handle, attribute_key)  # noqa
                except Exception as e:
                    LOGGER.error(f"Error reading {attribute_key}")
                    raise e

            units = attributes["units"]
            # next decoded value if from code table
            description = None
            if attributes["units"] == "CODE TABLE":
                description = self.get_code_value(attributes["code"], value)
            elif attributes["units"] == "CCITT IA5":
                description = value
                value = None
            if (units in PREFERRED_UNITS) and (value is not None):
                value = Units.conform(value, Units(units),
                                      Units(PREFERRED_UNITS[units]))
                value = round(value, attributes["scale"])
                units = PREFERRED_UNITS[units]
                attributes["units"] = units
            # now process
            # first process key to something more sensible
            key = re.sub("#[0-9]+#", "", key)
            key = re.sub("([a-z])([A-Z])", r"\1_\2", key)
            key = key.lower()
            LOGGER.debug(key)
            append = False
            if xx < 9:
                if ((xx >= 4) and (xx < 8)) and (key == last_key):
                    append = True
                self.set_qualifier(fxxyyy, key, value, description, attributes, append)  # noqa
            elif xx == 31:
                pass
            else:
                if fxxyyy == "022067":
                    append = False
                    self.set_qualifier(fxxyyy, key, value, description,
                                       attributes, append)  # noqa
                    continue
                if value is not None:
                    self.get_identification()
                    metadata = self.get_qualifiers()
                    metadata_hash = hashlib.md5( json.dumps(metadata).encode("utf-8")).hexdigest()  # noqa
                    md = {
                        "id": metadata_hash,
                        "metadata": list()
                    }
                    for idx in range(len(metadata)):
                        md["metadata"].append(metadata[idx])
                    wsi = self.get_wsi()
                    feature_id = f"WIGOS_{wsi}_{characteristic_date}T{characteristic_time}"  # noqa
                    feature_id = f"{feature_id}{id}-{index}"
                    phenomenon_time = self.get_time()
                    if "/" in phenomenon_time:
                        result_time = phenomenon_time.split("/")
                        result_time = result_time[1]
                    else:
                        result_time = phenomenon_time
                    data[feature_id] = {
                        "geojson": {
                            "id": uuid4().hex,
                            "reportId": f"WIGOS_{wsi}_{characteristic_date}T{characteristic_time}{id}",  # noqa
                            "type": "Feature",
                            "geometry": self.get_location(),
                            "properties": {
                                # "identifier": feature_id,
                                "wigos_station_identifier": wsi,
                                "phenomenonTime": phenomenon_time,
                                "resultTime": result_time,  # noqa
                                "name": key,
                                "value": value,
                                "units": attributes["units"],
                                "description": description,
                                "metadata": metadata,
                                "index": index,
                                "fxxyyy": fxxyyy
                            }
                        },
                        "_meta": {
                            "data_date": self.get_time(),
                            "identifier": feature_id,
                            "metadata_hash": metadata_hash
                        },
                        "_headers": deepcopy(headers)
                        }
                else:
                    pass
            last_key = key
            index += 1
        codes_bufr_keys_iterator_delete(key_iterator)
        LOGGER.info(json.dumps(data, indent=4))
        if serialize:
            data = json.dumps(data, indent=4)
        return data
    
# data[uid]
#     |--- geojson
#     |--- _meta

# data[uid]
#     |--- geojson
#          |---- feature id


def transform(input_file: str, serialize: bool = False) -> Iterator[dict]:
    # check data type, only in situ supported
    # not yet implemented
    # split subsets into individual messages and process
    error = False
    bufr_handle = codes_bufr_new_from_file(input_file)
    try:
        codes_set(bufr_handle, "unpack", True)
    except Exception as e:
        LOGGER.error("Error unpacking message")
        LOGGER.error(e)
        if FAIL_ON_ERROR:
            raise e
        error = True
    if not error:
        nsubsets = codes_get(bufr_handle, "numberOfSubsets")
        LOGGER.info(f"{nsubsets} subsets in file {input_file}")
        id = Path(input_file.name).stem
        collections = dict()
        for idx in range(nsubsets):
            LOGGER.debug(bufr_handle)
            LOGGER.debug(f"Extracting subset {idx}")
            codes_set(bufr_handle, "extractSubset", idx+1)
            codes_set(bufr_handle, "doExtractSubsets", 1)
            LOGGER.debug("Cloning subset to new message")
            single_subset = codes_clone(bufr_handle)
            LOGGER.debug("Unpacking")
            codes_set(single_subset, "unpack", True)

            parser = BUFRParser()
            # only include tag if more than 1 subset in file
            tag = ""
            if nsubsets > 1:
                tag = f"-{idx}"
            try:
                data = parser.as_geojson(single_subset, id=tag, serialize=serialize)  # noqa
            except Exception as e:
                LOGGER.error("Error parsing BUFR to geoJSON, no data written")
                LOGGER.error(e)
                if FAIL_ON_ERROR:
                    raise e
                data = {}
            del parser
            collections = deepcopy(data)

            yield collections
            codes_release(single_subset)
    else:
        collections = {}
        yield collections

    if not error:
        codes_release(bufr_handle)


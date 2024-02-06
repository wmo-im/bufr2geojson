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

__version__ = "0.5.dev2"

from collections import OrderedDict
from copy import deepcopy
import csv
from datetime import datetime, timedelta
import hashlib
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
                     codes_get_array, codes_set,
                     codes_release, codes_get,
                     CODES_MISSING_LONG, CODES_MISSING_DOUBLE,
                     codes_bufr_keys_iterator_new,
                     codes_bufr_keys_iterator_next,
                     codes_bufr_keys_iterator_delete, codes_definition_path,
                     codes_bufr_keys_iterator_get_name)

import numpy as np

LOGGER = logging.getLogger(__name__)

# some 'constants'
SUCCESS = True
NUMBERS = (float, int, complex)
MISSING = ("NA", "NaN", "NAN", "None")
NULLIFY_INVALID = True  # TODO: move to env. variable

BUFR_TABLE_VERSION = 37  # default BUFR table version
THISDIR = os.path.dirname(os.path.realpath(__file__))
RESOURCES = f"{THISDIR}{os.sep}resources"
CODETABLES = {}

ECCODES_DEFINITION_PATH = codes_definition_path()
if not os.path.exists(ECCODES_DEFINITION_PATH):
    LOGGER.debug('ecCodes definition path does not exist, trying environment')
    ECCODES_DEFINITION_PATH = os.environ.get('ECCODES_DEFINITION_PATH')
    LOGGER.debug(f'ECCODES_DEFINITION_PATH: {ECCODES_DEFINITION_PATH}')
    if ECCODES_DEFINITION_PATH is None:
        raise EnvironmentError('Cannot find ecCodes definition path')

TABLEDIR = Path(ECCODES_DEFINITION_PATH) / 'bufr' / 'tables' / '0' / 'wmo'

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

LOCATION_DESCRIPTORS = ["latitude", "latitude_increment",
                        "latitude_displacement", "longitude",
                        "longitude_increment", "longitude_displacement",
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
    def __init__(self, raise_on_error=False):

        self.raise_on_error = raise_on_error

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
        :param key: Plain text key of fxxyyy qualifier based on ecCodes library
        :param value: Numeric value of the qualifier
        :param description: Character value of the qualifier
        :param attributes: BUFR attributes (scale, reference value, width,
                           units etc) associated with element)
        :param append: Flag to indicate whether to append qualifier on to list
                       of values. Only valid for coordinates

        :returns: None
        """

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

    def set_time_displacement(self, key, value, append=False):
        raise NotImplementedError()

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
                try:
                    description = strip2(description)
                except AttributeError:
                    pass
                q = {
                    "name": name,
                    "value": value,
                    "units": units,
                    "description": description
                }
                result.append(q)
        return result

    def get_location(self) -> Union[dict, None]:
        """
        Function to get location from qualifiers and to apply any displacements
        or increments

        :returns: dictionary containing GeoJSON geometry or None
                  (if geometry contains null values/cannot be derived)
                  example: `{"type":"", "coordinates": [x,y,z?]}`
        """

        # first get latitude
        #if not (("005001" in self.qualifiers["05"]) ^ ("005002" in self.qualifiers["05"])):  # noqa
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
            latitude = round(latitude["value"],
                             latitude["attributes"]["scale"])

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

        # now station elevation
        if "height_of_station_ground_above_mean_sea_level" in self.qualifiers["07"]:  # noqa
            elevation = deepcopy(self.qualifiers["07"]["height_of_station_ground_above_mean_sea_level"])  # noqa
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

        location = [longitude, latitude]

        if elevation is not None:
            location.append(elevation)

        if None in location:
            LOGGER.debug('geometry contains null values; setting to None')
            return None
        return {
            "type": "Point",
            "coordinates": location
        }

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
        time_ = datetime.strptime(time_, "%Y-%m-%d %H:%M:%S")
        time_ = time_ + timedelta(days=offset)
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
        _type = "marine_observing_platform_identifier"
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
                "type": "7_digit_marine_observing_platform_identifier"
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

    def as_geojson(self, bufr_handle: int, id: str,
                   serialize: bool = False, guess_wsi: bool = False) -> dict:
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
        LOGGER.debug(sequence)

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
            xx = int(fxxyyy[1:3])
            # get value and attributes
            # get as array and convert to scalar if required
            value = codes_get_array(bufr_handle, key)
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
                    attribute_value = codes_get(bufr_handle, attribute_key)
                except Exception as e:
                    LOGGER.warning(f"Error reading {attribute_key}: {e}")
                    attribute_value = None
                if attribute_value is not None:
                    attributes[attribute] = attribute_value

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
                # round to 6 d.p. to remove any erroneous digits
                # due to IEEE arithmetic
                value = round(value, 6)
                units = PREFERRED_UNITS[units]
                attributes["units"] = units
            # now process
            # first process key to something more sensible
            key = re.sub("#[0-9]+#", "", key)
            key = re.sub("([a-z])([A-Z])", r"\1_\2", key)
            key = key.lower()
            append = False
            if xx < 9:
                if ((xx >= 4) and (xx < 8)) and (key == last_key):
                    append = True
                self.set_qualifier(fxxyyy, key, value, description,
                                   attributes, append)
            elif xx == 31:
                pass
            else:
                if fxxyyy == "022067":
                    append = False
                    self.set_qualifier(fxxyyy, key, value, description,
                                       attributes, append)
                    continue
                if value is not None:
                    # self.get_identification()
                    metadata = self.get_qualifiers()
                    metadata_hash = hashlib.md5(json.dumps(metadata).encode("utf-8")).hexdigest()  # noqa
                    md = {
                        "id": metadata_hash,
                        "metadata": list()
                    }
                    for idx in range(len(metadata)):
                        md["metadata"].append(metadata[idx])
                    wsi = self.get_wsi(guess_wsi)
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
                            "id": feature_id,
                            "conformsTo": ["http://www.wmo.int/spec/om-profile-1/1.0/req/geojson"],  # noqa
                            "reportId": f"WIGOS_{wsi}_{characteristic_date}T{characteristic_time}{id}",  # noqa
                            "type": "Feature",
                            "geometry": self.get_location(),
                            "properties": {
                                # "identifier": feature_id,
                                "wigos_station_identifier": wsi,
                                "phenomenonTime": phenomenon_time,
                                "resultTime": result_time,
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
                            "geometry": self.get_location(),
                            "metadata_hash": metadata_hash
                        },
                        "_headers": deepcopy(headers)
                        }
                else:
                    pass
            last_key = key
            index += 1
        codes_bufr_keys_iterator_delete(key_iterator)
        if serialize:
            data = json.dumps(data, indent=4)
        return data


def transform(data: bytes, serialize: bool = False,
              guess_wsi: bool = False) -> Iterator[dict]:
    """
    Main transformation

    :param data: byte string of BUFR data
    :param serialize: whether to return as JSON string (default is False)
    :param guess_wsi: whether to 'guess' WSI based on TSI and allocaiotn rules

    :returns: `generator` of GeoJSON features
    """

    error = False

    # FIXME: figure out how to pass a bytestring to ecCodes BUFR reader
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
                collections = dict()
                for idx in range(nsubsets):
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
                        data = parser.as_geojson(single_subset, id=tag,
                                                 serialize=serialize,
                                                 guess_wsi=guess_wsi)  # noqa

                    except Exception as e:
                        LOGGER.error("Error parsing BUFR to GeoJSON, no data written")  # noqa
                        LOGGER.error(e)
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
        space = ' '
    elif isinstance(value, bytes):
        space = b' '
    else:  # make sure we have a string
        space = ' '
        value = f"{value}"

    if value.startswith(space) or value.endswith(space):
        LOGGER.warning(f"value '{value}' is space padded; upstream data should be fixed")  # noqa

    return value.strip()

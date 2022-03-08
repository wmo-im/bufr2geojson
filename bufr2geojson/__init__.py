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
import hashlib
from io import StringIO, BytesIO
import json
import logging
import os.path
import re
from typing import Any, Iterator, Union
from uuid import uuid4
from pathlib import Path

from cfunits import Units
from eccodes import (codes_bufr_new_from_file, codes_clone,
                     codes_get_array, codes_set, codes_get_native_type,
                     codes_write, codes_release, codes_get,
                     CODES_MISSING_LONG, CODES_MISSING_DOUBLE,
                     codes_bufr_keys_iterator_new,
                     codes_bufr_keys_iterator_next,
                     codes_bufr_keys_iterator_get_name, CodesInternalError)
from jsonpath_ng.ext import parser
from jsonschema import validate
import numpy as np
import pandas as pd

# pandas config
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

# some 'constants'
SUCCESS = True
NUMBERS = (float, int, complex)
MISSING = ("NA", "NaN", "NAN", "None")

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
           "unexpandedDescriptors", "subsetNumber"]

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

json_template = {
        "id": None,
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": None
        },
        "properties": {
            "identifier": None,
            "wigos_station_identifier": None,
            "phenomenonTime": None,
            "resultTime": None,
            "observations": [
                {}
            ]
        },
        "_bufr_headers": [],
        "_meta": []
    }




class BUFRParser:
    def __init__(self):
        # load BUFR tables
        self.code_table = pd.read_csv(f"{TABLES}{os.sep}BUFRCREX_CodeFlag_en.txt",\
                                      dtype="object")  # noqa
        # strip out non numeric rows, these typically give a range
        numeric_rows = self.code_table["CodeFigure"].apply(
            lambda x: (not np.isnan(x)) if isinstance(x, (int, float)) else x.isnumeric() )
        self.code_table = self.code_table.loc[numeric_rows, ]
        # now convert to integers for matching
        self.code_table = self.code_table.astype({"CodeFigure":"int"})
        # dict to store qualifiers
        self.qualifiers = {
            "00": {},
            "01": {},
            "02": {},
            "03": {},
            "04": {},
            "05": {},
            "06": {},
            "07": {},
            "08": {}
        }

    def set_qualifier(self, fxxyyy, key, value, description, attributes, append=False):
        # get class of descriptor
        xx = fxxyyy[1:3]
        # first check whether the value is None, if so remove and exit
        if key == "ship_or_mobile_land_station_identifier":
            LOGGER.debug(f"CALLSIGN: {xx} {key}")
        if value is None:
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

    def set_time_displcaement(self, key, value, append=False):
        if append:
            pass
        else:
            pass

    def get_qualifer(self, xx, key, default=None):
        if key in self.qualifiers[xx]:
            value = self.qualifiers[xx][key]["value"]
        else:
            LOGGER.debug(f"No value found for requested qualifier ({key}), setting to default ({default})")  # noqa
            value = default
        return value

    def get_qualifiers(self):
        classes = ("01", "02", "03", "04", "05", "06", "07", "08")
        result = list()
        # name, value, units
        for c in classes:
            for k in self.qualifiers[c]:
                if k in LOCATION_DESCRIPTORS:
                    continue
                if k in TIME_DESCRIPTORS:
                    continue
                if k in ID_DESCRIPTORS:
                    continue
                name = k # self.qualifiers[c][k]["key"]
                code = self.qualifiers[c][k]["code"]
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

    def get_location(self):  # special rules for the definition of the location
        # first get latitude
        #if not (("005001" in self.qualifiers["05"]) ^ ("005002" in self.qualifiers["05"])):  # noqa
        if "latitude" not in self.qualifiers["05"]:
            LOGGER.error("Invalid location in BUFR message, no latitude")
            LOGGER.error(self.qualifiers["05"])
            raise
        latitude = deepcopy(self.qualifiers["05"]["latitude"])

        # check if we need to add a displacement
        if "latitude_displacement" in self.qualifiers["05"]:  # noqa
            y_displacement = deepcopy(self.qualifiers["05"]["latitude_displacement"])  # noqa
            latitude["value"] += y_displacement["value"]
        latitude = round(latitude["value"], latitude["attributes"]["scale"])

        # now get longitude
        if "longitude" not in self.qualifiers["06"]:
            LOGGER.error("Invalid location in BUFR message, no longitude")
            LOGGER.error(self.qualifiers["06"])
            raise
        longitude = self.qualifiers["06"]["longitude"]

        # check if we need to add a displacement
        if "longitude_displacement" in self.qualifiers["06"]:
            x_displacement = deepcopy(self.qualifiers["06"]["longitude_displacement"])  # noqa
            longitude["value"] += x_displacement["value"]

        # round to avoid extraneous digits
        longitude = round(longitude["value"], longitude["attributes"]["scale"])  # noqa

        # now station elevation
        if "height_of_station_ground_above_mean_sea_level" in self.qualifiers["07"]:  # noqa
            elevation = self.qualifiers["07"]["height_of_station_ground_above_mean_sea_level"]  # noqa
            elevation = round(elevation["value"], elevation["attributes"]["scale"])  #noqa
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

    def get_time(self):
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
                LOGGER.debug( f"DISPLACEMENT: {value}")
                LOGGER.debug(len(value))
                if len(value) > 2:
                    LOGGER.error("More than two time displacements")
                    raise NotImplementedError
            else:
                # many sequences only specify a single displacement when they should indicate two
                # for example, average wind speed over proceeding 10 minutes
                # if only negative single displacement assume time period up to
                # current time
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

    def get_identification(self):
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
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
            station_id["tsi"] = {
                "block": block,
                "station": station
            }
        # ship or mobile land station identifier (001011)
        if "ship_or_mobile_land_station_identifier" in self.qualifiers["01"]:
            LOGGER.debug("CALLSIGN")
            callsign = self.get_qualifer("01", "ship_or_mobile_land_station_identifier")  # noqa
            wsi_series = 0
            wsi_issuer = 20004
            wsi_number = 0
            wsi_local = callsign.strip()
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
            station_id["tsi"] = {
                "station_id": wsi_local
            }

        if "ship_or_mobile_land_station_identifier" not in self.qualifiers["01"]:
            LOGGER.debug("NO CALLSIGN")
            LOGGER.debug( json.dumps(self.qualifiers, indent=4))

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
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
            station_id["tsi"] = {
                "buoy_number": wsi_local
            }
        # station buoy identifier
        # 001010
        if "stationary_buoy_platform_identifier_e_g_c_man_buoys" in self.qualifiers["01"]:
            id = self.get_qualifer("01", "stationary_buoy_platform_identifier_e_g_c_man_buoys")  # noqa
            wsi_series = 0
            wsi_issuer = 20002
            wsi_number = 0
            wsi_local = id.strip()
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
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
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
            station_id["tsi"] = {
                "buoy_number": wsi_local
            }
        station_id["wsi"] = wigosID

        if wigosID is None:
            LOGGER.debug( self.qualifiers["01"])

        return station_id

    def get_code_value(self, fxxyyy, code):
        table = self.code_table.loc[ (self.code_table["FXY"] == fxxyyy), ]
        decoded = table.loc[ table["CodeFigure"] == code, ]
        decoded.reset_index(drop=True, inplace=True)
        if len(decoded) == 1:
            decoded = decoded.EntryName_en[0]
        else:
            assert len(decoded) == 0
        return decoded

    def as_geojson(self, bufr_handle, id, filter=None):
        # because of the way ecCodes works we don't have direct access to the
        # BUFR data, instead we need to go through the keys

        # get handle for BUFR message
        #bufr_handle = codes_bufr_new_from_file(fileObj)
        # check we have data
        if not bufr_handle:
            LOGGER.warn("Empty BUFR")
            return {}

        # unpack the message
        codes_set(bufr_handle, "unpack", True)

        # get number of subsets
        nsubsets = codes_get(bufr_handle, "numberOfSubsets")
        LOGGER.debug(f"as_geojson.nsubsets: {nsubsets}")
        assert nsubsets == 1

        # now get key iterator
        key_iterator = codes_bufr_keys_iterator_new(bufr_handle)

        data = list()
        headers = OrderedDict()
        metadata_list = list()
        md5sums = list()
        last_key = None
        last_fxxyyy = None
        index = 0
        # iterate over keys and add to dict
        while codes_bufr_keys_iterator_next(key_iterator):
            # get key
            key = codes_bufr_keys_iterator_get_name(key_iterator)
            # identify what we are processing
            if key in HEADERS:
                if key == "unexpandedDescriptors":
                    # headers[key] = codes_get_array(bufr_handle, key)
                    LOGGER.debug( codes_get_array(bufr_handle, key) )
                    pass
                else:
                    headers[key] = codes_get(bufr_handle, key)
                continue
            if key in ECMWF_HEADERS:
                continue
            else:  # data descriptor
                fxxyyy = codes_get(bufr_handle, f"{key}->code")

            LOGGER.debug(key)
            # get class
            f = int(fxxyyy[0])
            xx = int(fxxyyy[1:3])
            yyy = int(fxxyyy[3:6])

            # get value and attributes
            value = codes_get_array(bufr_handle, key)  # get as array and convert to scalar if required
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
                attributes[attribute] = codes_get(bufr_handle, attribute_key)

            units = attributes["units"]
            # next decoded value if from code table
            description = None
            if attributes["units"] == "CODE TABLE":
                description = self.get_code_value(attributes["code"], value)
            elif attributes["units"] == "CCITT IA5":
                description = value

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
                #if ((xx >= 4) and (xx < 8)) and (fxxyyy == last_fxxyyy):
                    append = True
                self.set_qualifier(fxxyyy, key, value, description, attributes, append)  # noqa
            elif xx == 31:
                pass
            else:
                if value is not None:
                    self.get_identification()
                    if filter:
                        if key not in filter:
                            continue
                    metadata = self.get_qualifiers()
                    metadata_hash = hashlib.md5( json.dumps(metadata).encode("utf-8")).hexdigest()  # noqa
                    md = {
                        "id": metadata_hash,
                        "metadata": list()
                    }
                    for idx in range(len(metadata)):
                        md["metadata"].append(metadata[idx])

                    data.append({
                        "id": uuid4().hex,
                        "type": "Feature",
                        "geometry": self.get_location(),
                        "properties": {
                            "key": key,
                            "value": value,
                            "units": attributes["units"],
                            "description": description,
                            "phenomenonTime": self.get_time(),
                            "resultTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S+0"),  #noqa
                            "metadata": metadata_hash,
                            "index": index
                        }
                    })
                    if metadata_hash not in md5sums:
                        metadata_list.append(md)
                        md5sums.append(metadata_hash)

                else:
                    #LOGGER.debug(f"No data found for {key}, data omitted")
                    pass
            last_key = key
            last_fxxyyy = fxxyyy
            index += 1
        collection = {
            "id": id,
            "type": "FeatureCollection",
            "station_id": self.get_identification(),
            "features": deepcopy(data),
            "metadata": deepcopy(metadata_list),
            "headers": headers
        }
        metadata_df = pd.json_normalize(metadata_list, ["metadata"], ["id"])
        # reorder columns
        metadata_df = metadata_df[["id", "name", "value", "units", "description"]]
        records_df = pd.json_normalize(collection,
                                       record_path = ["features"],
                                       meta = [["id"], ["station_id", "wsi"]],  # noqa
                                       meta_prefix="record.", record_prefix="feature.", sep=".")  #noqa


        records_df = records_df[["feature.id","record.id", "record.station_id.wsi",  # noqa
                                "feature.type", "feature.geometry.type",  # noqa
                                "feature.geometry.coordinates", "feature.properties.phenomenonTime",  # noqa
                                 "feature.properties.key",  # noqa
                                "feature.properties.value", "feature.properties.units",  # noqa
                                "feature.properties.description",
                                "feature.properties.resultTime", "feature.properties.metadata",  # noqa
                                "feature.properties.index"]]

        result = {
            "geojson": collection,
            "records.csv": records_df,
            "metadata.csv": metadata_df
        }
        return result


def transform(input_file):

    # check data type, only in situ supported
    # not yet implemented
    # split subsets into individual messages and process
    bufr_handle = codes_bufr_new_from_file(input_file)
    codes_set(bufr_handle, "unpack", True)
    nsubsets = codes_get(bufr_handle, "numberOfSubsets")
    id = Path(input_file.name).stem
    collections = dict()
    for idx in range(nsubsets):
        LOGGER.debug(f"Extracting subset {idx}")
        codes_set(bufr_handle, "extractSubset",idx+1)
        codes_set(bufr_handle, "doExtractSubsets",1)
        single_subset = codes_clone(bufr_handle)
        codes_set(single_subset, "unpack", True)
        parser = BUFRParser()
        data = parser.as_geojson(single_subset, id = f"{id}-{idx}")
        collections[f"{idx}"] = deepcopy(data)
        codes_release(single_subset)
    codes_release(bufr_handle)
    return collections

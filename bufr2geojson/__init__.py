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

LOCATION_DESCRIPTORS = ["007030", "005001", "005002", "005003", "005015",
                        "005016", "006001", "006002", "006003", "006015",
                        "006016", "005011", "005012", "006011", "006012"]

TIME_DESCRIPTORS = ["004001", "004002", "004003", "004004", "004005",
                    "004006", "004011", "004012", "004013", "004014",
                    "004015", "004016", "004023", "004024", "004025",
                    "004026", "004021", "004022"]

ID_DESCRIPTORS = ["001001", "001002", "001011", "001003", "001020",
                  "001005", "001010", "001087"]

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

    def set_qualifiers(self, fxxyyy, key, value, attributes, append=False):
        # first check whether the value is None, if so remove and exit
        xx = fxxyyy[1:3]
        key = re.sub("#[0-9]+#", "", key)
        key = re.sub("([a-z])([A-Z])", r"\1_\2", key)
        key = key.lower()
        if value is None:
            if fxxyyy in self.qualifiers[xx]:
                del self.qualifiers[xx][fxxyyy]
        else:
            if fxxyyy in self.qualifiers[xx] and append:
                self.qualifiers[xx][fxxyyy]["value"] = \
                    [self.qualifiers[xx][fxxyyy]["value"], value]  # noqa
            else:
                self.qualifiers[xx][fxxyyy] = {
                    "key": key,
                    "value": value,
                    "attributes": attributes
                }

    def get_qualifer(self, fxxyyy, default=None):
        xx = fxxyyy[1:3]
        if fxxyyy in self.qualifiers[xx]:
            value = self.qualifiers[xx][fxxyyy]["value"]
        else:
            # LOGGER.debug(f"No value found for requested qualifier ({fxxyyy}), setting to default ({default})")  # noqa
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
                name = self.qualifiers[c][k]["key"]
                value = self.qualifiers[c][k]["value"]
                units = self.qualifiers[c][k]["attributes"]["units"]
                if units == "CODE TABLE":
                    description = self.get_code_value(k, value)
                elif units == "CCITT IA5":
                    description = value.strip()
                    value = None
                else:
                    description = None
                q = {
                    "name": name,
                    "value": value,
                    "units": units,
                    "description": description
                }
                #if q["units"] == "CODE TABLE":
                #    q["description"] = self.get_code_value(k, q["value"])
                result.append(q)
        return result

    def get_location(self):  # special rules for the definition of the location
        # first get latitude
        if not (("005001" in self.qualifiers["05"]) ^ ("005002" in self.qualifiers["05"])):  # noqa
            LOGGER.error("Invalid location in BUFR message, no latitude")
            raise
        # LOGGER.debug(self.qualifiers["05"])
        latitude = deepcopy(self.qualifiers["05"]["005001"]) if "005001" in \
            self.qualifiers["05"] else deepcopy(self.qualifiers["05"]["005002"])  # noqa

        # check if we need to add a displacement
        if ("005015" in self.qualifiers["05"]) ^ ("005016" in self.qualifiers["05"]):  # noqa
            y_displacement = deepcopy(self.qualifiers["05"]["005015"]) if "005015" \
                            in self.qualifiers["05"] else deepcopy(self.qualifiers["05"]["005016"])  # noqa
            latitude["value"] += y_displacement["value"]

        latitude = round(latitude["value"], latitude["attributes"]["scale"])

        # now get longitude
        if not (("006001" in self.qualifiers["06"]) ^ ("006002" in self.qualifiers["06"])):  # noqa
            LOGGER.error("Invalid location in BUFR message, no longitude")
            raise
        longitude = self.qualifiers["06"]["006001"] if "006001" in \
            self.qualifiers["06"] else self.qualifiers["06"]["006002"]
        # check if we need to add a displacement
        if ("006015" in self.qualifiers["06"]) ^ ("006016" in self.qualifiers["06"]):  # noqa
            x_displacement = deepcopy(self.qualifiers["06"]["006015"]) if "006015" \
                            in self.qualifiers["06"] else deepcopy(self.qualifiers["06"]["006016"])  # noqa
            longitude["value"] += x_displacement["value"]

        # round to avoid extraneous digits
        longitude = round(longitude["value"], longitude["attributes"]["scale"])  # noqa

        # now station elevation
        if "007030" in self.qualifiers["07"]:
            elevation = self.qualifiers["07"]["007030"]
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
            location= [longitude, latitude, elevation]
        else:
            location = [longitude, latitude]

        geom = {
            "type": "Point",
            "coordinates": location
        }

        return geom

    def get_time(self):
        # get year
        year = self.get_qualifer("004001")
        month = self.get_qualifer("004002")
        day = self.get_qualifer("004003", 1)
        hour = self.get_qualifer("004004", 0)
        minute = self.get_qualifer("004005", 0)
        second = self.get_qualifer("004006", 0)
        if hour == 24:
            hour = 0
            offset = 1
            LOGGER.debug("Hour == 24 found in get time, increment day by 1")
        else:
            offset = 0
        time = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"  # noqa
        time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        time = time + timedelta(days=offset)
        # check if we have any increment descriptors, not yet supported for date  # noqa
        yyy = ("004011", "004012", "004013", "004014", "004015", "004016")
        for qualifier in yyy:
            if qualifier in self.qualifiers["04"]:
                LOGGER.error(qualifier)
                raise NotImplementedError

        # check if we have any displacement descriptors, years and months
        # done separately
        #yyy = ("004021", "004022", "004023", "004024", "004025", "004026")
        kwargs = {
            "days": "004023",
            "hours": "004024",
            "minutes": "004025",
            "seconds": "004026"
        }
        for k in kwargs:
            kwargs[k] = self.get_qualifer(kwargs[k], 0)
        time = time + timedelta(**kwargs)

        if "004021" in self.qualifiers["04"]:
            time.year += self.get_qualifer("004021")

        if "004022" in self.qualifiers["04"]:
            time.month += self.get_qualifer("004022")

       # finally convert datetime to string
        time = time.strftime("%Y-%m-%dT%H:%M:%S+0")

        return time

    def get_identification(self):
        # see https://library.wmo.int/doc_num.php?explnum_id=11021
        # page 19 for allocation of WSI if not set
        # check to see what identification we have
        # WIGOS id
        # 001125, 001126, 001127, 001128
        station_id = dict()
        if all(x in self.qualifiers["01"] for x in ("001125", "001126", "001127", "001128")):  # noqa
            wsi_series = self.get_qualifer("001125")
            wsi_issuer = self.get_qualifer("001126")
            wsi_number = self.get_qualifer("001127")
            wsi_local = self.get_qualifer("001128")
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
        else:
            wigosID = None
        # block number and station number
        # 001001, 001002
        if all(x in self.qualifiers["01"] for x in ("001001", "001002")):
            block = self.get_qualifer("001001")
            station = self.get_qualifer("001002")
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
        if "001011" in self.qualifiers["01"]:
            callsign = self.get_qualifer("001011")
            wsi_series = 0
            wsi_issuer = 20004
            wsi_number = 0
            wsi_local = callsign.strip()
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
            station_id["tsi"] = {
                "station_id": wsi_local
            }
        # 5 digit buoy number
        # 001003, 001020, 001005
        if all(x in self.qualifiers["01"] for x in ("001003", "001020", "001005")):  # noqa
            wmo_region = self.get_qualifer("001003")
            wmo_subregion = self.get_qualifer("001020")
            wmo_number = self.get_qualifer("001005")
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
        if "001010" in self.qualifiers["01"]:
            id = self.get_qualifer("001010")
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
        if "001087" in self.qualifiers["01"]:
            id = self.get_qualifer("001087")
            wsi_series = 0
            wsi_issuer = 20002
            wsi_number = 0
            wsi_local = id
            wigosID = f"{wsi_series}-{wsi_issuer}-{wsi_number}-{wsi_local}"
            station_id["tsi"] = {
                "buoy_number": wsi_local
            }
        station_id["wsi"] = wigosID

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

    def as_geojson(self, bufr_handle, filter=None):
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
        # iterate over keys and add to dict
        while codes_bufr_keys_iterator_next(key_iterator):
            # get key
            key = codes_bufr_keys_iterator_get_name(key_iterator)
            # identify what we are processing
            if key in HEADERS:
                if key == "unexpandedDescriptors":
                    # headers[key] = codes_get_array(bufr_handle, key)
                    pass
                else:
                    headers[key] = codes_get(bufr_handle, key)
                continue
            if key in ECMWF_HEADERS:
                continue
            else:  # data descriptor
                fxxyyy = codes_get(bufr_handle, f"{key}->code")

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

            attributes = {}
            for attribute in ATTRIBUTES:
                attribute_key = f"{key}->{attribute}"
                attributes[attribute] = codes_get(bufr_handle, attribute_key)

            # now process
            # first process key to something more sensible
            key = re.sub("#[0-9]+#", "", key)
            key = re.sub("([a-z])([A-Z])", r"\1_\2", key)
            key = key.lower()
            append = False
            if xx < 9:
                #if ((xx >= 4) and (xx < 8)) and (key == last_key):
                if ((xx >= 4) and (xx < 8)) and (fxxyyy == last_fxxyyy):
                    append = True
                self.set_qualifiers(fxxyyy, key, value, attributes, append)
            elif xx == 31:
                pass
            else:
                if value is not None:
                    self.get_identification()
                    # strip extraneous #number# from eccodes key
                    # and change to snake case
                    #key = re.sub("#[0-9]+#","",key)
                    #key = re.sub("([a-z])([A-Z])",r"\1_\2",key)
                    #key = key.lower()
                    if filter:
                        if key not in filter:
                            continue
                    # get decoded value if coded
                    description = None
                    if attributes["units"] == "CODE TABLE":
                        description = self.get_code_value(fxxyyy, value)
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
                            "metadata": metadata_hash
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
        collection = {
            "type": "FeatureCollection",
            "station_id": self.get_identification(),
            "features": deepcopy(data),
            "metadata": deepcopy(metadata_list),
            "headers": headers
        }
        LOGGER.debug(pd.json_normalize(metadata_list, ["metadata"], ["id"]))  # noqa
        return collection


def transform(input_file, as_collection=False):

    # check data type, only in situ supported
    # not yet implemented
    # split subsets into individual messages and process
    bufr_handle = codes_bufr_new_from_file(input_file)
    codes_set(bufr_handle, "unpack", True)
    nsubsets = codes_get(bufr_handle, "numberOfSubsets")
    single_subset = None
    collections = dict()
    for idx in range(nsubsets):
        LOGGER.debug(f"Extracting subset {idx}")
        codes_set(bufr_handle, "extractSubset",idx+1)
        codes_set(bufr_handle, "doExtractSubsets",1)
        single_subset = codes_clone(bufr_handle)
        codes_set(single_subset, "unpack", True)
        parser = BUFRParser()
        data = parser.as_geojson(single_subset)
        collections[f"{idx}"] = deepcopy(data)
        codes_release(single_subset)
    codes_release(bufr_handle)
    return collections
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

from __future__ import annotations
import base64
import itertools
import json

from jsonschema import validate, FormatChecker
import pytest
import yaml

from bufr2geojson import RESOURCES, strip2, transform

WSI_FORMATCHECKER = FormatChecker()


@WSI_FORMATCHECKER.checks("wsi", ValueError)
def is_wsi(instance):
    assert isinstance(instance, str)
    words = instance.split("-")
    assert words[0] == "0"
    assert int(words[1]) <= 65534
    assert int(words[2]) <= 65534
    local_id = words[3]
    assert len(local_id) <= 16
    assert local_id.isalnum()
    return True


@pytest.fixture
def multimsg_bufr():
    bufr_b64 = \
        "QlVGUgAA5wQAABYAABUAAAAAAAEADgAH" \
        "5gMUDwAAAAAJAAABgMdQAAC8AHivpTS1" \
        "MrYQILG0N7qwuhAQEBAQEBAvzGo8BgvH" \
        "Qjc9SA/wCAJ//z8z2t//////+AZDi1t7" \
        "bIAMgu4AZH////8sdQyTLlAQJkBkCMYQ" \
        "QAP/yP+T/////////////////////H/V" \
        "Kf//+/R/8AyP////////AMj/////////" \
        "////A+jBP7B4C77+3///////////v0f/" \
        "7///////////////////9+j/////////" \
        "/////////////+A3Nzc3QlVGUgAA5wQA" \
        "ABYAABUAAAAAAAEADgAH5gMUCQAAAAAJ" \
        "AAABgMdQAAC8AHixqbW0tbIwkBAQEBAQ" \
        "EBAQEBAQEBAvzGokBgzdYjpfoA+0B99/" \
        "/z8kCF//////+AZDg9t5jRAMgfQAZH//" \
        "//8sdgyTqFAQgkhkBYgQQAP/yP+T////" \
        "/////////////////H/VKf//+/R/8AyP" \
        "////////AMj/////////////A+jBP7G4" \
        "Cn7+3///////////v0f/7///////////" \
        "////////9+j/////////////////////" \
        "/+A3Nzc3"
    msg = base64.b64decode(bufr_b64.encode("ascii"))
    return msg


@pytest.fixture
def geojson_schema():
    #with open(f"{RESOURCES}/schemas/wmo-om-profile-geojson.yaml") as fh:
    with open(f"{RESOURCES}/schemas/wccdm-obs.json") as fh:
        return json.load(fh)
        #return yaml.load(fh, Loader=yaml.SafeLoader)


@pytest.fixture
def geojson_output():
    return {
        'id': 'WIGOS_0-20000-0-03951_20220320T210000-0-13',
        'conformsTo': ['http://www.wmo.int/spec/om-profile-1/1.0/req/geojson'],
        'reportId': 'WIGOS_0-20000-0-03951_20220320T210000-0',
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [
                -9.42,
                51.47,
                20.0
            ]
        },
        'properties': {
            'wigos_station_identifier': '0-20000-0-03951',
            'phenomenonTime': '2022-03-20T21:00:00Z',
            'resultTime': '2022-03-20T21:00:00Z',
            'name': 'non_coordinate_pressure',
            'value': 1019.3,
            'units': 'hPa',
            'description': None,
            'metadata': [
                {
                    'name': 'station_or_site_name',
                    'value': None,
                    'units': 'CCITT IA5',
                    'description': 'SHERKIN ISLAND'
                },
                {
                    'name': 'station_type',
                    'value': 0,
                    'units': 'CODE TABLE',
                    'description': 'AUTOMATIC STATION'
                },
                {
                    'name': 'height_of_barometer_above_mean_sea_level',
                    'value': 21.0,
                    'units': 'm',
                    'description': None
                }
            ],
            'index': 13,
            'fxxyyy': '010004'
        }
    }

@pytest.fixture
def geojson_output_new():
    return {
        "id": "1ec58338aab209c8ab22f05309315b71-0",
        "conformsTo": [
            "https://schemas.wmo.int/wccdm-obs/2024/wccdm-obs.json"
        ],
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [
                -9.42,
                51.47,
                21.0
            ]
        },
        "properties": {
            "host": "0-20000-0-03951",
            "observer": None,
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "observedProperty": "non_coordinate_pressure",
            "observingProcedure": "http://codes.wmo.int/wmdr/SourceOfObservation/unknown",
            "phenomenonTime": "2022-03-20T21:00:00Z",
            "resultTime": "2024-12-19 00:00:00",
            "validTime": None,
            "result": {
                "value": 1019.3,
                "units": "hPa",
                "standardUncertainty": None
            },
            "resultQuality": [
                {
                    "inScheme": None,
                    "flag": None,
                    "flagValue": None
                }
            ],
            "parameter": {
                "hasProvenance": {
                    "prefix": {
                        "prov": "http://www.w3.org/ns/prov#",
                        "schema": "https://schema.org/"
                    },
                    "entity": {
                        "0-20000-0-03951": {
                            "prov:type": "schema:DigitalDocument",
                            "prov:label": "Input data file",
                            "schema:encodingFormat": "application/bufr"
                        },
                        "1ec58338aab209c8ab22f05309315b71-0": {
                            "prov:type": "observation",
                            "prov:label": "Observation 0 from subset 0 of message 1"
                        }
                    },
                    "wasDerivedFrom": {
                        "_:wdf": {
                            "prov:generatedEntity": "1ec58338aab209c8ab22f05309315b71-0",
                            "prov:usedEntity": "0-20000-0-03951",
                            "prov:activity": "_:bufr2geojson"
                        }
                    },
                    "activity": {
                        "_:bufr2geojson": {
                            "prov:type": "prov:Activity",
                            "prov:label": "Data transformation using version 0.7.dev0 of bufr2geojson",
                            "prov:endTime": "2024-12-19 00:00:00"
                        }
                    }
                },
                "status": None,
                "version": 0,
                "comment": None,
                "reportType": "000001",
                "reportIdentifier": "1ec58338aab209c8ab22f05309315b71",
                "isMemberOf": None,
                "additionalProperties": {
                    "identification": {
                        "station_or_site_name": {
                            "value": "SHERKIN ISLAND"
                        }
                    },
                    "instrumentation": {
                        "station_type": {
                            "value": {
                                "codetable": "http://codes.wmo.int/bufr4/codeflag/0-02-001",
                                "entry": "0",
                                "description": "AUTOMATIC STATION"
                            }
                        },
                        "height_of_station_ground_above_mean_sea_level": {
                            "value": 20.0,
                            "units": "m",
                            "description": None
                        },
                        "height_of_barometer_above_mean_sea_level": {
                            "value": 21.0,
                            "units": "m",
                            "description": None
                        }
                    },
                    "qualifiers": {},
                    "processing": {},
                    "monitoring": {},
                    "quality": {},
                    "associated_field": {},
                    "BUFR_element": "010004",
                    "zCoordinate": {
                        "name": "height_above_local_ground",
                        "value": 1.0,
                        "units": "m"
                    },
                    "BUFRheaders": {
                        "edition": 4,
                        "masterTableNumber": 0,
                        "bufrHeaderCentre": 233,
                        "bufrHeaderSubCentre": 0,
                        "updateSequenceNumber": 0,
                        "dataCategory": 0,
                        "internationalDataSubCategory": 1,
                        "dataSubCategory": 0,
                        "masterTablesVersionNumber": 14,
                        "localTablesVersionNumber": 0,
                        "typicalYear": 2022,
                        "typicalMonth": 3,
                        "typicalDay": 20,
                        "typicalHour": 21,
                        "typicalMinute": 0,
                        "typicalSecond": 0,
                        "typicalDate": "20220320",
                        "typicalTime": "210000",
                        "numberOfSubsets": 1,
                        "observedData": 1,
                        "compressedData": 0,
                        "subsetNumber": 1,
                        "sequence": "307080"
                    }
                }
            },
            "featureOfInterest": [
                {
                    "id": None,
                    "label": None,
                    "relation": None
                }
            ],
            "index": 0
        }
    }


def test_multi(multimsg_bufr):
    results = transform(multimsg_bufr, guess_wsi=True)
    # count number of geojsons
    icount = 0
    for res in results:
        for key, val in res.items():
            if key != "geojson" :
                continue
            icount += 1
    assert icount == 48


def test_transform(geojson_schema, geojson_output_new):
    test_bufr_file = 'A_ISIA21EIDB202100_C_EDZW_20220320210902_11839953.bin'
    with open(test_bufr_file, 'rb') as fh:
        messages1, messages2 = itertools.tee(transform(fh.read(),
                                                       guess_wsi=True))

        # validate against JSON Schema
        for message in messages1:
            geojson_dict = message['geojson']
            assert isinstance(geojson_dict, dict)
            _ = validate(geojson_dict, geojson_schema,
                         format_checker=WSI_FORMATCHECKER)


        print("==========================================")
        print("Messages validated against schema")
        print("==========================================")


        # validate content
        message = next(messages2)
        geojson = message['geojson']  # noqa
        geojson['properties']['parameter']['hasProvenance']['activity']['_:bufr2geojson']['prov:endTime'] = "2024-12-19 00:00:00"
        geojson['properties']['resultTime'] = "2024-12-19 00:00:00"
        for k, v in geojson.items():
            assert v == geojson_output_new[k]
        assert geojson == geojson_output_new

        print("Message matches expected value")


def test_strip2():

    for value in ['test', ' test', 'test ', ' test ', '  test    ']:
        assert strip2(value) == 'test'

    for value in [b'test', b' test', b'test ', b' test ', b'  test    ']:
        assert strip2(value) == b'test'

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
import itertools

from jsonschema import validate, FormatChecker
import pytest
import yaml

from bufr2geojson import RESOURCES, transform

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
def geojson_schema():
    with open(f"{RESOURCES}/schemas/wmo-om-profile-geojson.yaml") as fh:
        return yaml.load(fh, Loader=yaml.SafeLoader)


@pytest.fixture
def geojson_output():
    return {
        'id': 'WIGOS_0-20000-0-03951_20220320T210000-0-13',
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
                    'description': 'SHERKIN ISLAND      '
                },
                {
                    'name': 'station_type',
                    'value': 0,
                    'units': 'CODE TABLE',
                    'description': 'AUTOMATIC'
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



def test_transform(geojson_schema, geojson_output):
    test_bufr_file = 'A_ISIA21EIDB202100_C_EDZW_20220320210902_11839953.bin'
    with open(test_bufr_file, 'rb') as fh:
        messages1, messages2 = itertools.tee(transform(fh.read()))

        # validate against JSON Schema
        for message in messages1:
            geojson_dict = list(message.values())[0]['geojson']
            assert isinstance(geojson_dict, dict)
            print("Validating GeoJSON")
            _ = validate(geojson_dict, geojson_schema, format_checker=WSI_FORMATCHECKER)
        print("Messages validated against schema")
        # validate content
        message = next(messages2)
        assert 'WIGOS_0-20000-0-03951_20220320T210000-0-13' in message
        print("Message found in result")
        geojson = message['WIGOS_0-20000-0-03951_20220320T210000-0-13']['geojson']  # noqa
        assert geojson == geojson_output
        print("Message matches expected value")
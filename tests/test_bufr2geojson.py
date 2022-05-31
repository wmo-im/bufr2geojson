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

from jsonschema import validate, FormatChecker
import pytest
import yaml

from bufr2geojson import RESOURCES, transform

WSI_FORMATCHECKER = FormatChecker()


@pytest.fixture
def geojson_schema():
    with open(f"{RESOURCES}/schemas/wmo-om-profile-geojson.yaml") as fh:
        return yaml.load(fh, Loader=yaml.SafeLoader)


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


def test_transform(geojson_schema):
    test_bufr_file = 'A_ISIA21EIDB202100_C_EDZW_20220320210902_11839953.bin'
    with open(test_bufr_file, 'rb') as fh:
        geojson_messages = transform(fh.read())

        for geojson_message in geojson_messages:
            geojson_dict = list(geojson_message.values())[0]['geojson']
            assert isinstance(geojson_dict, dict)
            print("Validating GeoJSON")
            _ = validate(geojson_dict, geojson_schema,
                         format_checker=WSI_FORMATCHECKER)

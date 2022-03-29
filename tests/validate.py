from __future__ import annotations
from jsonschema import (validate, validators, TypeChecker, Draft7Validator,
                        FormatChecker)
import json


BASEDIR = "/app/"

checker = FormatChecker()


@checker.checks("wsi", ValueError)
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


with open(f"{BASEDIR}/bufr2geojson/resources/schemas/observation.json") as fh:
    the_schema = json.load(fh)

with open(f"{BASEDIR}/output/test.json") as fh:
    the_data = json.load(fh)

the_result = validate(the_data, the_schema, format_checker=checker)



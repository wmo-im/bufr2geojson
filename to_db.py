import argparse
from copy import deepcopy
from csv import QUOTE_NONNUMERIC
import hashlib
import json
import pandas as pd
import os
import sys
import gc as gc

from bufr2geojson import __version__, BUFRParser, transform as as_geojson

BUFFER_SIZE = 10000

OBSERVATIONS_TABLE = {
    "uuid": "",
    "reportId" : "",
    "resultTime": "",
    "wsi": "",
    "phenomenonTime": "",
    "startTime": "",
    "location": "",
    "zcoordinate": float(),
    "observedPhenomenon": "",
    "uom": "",
    "resultValue": float(),
    "description": "",
    "metadata": ""
}

METADATA_TABLE = {
    # "id": int(),
    "hash": "",
    "wsi": "",
    "name": "",
    "value": float(),
    "uom": "",
    "description": ""
}

REPORT_TABLE = {
    "reportId": "",
    "bufrEdition": int(),
    "masterTable": int(),
    "masterTableVersion": int(),
    "originatingCentre": int(),
    "originatingSubCentre": int(),
    "dataCategory": int(),
    "dataSubCategory": int(),
    "unexpandedDescriptors": ""
}


# convert the following to class and add various methods
def empty_table(table_definition, size):
    result = pd.DataFrame(table_definition, index = range(size))
    return result

#gc.set_debug(gc.DEBUG_LEAK)

def main(argv):
    # set up tables to import to DB
    observations_table = empty_table(OBSERVATIONS_TABLE, BUFFER_SIZE)
    report_table = empty_table(REPORT_TABLE, BUFFER_SIZE)
    metadata_table = empty_table(METADATA_TABLE, BUFFER_SIZE)

    # get list of files to process


    obsIdx = 0
    mdIdx = 0
    repIdx = 0
    print("START: reading file list from file_list.txt")
    with open("file_list.txt") as fh:
        files = fh.readlines()
    nfiles = len(files)

    data_dir = "/local/gts_sample/"

    # iterate over files
    for fileIdx in range(0,nfiles):
        #print(gc.get_stats())
        file = files[fileIdx]
        file = file[:-1] # remove new line character
        print(f"Processing file {fileIdx} / {nfiles} ({file})")
        fh = open(f"{data_dir}{file}")
        result = as_geojson(fh)
        for collection in result:
            for key, item in collection.items():
                # extract information for observations table
                # extract information for metadata table
                # extract information for report table
                data = item["geojson"]
                mdhash = hashlib.md5(json.dumps(data["properties"]["metadata"]).encode("utf-8")).hexdigest()  # noqa
                # extract time and locaiton elements
                phenomenon_time = data["properties"]["phenomenonTime"]
                start_time = ""
                if "/" in phenomenon_time:
                    phenomenon_time = phenomenon_time.split("/")
                    start_time = phenomenon_time[0]
                    phenomenon_time = phenomenon_time[1]
                geometry = data["geometry"]
                if geometry["type"] != "Point":
                    print(geometry)
                    raise NotImplementedError
                lon = geometry["coordinates"][0]
                lat = geometry["coordinates"][1]
                if len(geometry["coordinates"]) == 3:
                    zcoord = geometry["coordinates"][2]
                else:
                    zcoord = None
                location = f"POINT({lon} {lat})"
                # update elements in data frame, element by element.
                observations_table.loc[obsIdx,"uuid"] = (data["id"])
                observations_table.loc[obsIdx, "reportId"] = (data["reportId"])
                observations_table.loc[obsIdx, "resultTime"] = (data["properties"]["resultTime"])
                observations_table.loc[obsIdx, "wsi"] = (data["properties"]["wigos_station_identifier"])
                observations_table.loc[obsIdx, "phenomenonTime"] = phenomenon_time
                observations_table.loc[obsIdx, "startTime"] = start_time
                observations_table.loc[obsIdx, "location"] = location
                observations_table.loc[obsIdx, "zcoordinate"] = zcoord
                observations_table.loc[obsIdx, "observedPhenomenon"] = (data["properties"]["name"])
                observations_table.loc[obsIdx, "uom"] = (data["properties"]["units"])
                observations_table.loc[obsIdx, "resultValue"] = (data["properties"]["value"])
                observations_table.loc[obsIdx, "description"] = (data["properties"]["description"])
                observations_table.loc[obsIdx, "metadata"] = mdhash
                obsIdx += 1
                # check if we need to write table
                if obsIdx == BUFFER_SIZE:
                    # drop any duplicates we have
                    observations_table.drop_duplicates(inplace=True, ignore_index=True)  # noqa
                    write_headers = not os.path.exists("observations.csv")
                    observations_table.to_csv("observations.csv", mode="a",
                                              header=write_headers,
                                              index=False,
                                              quoting = QUOTE_NONNUMERIC)
                    observations_table = empty_table(OBSERVATIONS_TABLE, BUFFER_SIZE)
                    obsIdx = 0
                        
                for md in data["properties"]["metadata"]:
                    metadata_table.loc[mdIdx, "hash"] = mdhash
                    metadata_table.loc[mdIdx, "wsi"] = (data["properties"]["wigos_station_identifier"])
                    metadata_table.loc[mdIdx, "name"] = md["name"]
                    metadata_table.loc[mdIdx, "value"] = md["value"]
                    metadata_table.loc[mdIdx, "uom"] = md["units"]
                    metadata_table.loc[mdIdx, "description"] = md["description"]
                    mdIdx += 1
                    # check if we need to write table
                    if mdIdx == BUFFER_SIZE:
                        metadata_table.drop_duplicates(inplace=True, ignore_index=True)  # noqa
                        write_headers = not os.path.exists("metadata.csv")
                        metadata_table.to_csv("metadata.csv", mode="a", header=write_headers, index=False,
                                  quoting = QUOTE_NONNUMERIC)
                        metadata_table = empty_table(METADATA_TABLE, BUFFER_SIZE)  # noqa
                        mdIdx = 0

                report_table.loc[repIdx,"reportId"] = (data["reportId"])
                report_table.loc[repIdx,"bufrEdition"] = item["_headers"]["edition"]
                report_table.loc[repIdx,"masterTable"] = item["_headers"]["masterTableNumber"]
                report_table.loc[repIdx,"masterTableVersion"] = item["_headers"]["masterTablesVersionNumber"]
                report_table.loc[repIdx,"originatingCentre"] = item["_headers"]["bufrHeaderCentre"]
                report_table.loc[repIdx,"originatingSubCentre"] = item["_headers"]["bufrHeaderSubCentre"]
                report_table.loc[repIdx,"dataCategory"] = item["_headers"]["dataCategory"]
                report_table.loc[repIdx,"dataSubCategory"] = item["_headers"]["internationalDataSubCategory"]
                report_table.loc[repIdx,"unexpandedDescriptors"] = item["_headers"]["sequence"]
                repIdx += 1
                # check if we need to write table
                if repIdx == BUFFER_SIZE:
                    report_table.drop_duplicates(inplace=True, ignore_index=True)  # noqa
                    write_headers = not os.path.exists("reports.csv")
                    report_table.to_csv("reports.csv", mode="a", header=write_headers, index=False,
                                  quoting = QUOTE_NONNUMERIC)  # noqa
                    report_table = empty_table(REPORT_TABLE, BUFFER_SIZE)
                    repIdx = 0
        fh.close()

    if obsIdx != 0:
        observations_table = observations_table.iloc[0:obsIdx, ]
        observations_table.drop_duplicates(inplace=True, ignore_index=True)
        write_headers = not os.path.exists("observations.csv")
        observations_table.to_csv("observations.csv", mode="a",
                                  header=write_headers, index=False,
                                  quoting = QUOTE_NONNUMERIC)

    if mdIdx != 0:
        metadata_table = metadata_table.iloc[0:mdIdx, ]
        metadata_table.drop_duplicates(inplace=True, ignore_index=True)
        write_headers = not os.path.exists("metadata.csv")
        metadata_table.to_csv("metadata.csv", mode="a", header=write_headers,
                              index=False,
                                  quoting = QUOTE_NONNUMERIC)

    if repIdx != 0:
        report_table = report_table.iloc[0:repIdx, ]
        report_table.drop_duplicates(inplace=True, ignore_index=True)
        write_headers = not os.path.exists("reports.csv")
        report_table.to_csv("reports.csv", mode="a", header=write_headers,
                              index=False,
                                  quoting = QUOTE_NONNUMERIC)

if __name__ == '__main__':
    main(sys.argv[1:])
    
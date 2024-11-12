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

import json
import logging
import os.path
import sys

import click

from bufr2geojson import __version__, transform as as_geojson


def cli_option_verbosity(f):
    logging_options = ["ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

    def callback(ctx, param, value):
        if value is not None:
            logging.basicConfig(stream=sys.stdout,
                                level=getattr(logging, value))
        return True

    return click.option("--verbosity", "-v",
                        type=click.Choice(logging_options),
                        help="Verbosity",
                        callback=callback)(f)


def cli_callbacks(f):
    f = cli_option_verbosity(f)
    return f


@click.group()
@click.version_option(version=__version__)
def cli():
    """bufr2geojson"""
    pass


@click.group()
def data():
    """data utilities"""
    pass


@click.command()
@click.pass_context
@click.argument("bufr_file", type=click.File(mode="rb", errors="ignore"))
@click.option("--output-dir", "output_dir", required=True,
              help="Name of output file")
@cli_option_verbosity
def transform(ctx, bufr_file, output_dir, verbosity):
    click.echo(f"Transforming {bufr_file.name} to geojson")
    result = as_geojson(bufr_file.read(), source_identifier=bufr_file.name)
    for collection in result:
        for key, item in collection.items():
            if key == "geojson":
                identifier = item['id']
                outfile = f"{output_dir}{os.sep}{identifier}.json"
                with open(outfile, "w") as fh:
                    fh.write(json.dumps(item, indent=4))

    click.echo("Done")


data.add_command(transform)
cli.add_command(data)

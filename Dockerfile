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

FROM wmoim/dim_eccodes_baseimage:jammy-2.36.0
ENV DEBIAN_FRONTEND="noninteractive" \
    TZ="Etc/UTC" \
    ECCODES_DIR=/opt/eccodes \
    ECCODES_DEFINITION_PATH=/opt/eccodes/share/eccodes/definitions \
    PATH="${PATH}:/opt/eccodes/bin"

# Install additional packges
RUN echo apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y libudunits2-0 curl

WORKDIR /tmp
COPY . /tmp
RUN cd /tmp && python3 setup.py install
RUN cd /tmp && rm -r ./*

WORKDIR /local


# bufr2geojson

Python module and CLI to convert from bufr to geoJSON.
Please note, the geoJSON produced by this module is experimental and subject to change.

## Install
Download and build Docker image
````
git clone https://github.com/wmo-im/bufr2geojson.git
cd bufr2geojson
docker build -t bufr2geojson .
````

Now run:
````
docker run -it -v ${pwd}:/app bufr2geojson
cd /app
````

## Usage

````
bufr2geojson transform <input_file> --output-dir <output_directory> --csv <True|False>
````

e.g.

````
bufr2geojson transform ./example_data/A_ISIA21EIDB202100_C_EDZW_20220320210902_11839953.bin  --output-dir ./output/
````

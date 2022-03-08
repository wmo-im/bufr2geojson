# bufr2geojson

Python module and CLI to convert from bufr to geoJSON.

Usage:

````
bufr2geojson transform <input_file> --output-dir <output_directory> --csv <True|False>
````

e.g.

````
bufr2geojson transform A_ISSL01EGRR170600_C_EDZW_20220217063602_77195428.bin --output-dir ./output/
````

or with CSV:

bufr2geojson transform A_ISSL01EGRR170600_C_EDZW_20220217063602_77195428.bin --output-dir ./output/ --csv True
# bufr2geojson

The bufr2geojson Python module contains both a command line interface and an API to convert data stored as WMO BUFR to GeoJSON (RFC 7946).
More information on the BUFR format can be found in the [WMO Manual on Codes, Volume I.2](https://library.wmo.int/doc_num.php?explnum_id=10722).

## Installation

### Requirements
- Python 3 and above
- [ecCodes](https://confluence.ecmwf.int/display/ECC)

### Dependencies

Dependencies are listed in [requirements.txt](https://github.com/wmo-im/bufr2geojson/blob/main/requirements.txt). Dependencies are automatically installed during bufr2geojson installation.

```bash
# download and build Docker image
git clone https://github.com/wmo-im/bufr2geojson.git
cd bufr2geojson
docker build -t bufr2geojson .

# login to Docker container
docker run -it -v ${pwd}:/app bufr2geojson
cd /app
```

## Running

```bash
bufr2geojson data transform <input_file> --output-dir <output_directory> --csv <True|False>
```

e.g.

```bash
bufr2geojson data transform ./tests/A_ISIA21EIDB202100_C_EDZW_20220320210902_11839953.bin  --output-dir ./output/
```

## Releasing

```bash
# create release (x.y.z is the release version)
vi bufr2geojson/__init__.py  # update __version__
git commit -am 'update release version vx.y.z'
git push origin main
git tag -a vx.y.z -m 'tagging release version vx.y.z'
git push --tags

# upload to PyPI
rm -fr build dist *.egg-info
python setup.py sdist bdist_wheel --universal
twine upload dist/*

# publish release on GitHub (https://github.com/wmo-im/bufr2geojson/releases/new)

# bump version back to dev
vi bufr2geojson/__init__.py  # update __version__
git commit -am 'back to dev'
git push origin main
```

## Documentation

The full documentation for bufr2geojson can be found at [https://bufr2geojson.readthedocs.io](https://bufr2geojson.readthedocs.io), including sample files.

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/wmo-im/bufr2geojson/issues).

## Contact

* [David Berry](https://github.com/david-i-berry)

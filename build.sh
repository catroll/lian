#!/usr/bin/env bash

# python3 setup.py build
# python3 setup.py build_py

# python3 setup.py egg_info
# python3 setup.py dist_info

python3 setup.py sdist
# python3 setup.py bdist
# python3 setup.py bdist_egg
python3 setup.py bdist_wheel

# python setup.py sdist
# python setup.py bdist_wheel

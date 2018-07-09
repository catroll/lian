#!/usr/bin/env bash

BASEDIR=$(dirname "$0")

echo '# lian' > $BASEDIR/README.md
wget https://www.gitignore.io/api/vim,python -O $BASEDIR/.gitignore
wget https://raw.githubusercontent.com/tornadoweb/tornado/master/LICENSE -O $BASEDIR/LICENSE

echo 'setuptools' > $BASEDIR/requirements.txt
echo 'pymysql' >> $BASEDIR/requirements.txt
echo 'tornado' >> $BASEDIR/requirements.txt
echo 'redis' >> $BASEDIR/requirements.txt

touch setup.py

pandoc README.md --to rst -o README.rst

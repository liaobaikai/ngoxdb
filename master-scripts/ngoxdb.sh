#!/bin/bash

BASE_DIR=`dirname "$0"`/..
ngoxdb_version=
ngoxdb_lib=$BASE_DIR/lib/
ngoxdb_jar=
ngoxdb_config=$BASE_DIR/config/ngoxdb.yml

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
        cat $BASE_DIR/README.txt
fi

java \
-Dloader.path=$ngoxdb_lib \
-Xms512m \
-Xmx1024m \
-jar $ngoxdb_lib/$ngoxdb_jar \
--spring.config.location=$ngoxdb_config \
--spring.profiles.active=prod \
"$@"


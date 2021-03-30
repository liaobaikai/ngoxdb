#!/bin/bash

ngoxdb_version=
ngoxdb_libs=./libs/
ngoxdb_jar=

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
        cat man/usage.txt
fi

#arguments=""
#for arg in "$@";
#do
#        if [[ $arg =~ ^--slave* ]]; then
#                arg=$(echo $arg | sed 's/^--slave-/--slave./')
#        else
#                arg=$(echo $arg | sed 's/^--/--master./')
#        fi
#        arguments=" $arguments $arg "
#done

java -Dloader.path=$ngoxdb_libs -Xms512m -Xmx1024m -jar $ngoxdb_libs/$ngoxdb_jar $@


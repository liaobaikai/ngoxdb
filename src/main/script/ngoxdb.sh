#!/bin/bash

ngoxdb_executable=../../../target/ngoxdb-0.0.1-SNAPSHOT.jar

for arg in "$*"
do
	      value=`echo "$arg" | sed -e 's/^[^=]*=//'`
        case "$arg" in
                --user=*) source_username=$value ;;
                --host=*) source_host=$value ;;
                --port=*) source_port=$value ;;
                --password=*) source_password=$value ;;

                --slave-user=*) source_username=$value ;;
                --slave-host=*) source_host=$value ;;
                --slave-port=*) source_port=$value ;;
                --slave-password=*) source_password=$value ;;
        esac
done

 java -jar ngoxdb-0.0.1-SNAPSHOT.jar \
 --ngoxdb.master.database=oracle \
 --ngoxdb.master.database-name=orcl \
 --ngoxdb.master.host=baikai.top \
 --ngoxdb.master.username=baikai \
 --ngoxdb.master.password.baikai \
 --ngoxdb.master.port=15210
# --ngoxdb.slave.0.database=oracle
java -jar $ngoxdb_executable $*



#!/bin/bash

NGOXDB_JAR=./libs/ngoxdb*.jar

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then

        echo "Usage: ngoxdb [master-parameters] [slave-parameters]"
        echo ""
        echo "example: "
        cat > __ngoxdb_example.out <<- EOF
ngoxdb --master.database=mariadb \
--master.host=127.0.0.1 \
--master.database-name=sakila \
--master.username=root \
--master.password=123456 \
--slave.database=msaccess \
--slave.local-file=/tmp/sakila.mdb \

ngoxdb --master.database=msaccess \
--master.local-file=/tmp/sakila.mdb \
--master.tables=actor \
--slave.database=mariadb \
--slave.host=127.0.0.1 \
--slave.database-name=sakila2 \
--slave.username=root \
--slave.password=123456 \
--slave.replace-table=true \
--slave.generate-name=true \
--slave.remap-table=actor:actor2

EOF
        cat __ngoxdb_example.out
        rm -rf __ngoxdb_example.out

fi

# spring.datasource.master  <=> {}
# --spring.datasource.master.database=oracle
# --spring.datasource.master.database-name=orcl
# --spring.datasource.master.database=oracle
# --spring.datasource.master.database=oracle
# --spring.datasource.master.database=oracle
#
# spring.datasource.slave   <=> slave {}

# java -jar ngoxdb-0.0.1-SNAPSHOT.jar \
# --ngoxdb.master.database=oracle \
# --ngoxdb.master.database-name=orcl \
# --ngoxdb.master.host=baikai.top \
# --ngoxdb.master.username=baikai \
# --ngoxdb.master.password.baikai \
# --ngoxdb.master.port=15210
# --ngoxdb.slave.0.database=oracle
java -jar $NGOXDB_JAR $*

# usage
# --master-local-file


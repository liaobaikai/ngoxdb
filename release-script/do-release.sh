#!/bin/bash

bashdir=$(cd `dirname $0`; pwd)

# 复制
rm -rf $bashdir/../release/libs/* 2&>/dev/null
cp -r $bashdir/../target/ngoxdb-*.jar $bashdir/../release/libs
rm -rf $bashdir/../release/logs

# 生成执行文件
cp -r $bashdir/../release-script/ngoxdb.sh $bashdir/../release/ngoxdb
chmod +x $bashdir/../release/ngoxdb

# 打包
#tar -zcvf $basedir/../out/ngoxdb.tar.gz $bashdir/../release/*
#gzip -r $bashdir/../release/* $basedir/../dist/ngoxdb.tar.gz
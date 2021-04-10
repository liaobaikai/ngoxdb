#!/bin/bash

file_basedir=$(cd `dirname $0`; pwd)
basedir=$1
version=$2
ngoxdb_release=$basedir/release
ngoxdb_release_jar=$3
ngoxdb_release_lib=$ngoxdb_release/lib
ngoxdb_release_bin=$ngoxdb_release/bin
ngoxdb_release_config=$ngoxdb_release/config
ngoxdb_release_logs=$ngoxdb_release/logs

mkdir -p $ngoxdb_release
mkdir -p $ngoxdb_release_lib
mkdir -p $ngoxdb_release_bin
mkdir -p $ngoxdb_release_config
mkdir -p $ngoxdb_release_logs

# 复制
rm -rf $ngoxdb_release_lib/*
cp -r $basedir/target/libs/* $ngoxdb_release_lib
cp -r $basedir/target/$ngoxdb_release_jar $ngoxdb_release_lib
rm -rf $basedir/release/logs/*

# config
cp -r $basedir/src/main/resources/application.yml $ngoxdb_release_config/ngoxdb.yml
# license
cp -r $basedir/LICENSE $ngoxdb_release
# notice
cp -r $basedir/NOTICE.txt $ngoxdb_release
# usage
cp -r $basedir/USAGE.txt $ngoxdb_release/README.txt

# 生成执行文件
executable=$ngoxdb_release_bin/ngoxdb

cp -r $file_basedir/ngoxdb.sh $executable
sed -i "" "s/ngoxdb_version=/ngoxdb_version=${version}/" $executable
sed -i "" "s/ngoxdb_jar=/ngoxdb_jar=$ngoxdb_release_jar/" $executable
chmod +x $executable

# 打包
cd $basedir
rm -rf $basedir/dist/ngoxdb-$version 2&>/dev/null
mkdir $basedir/dist/ngoxdb-$version
cp -r $basedir/release/* $basedir/dist/ngoxdb-$version
cd $basedir/dist
tar cfz ngoxdb-$version.tar.gz ngoxdb-$version
rm -rf $basedir/dist/ngoxdb-$version
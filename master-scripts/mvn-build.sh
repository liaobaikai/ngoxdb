#!/bin/bash

file_bashdir=$(cd `dirname $0`; pwd)
bashdir=$1
version=$2
ngoxdb_libs=$3
ngoxdb_jar=$4

# 复制
rm -rf $ngoxdb_libs/*
cp -r $bashdir/target/libs/* $ngoxdb_libs
cp -r $bashdir/target/$ngoxdb_jar $ngoxdb_libs
rm -rf $bashdir/release/logs

# 生成执行文件
executable=$bashdir/release/ngoxdb

cp -r $file_bashdir/ngoxdb.sh $executable
sed -i "" "s/ngoxdb_version=/ngoxdb_version=${version}/" $executable
sed -i "" "s/ngoxdb_jar=/ngoxdb_jar=$ngoxdb_jar/" $executable
chmod +x $executable

# 打包
cd $bashdir
rm -rf $bashdir/dist/ngoxdb-$version 2&>/dev/null
mkdir $bashdir/dist/ngoxdb-$version
cp -r $bashdir/release/* $bashdir/dist/ngoxdb-$version
cd $bashdir/dist
tar cfz ngoxdb-$version.tar.gz ngoxdb-$version
rm -rf $bashdir/dist/ngoxdb-$version
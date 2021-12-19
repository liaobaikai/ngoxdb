![image](https://user-images.githubusercontent.com/24989504/113009591-ab56ea80-91aa-11eb-8c40-d9e9fd603b58.png)


1) ngoxdb命令支持多个数据库之间转换(支持Access、Oracle、SqlServer、PostgreSQL、DM、MariaDB、MySQL、SQLITE等)<br/>

2) 支持转换类型：普通表(Table)<br/>
   暂不支持类型：分区表(Partition Table)，视图(View)，存储过程(Procedure)，函数(Function)，自定义类型(Type)。<br/>

3) 如ngoxdb命令执行失败，可直接使用java命令执行，命令如下：<br/>
   cd <unzip_dir> <br/>
   java -Dloader.path=./lib -Xms512m -Xmx1024m -jar ./lib/ngoxdb_<ngoxdb_version>.jar [master-parameters] [slave-parameters]



Usage: ./ngoxdb [master-parameters] [slave-parameters]

example:

###[MySQL -> Access]
./ngoxdb --master.database=mariadb \
--master.host=127.0.0.1 \
--master.database-name=sakila \
--master.username=root \
--master.password=123456 \
--slave.database=msaccess \
--slave.local-file=/tmp/sakila.mdb

###[Access -> MySQL]
./ngoxdb --master.database=msaccess \
--master.local-file=/tmp/sakila.mdb \
--master.tables=actor \
--slave.database=mysql \
--slave.host=127.0.0.1 \
--slave.database-name=sakila2 \
--slave.username=root \
--slave.password=123456 \
--slave.replace-table=true \
--slave.generate-name=true \
--slave.remap-table=actor:actor2

###[Access -> MariaDB]
./ngoxdb --master.database=msaccess \
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

###[Access -> Oracle]
./ngoxdb --master.database=msaccess \
--master.local-file=/tmp/sakila.mdb \
--slave.database=oracle \
--slave.host=127.0.0.1 \
--slave.port=11521 \
--slave.database-name=baika \
--slave.username=baikai \
--slave.password=baikai \
--slave.replace-table=true \
--slave.generate-name=true \
--slave.remap-table=actor:actor2

###[Access -> DM(达梦数据库)]
./ngoxdb --master.database=msaccess \
--master.local-file=/tmp/sakila.mdb \
--slave.database=dm \
--slave.host=127.0.0.1 \
--slave.port=15236 \
--slave.database-name=DMSERVER \
--slave.username=baikai \
--slave.password=baikai \
--slave.replace-table=true \
--slave.generate-name=true \
--slave.remap-table=actor:actor2

###[Access -> PostgreSQL]
./ngoxdb --master.database=msaccess \
--master.local-file=/tmp/sakila.mdb \
--slave.database=postgresql \
--slave.host=127.0.0.1 \
--slave.port=15432 \
--slave.database-name=sakila \
--slave.username=baikai \
--slave.password=baikai \
--slave.replace-table=true \
--slave.generate-name=true \
--slave.remap-table=actor:actor2

###[Access -> SQLServer]
./ngoxdb --master.database=msaccess \
--master.local-file=/tmp/sakila.mdb \
--slave.database=sqlserver \
--slave.host=127.0.0.1 \
--slave.port=14330 \
--slave.database-name=sakila \
--slave.username=sa \
--slave.password=123456 \
--slave.replace-table=true \
--slave.generate-name=true \
--slave.remap-table=actor:actor2

###[Access -> Access]
./ngoxdb --master.database=msaccess \
--master.local-file=/tmp/sakila.mdb \
--slave.database=msaccess \
--slave.local-file=/tmp/sakila2.mdb \
--slave.remap-table=actor:actor2


主库参数:
--master.name                    配置标识
                                 默认值=slave|master
--master.database                数据库厂家
                                 支持的数据库: sqlserver,mysql,mariadb,postgresql,oracle,db2,dm,msaccess,sqlite
--master.host                    主机名
--master.port                    端口
--master.database-name           数据库名
--master.driver-class-name       驱动类
--master.username                用户名
--master.password                密码
--master.params                  其他参数信息
--master.local-file              本地文件, applyOn=(msaccess,sqlite)
--master.new-database-version    生成的数据库版本, applyOn=(msaccess)
                                 默认值=V2007
--master.jdbc-url                jdbc url
                                 默认值=根据其他配置参数自动生成
--master.protocol                协议: tcp, tcps...等等, applyOn=(oracle)
--master.servers                 多个主机名，如: host1[,host2,host3][:port1][,host4:port2], applyOn=(oracle)
--master.page-size               批量导出导入的数量
                                 默认值=256
--master.tables                  需要处理的表名，用逗号隔开
--master.parallel-workers        并行工作的线程数，默认为可用的核心线程数的2倍，主库(收集表信息、导出数据)；从库(导入数据)
--master.thread-pool-size        线程池大小，默认为并行工作的线程数的2倍

从库参数:
--slave.name                     配置标识
                                 别名: --slave.0.name
                                 默认值=slave|master
--slave.database                 数据库厂家
                                 别名: --slave.0.database
                                 支持的数据库: sqlserver,mysql,mariadb,postgresql,oracle,db2,dm,msaccess,sqlite
--slave.host                     主机名
                                 别名: --slave.0.host
--slave.port                     端口
                                 别名: --slave.0.port
--slave.database-name            数据库名
                                 别名: --slave.0.database-name
--slave.driver-class-name        驱动类
                                 别名: --slave.0.driver-class-name
--slave.username                 用户名
                                 别名: --slave.0.username
--slave.password                 密码
                                 别名: --slave.0.password
--slave.params                   其他参数信息
                                 别名: --slave.0.params
--slave.local-file               本地文件
                                 别名: --slave.0.local-file, applyOn=(msaccess,sqlite)
--slave.new-database-version     生成的数据库版本
                                 别名: --slave.0.new-database-version, applyOn=(msaccess)
                                 默认值=V2007
--slave.jdbc-url                 jdbc url
                                 别名: --slave.0.jdbc-url
                                 默认值=根据其他配置参数自动生成
--slave.replace-table            表存在时是否替换(false: 跳过, true: 替换)?
                                 别名: --slave.0.replace-table
                                 默认值=false
--slave.truncate-table           表数据存在时是否截断(false: 跳过, true: 替换)?
                                 别名: --slave.0.truncate-table
                                 默认值=false
--slave.create-table-params      创建表的参数
                                 别名: --slave.0.create-table-params
--slave.protocol                 协议: tcp, tcps...等等
                                 别名: --slave.0.protocol, applyOn=(oracle)
--slave.servers                  多个主机名，如: host1[,host2,host3][:port1][,host4:port2]
                                 别名: --slave.0.servers, applyOn=(oracle)
--slave.generate-name            自动生成名称，主要针对索引(false: 默认, true: 自动生成)。
                                 别名: --slave.0.generate-name
                                 默认值=false
--slave.remap-table              重新映射表名，如：a:a1,b:b2
                                 别名: --slave.0.remap-table
--slave.remap-column             重新映射表的列名，如：a1.c1:a1.a1,b2.c1:b2.c2，如果存在表名映射，指定的表名应该是映射后的表名
                                 别名: --slave.0.remap-column
--slave.parallel-workers         并行工作的线程数，默认为可用的核心线程数的2倍，主库(收集表信息、导出数据)；从库(导入数据)
                                 别名: --slave.0.parallel-workers
--slave.thread-pool-size         线程池大小，默认为并行工作的线程数的2倍
                                 别名: --slave.0.thread-pool-size

以下写法的参数可支持多个同步从库，数字从0开始
--slave.0.name                   配置标识
                                 默认值=slave|master
--slave.0.database               数据库厂家
                                 支持的数据库: sqlserver,mysql,mariadb,postgresql,oracle,db2,dm,msaccess,sqlite
--slave.0.host                   主机名
--slave.0.port                   端口
--slave.0.database-name          数据库名
--slave.0.driver-class-name      驱动类
--slave.0.username               用户名
--slave.0.password               密码
--slave.0.params                 其他参数信息
--slave.0.local-file             本地文件, applyOn=(msaccess,sqlite)
--slave.0.new-database-version   生成的数据库版本, applyOn=(msaccess)
                                 默认值=V2007
--slave.0.jdbc-url               jdbc url
                                 默认值=根据其他配置参数自动生成
--slave.0.replace-table          表存在时是否替换(false: 跳过, true: 替换)?
                                 默认值=false
--slave.0.truncate-table         表数据存在时是否截断(false: 跳过, true: 替换)?
                                 默认值=false
--slave.0.create-table-params    创建表的参数
--slave.0.protocol               协议: tcp, tcps...等等, applyOn=(oracle)
--slave.0.servers                多个主机名，如: host1[,host2,host3][:port1][,host4:port2], applyOn=(oracle)
--slave.0.generate-name          自动生成名称，主要针对索引(false: 默认, true: 自动生成)。
                                 默认值=false
--slave.0.remap-table            重新映射表名，如：a:a1,b:b2
--slave.0.remap-column           重新映射表的列名，如：a1.c1:a1.a1,b2.c1:b2.c2，如果存在表名映射，指定的表名应该是映射后的表名
--slave.0.parallel-workers       并行工作的线程数，默认为可用的核心线程数的2倍，主库(收集表信息、导出数据)；从库(导入数据)
--slave.0.thread-pool-size       线程池大小，默认为并行工作的线程数的2倍

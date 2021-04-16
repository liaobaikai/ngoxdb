![image](https://user-images.githubusercontent.com/24989504/113009591-ab56ea80-91aa-11eb-8c40-d9e9fd603b58.png)


1) ngoxdb命令支持多个数据库之间转换(支持Access、Oracle、SqlServer、PostgreSQL、DM、MariaDB、MySQL、SQLITE等)<br/>

2) 支持转换类型：普通表(Table)<br/>
   暂不支持类型：分区表(Partition Table)，视图(View)，存储过程(Procedure)，函数(Function)，自定义类型(Type)。<br/>

3) 如ngoxdb命令执行失败，可直接使用java命令执行，命令如下：<br/>
   cd <unzip_dir> <br/>
   java -Dloader.path=./lib -Xms512m -Xmx1024m -jar ./lib/ngoxdb_<ngoxdb_version>.jar [master-parameters] [slave-parameters]

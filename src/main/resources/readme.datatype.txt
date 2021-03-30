###支持的数据库jdbctype和对应数据库的类型映射
@author: liao baikai<baikai.liao@qq.com>
@date: 2021-03-09

>>> ORACLE:
   JDBC TYPE                        ORACLE TYPE
-------------------------------------------------------------
   BOOLEAN                          NUMBER(1)
   BIT                              NUMBER(1)
   TINYINT                          NUMBER(3)
   SMALLINT                         NUMBER(5)
   INTEGER                          NUMBER(10)
   BIGINT                           NUMBER(19)
   FLOAT                            FLOAT(49)
   DOUBLE                           FLOAT(126)
   REAL                             FLOAT(23)
   NUMERIC                          NUMERIC(1 ~ 38, -84 ~ 127)
   DECIMAL                          NUMERIC(1 ~ 38, -84 ~ 127)
   CHAR
        1) > 4000                   CLOB
        2) 2000 ~ 3999              VARCHAR2
        3) < 2000                   CHAR
   VARCHAR
        1) > 4000                   CLOB
        2) < 4000                   VARCHAR2
   CLOB                             CLOB
   LONGVARCHAR                      CLOB
   DATE                             DATE
   TIME                             DATE
   TIME_WITH_TIMEZONE               DATE
   SMALLDATETIME                    DATE
   DATETIMEOFFSET                   DATE
   TIMESTAMP                        TIMESTAMP
   BINARY
   VARBINARY
        1) > 2000                   LONG RAW
        2) <= 2000                  RAW
   BLOB                             BLOB
   LONGVARBINARY                    BLOB
   NCHAR
        1) > 2000                   NCLOB
        2) 1000 ~ 1999              NVARCHAR2
        3) < 1000                   NCHAR
   NVARCHAR
        1) > 2000                   NCLOB
        1) <= 2000                  NVARCHAR2
   LONGNVARCHAR                     NCLOB
   NCLOB                            NCLOB
   SQLXML                           SQLXML
   TIMESTAMP_WITH_TIMEZONE          TIMESTAMP WITH TIME ZONE
   GUID                             CHAR(36)
   MONEY                            NUMBER(19, 4)
   SMALLMONEY                       NUMBER(10, 4)

>>> POSTGRESQL:
    JDBC TYPE                       PGSQL TYPE                  ALIAS
--------------------------------------------------------------------------------
    BIT                             bool
    BOOLEAN                         bool
    TINYINT                         smallint                    int2
    SMALLINT                        smallint                    int2
    INTEGER                         integer                     int4
    BIGINT                          bigint                      int8
    FLOAT                           float
    REAL                            real                        float4
    DOUBLE                          float8
    NUMERIC                         numeric(1~1000,0~1000)
    DECIMAL                         numeric(1~1000,0~1000)
    NCHAR
        1) > 10485760               text
        2) <= 10485760              char
    CHAR
        1) > 10485760               text
        2) <= 10485760              char
    VARCHAR
        1) > 10485760               text
        2) <= 10485760              varchar
    NVARCHAR
        1) > 10485760               text
        2) <= 10485760              varchar
    LONGNVARCHAR                    text
    LONGVARCHAR                     text
    CLOB                            text
    NCLOB                           text
    DATE                            date
    TIME                            time
    TIMESTAMP                       timestamp
    DATETIME                        timestamp
    SMALLDATETIME                   timestamp
    DATETIMEOFFSET                  timestamp
    BINARY                          bytea
    VARBINARY                       bytea
    LONGVARBINARY                   bytea
    BLOB                            bytea
    SQLXML                          xml
    REF_CURSOR                      refcursor
    TIME_WITH_TIMEZONE              timetz
    TIMESTAMP_WITH_TIMEZONE         timestamptz
    GEOMETRY                        geometry
    GEOGRAPHY                       geography
    GEOGRAPHY                       geography
    SQL_VARIANT                     sql_variant
    GUID                            CHAR(36)
    MONEY                           money
    SMALLMONEY                      numeric(10, 4)

>>> SQLSERVER:
    JDBC TYPE                       SQLSERVER TYPE
-------------------------------------------------------------
    BOOLEAN                         bit
    BIT                             bit
    TINYINT                         tinyint
    SMALLINT                        smallint
    INTEGER                         int
    BIGINT                          bigint
    FLOAT                           float
    DOUBLE                          float
    REAL                            real
    NUMERIC                         NUMERIC(1~38,0~38)
    DECIMAL                         NUMERIC(1~38,0~38)
    CHAR
        1) > 8000                   varchar(max)
        2) <= 8000                  CHAR
    VARCHAR
        1) > 8000                   varchar(max)
        2) <= 8000                  VARCHAR
    CLOB                            varchar(max)
    LONGVARCHAR                     varchar(max)
    DATE                            date
    TIME_WITH_TIMEZONE              time
    TIMESTAMP_WITH_TIMEZONE         datetime2
    TIMESTAMP                       datetime2
    BINARY
        1) > 8000                   varbinary(max)
        2) <= 8000                  binary
    VARBINARY
        1) > 8000                   varbinary(max)
        2) <= 8000                  varbinary
    LONGVARBINARY                   image/varbinary(max)
    BLOB                            image/varbinary(max)
    NCHAR
        1) > 4000                   nvarchar(max)
        2) <= 4000                  nchar
    NVARCHAR
        1) > 4000                   nvarchar(max)
        2) <= 4000                  nvarchar
    LONGNVARCHAR                    nvarchar(max)
    NCLOB                           nvarchar(max)
    GEOMETRY                        geometry
    GEOGRAPHY                       geography
    SQL_VARIANT                     sql_variant
    GUID                            uniqueidentifier
    DATETIME                        datetime
    SMALLDATETIME                   smalldatetime
    DATETIMEOFFSET                  datetimeoffset
    MONEY                           money
    SMALLMONEY                      smallmoney

>>> MYSQL/MARIADB:
    JDBC TYPE                       MYSQL TYPE
-------------------------------------------------------------
    BIT                             BIT
    TINYINT                         TINYINT
    SMALLINT                        SMALLINT
    INTEGER                         INT
    BIGINT                          BIGINT
    FLOAT                           FLOAT
    REAL                            REAL
    DOUBLE                          DOUBLE
    NUMERIC                         DECIMAL
    DECIMAL                         DECIMAL
    NCHAR
        1) 255 ~ 65535              NVARCHAR
        2) < 255                    NCHAR
    CHAR
        1) 255 ~ 65535              VARCHAR
        2) < 255                    CHAR
    NVARCHAR
    VARCHAR
        1) 16777215 ~ 4294967295    LONGTEXT
        2) 65535 ~ 16777215         MEDIUMTEXT
        3) <= 65535                 VARCHAR
    LONGNVARCHAR                    LONGTEXT
    NCLOB                           LONGTEXT
    CLOB                            LONGTEXT
    LONGVARCHAR                     LONGTEXT
    DATE                            DATE
    TIME                            TIME
    TIME_WITH_TIMEZONE              TIME
    TIMESTAMP                       TIMESTAMP
    TIMESTAMP_WITH_TIMEZONE         TIMESTAMP
    BINARY
        1) > 4294967295             LONGBLOB
        1) 65535 ~ 4294967295       MEDIUMBLOB
        2) 255 ~ 65535              VARBINARY
        3) <= 255                   BINARY
    VARBINARY
        1) > 4294967295             LONGBLOB
        1) 65535 ~ 4294967295       MEDIUMBLOB
        2) 255 ~ 65535              BLOB
        3) <= 255                   VARBINARY
    BLOB                            LONGBLOB
    LONGVARBINARY                   LONGBLOB
    BOOLEAN                         BOOLEAN
    GEOMETRY                        GEOMETRY
    GUID                            VARCHAR(36)
    DATETIME                        DATETIME
    SMALLDATETIME                   DATETIME
    DATETIMEOFFSET                  DATETIME
    MONEY                           DECIMAL(19, 4)
    SMALLMONEY                      DECIMAL(10, 4)

>>> ACCESS DB:
    JDBC TYPE                       ACCESS TYPE
-------------------------------------------------------------
    BIT                             YESNO
    BOOLEAN                         YESNO
    TINYINT                         BYTE
    SMALLINT                        BYTE
    INTEGER                         INTEGER
    BIGINT                          LONG
    FLOAT                           SINGLE
    REAL                            SINGLE
    DOUBLE                          DOUBLE
    NUMERIC                         NUMERIC(1~28, 0~28)
    DECIMAL                         NUMERIC(1~28, 0~28)
    CHAR                            CHAR
        1) = 36                     GUID
    NCHAR                           NCHAR
    VARCHAR
        1) > 255                    MEMO
        2) <= 255                   TEXT
    NVARCHAR
        1) > 255                    MEMO
        2) <= 255                   TEXT
    LONGVARCHAR                     MEMO
    LONGNVARCHAR                    MEMO
    CLOB                            MEMO
    NCLOB                           MEMO
    DATE                            DATE
    TIME                            TIME
    TIME_WITH_TIMEZONE              TIME
    TIMESTAMP                       TIMESTAMP
    TIMESTAMP_WITH_TIMEZONE         TIMESTAMP
    BINARY
        1) > 255                    OLE
        2) <= 255                   BYTE
    VARBINARY
        1) > 255                    OLE
        2) <= 255                   BYTE
    LONGVARBINARY                   OLE
    BLOB                            OLE

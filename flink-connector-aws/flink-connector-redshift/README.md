# Flink Redshift Connector

This is the initial Proof of Concept for Flink connector redshift in 2 modes 

- read.mode = JDBC 
- read.mode = COPY

This POC only supports Sink Table.

## Connector Options
| Option | Required | Default  | Type | Description |
|:-------|:---------|:---------|:-----|:------------|
 hostname | required | none | String | Redshift connection hostname
 port | required | 5439 | Integer | Redshift connection port
 username | required | none | String | Redshift user username
 password | required | none | String | Redshift user password
 database-name | required | dev | String | Redshift database to connect
 table-name | required | none | String | Reshift table name
 sink.batch-size | optional | 1000 | Integer | The max flush size, over this will flush data.
 sink.flush-interval | optional | 1s | Duration | Over this flush interval mills, asynchronous threads will flush data.
 sink.max-retries | optional | 3 | Integer | The max retry times when writing records to the database failed.
 copy-mode | required | false | Boolean | Using Redshift COPY command to insert/upsert or not.
 copy-temp-s3-uri | conditional required | none | String | If the copy-mode=true then then Redshift COPY command must need a S3 URI.
 iam-role-arn | conditional required | none | String | If the copy-mode=true then then Redshift COPY command must need a IAM role. And this role must have the privilege and attache to the Redshift cluser.

**Update/Delete Data Considerations:**
The data is updated and deleted by the primary key.

## Data Type Mapping

| Flink Type          | Redshift Type                                         |
|:--------------------|:--------------------------------------------------------|
| CHAR                | VARCHAR                                                  |
| VARCHAR             | VARCHAR                                      |
| STRING              | VARCHAR                                          |
| BOOLEAN             | Boolean                                                   |
| BYTES               | Not supported                                             |
| DECIMAL             | Decimal   |
| TINYINT             | Int8                                                    |
| SMALLINT            | Int16                                           |
| INTEGER             | Int32                             |
| BIGINT              | Int64                                      |
| FLOAT               | Float32                                                 |
| DOUBLE              | Float64                                                 |
| DATE                | Date                                                    |
| TIME                | Timestamp                                                |
| TIMESTAMP           | Timestamp                                                |
| TIMESTAMP_LTZ       | Timestamp                                                |
| INTERVAL_YEAR_MONTH | Int32                                                   |
| INTERVAL_DAY_TIME   | Int64                                                   |
| ARRAY               | Not supported                                                   |
| MAP                 | Not supported                                                     |
| ROW                 | Not supported                                           |
| MULTISET            | Not supported                                           |
| RAW                 | Not supported                                           |



## How POC is Tested

### Create and sink a table in pure JDBC mode

```SQL

-- register a Redshift table `t_user` in flink sql.
CREATE TABLE t_user (
    `user_id` BIGINT,
    `user_type` INTEGER,
    `language` STRING,
    `country` STRING,
    `gender` STRING,
    `score` DOUBLE,
    PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
    'connector' = 'redshift',
    'hostname' = 'xxxx.redshift.awsamazon.com',
    'port' = '5439',
    'username' = 'awsuser',
    'password' = 'passwordxxxx',
    'database-name' = 'tutorial',
    'table-name' = 'users',
    'sink.batch-size' = '500',
    'sink.flush-interval' = '1000',
    'sink.max-retries' = '3'
);

-- write data into the Redshift table from the table `T`
INSERT INTO t_user
SELECT cast(`user_id` as BIGINT), `user_type`, `lang`, `country`, `gender`, `score`) FROM T;

```

### Create and sink a table in COPY mode

```SQL

-- register a Redshift table `t_user` in flink sql.
CREATE TABLE t_user (
    `user_id` BIGINT,
    `user_type` INTEGER,
    `language` STRING,
    `country` STRING,
    `gender` STRING,
    `score` DOUBLE,
    PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
    'connector' = 'redshift',
    'hostname' = 'xxxx.redshift.awsamazon.com',
    'port' = '5439',
    'username' = 'awsuser',
    'password' = 'passwordxxxx',
    'database-name' = 'tutorial',
    'table-name' = 'users',
    'sink.batch-size' = '500',
    'sink.flush-interval' = '1000',
    'sink.max-retries' = '3',
    'copy-mode' = 'true',
    'copy-temp-s3-uri' = 's3://bucket-name/key/temp',
    'iam-role-arn' = 'arn:aws:iam::xxxxxxxx:role/xxxxxRedshiftS3Rolexxxxx'
);
```

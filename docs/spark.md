# Spark Integration

Olympia can be used through the Spark Iceberg connector by leveraging the Olympia Iceberg Catalog or
Olympia Iceberg REST Catalog integrations.

## Configuration

### Olympia Iceberg Catalog

To configure an Iceberg catalog with Olympia in Spark, you should:

- Add Olympia Spark Iceberg runtime package to the Spark classpath
- Use the [Spark Iceberg connector configuration for custom catalog](https://iceberg.apache.org/docs/nightly/spark-configuration/#loading-a-custom-catalog).

For example, to start a Spark shell session with a Olympia Iceberg catalog named `demo`:

```shell
spark-shell \
  --jars iceberg-spark-runtime-3.5_2.12.jar,olympia-spark-iceberg-runtime-3.5_2.12.jar \
  --conf spark.sql.extensions=org.apache.spark.iceberg.extensions.IcebergSparkSessionExtensions,
                              org.format.olympia.spark.extensions.OlympiaSparkExtensions \
  --conf spark.sql.catalog.demo=org.apache.spark.iceberg.SparkCatalog \
  --conf spark.sql.catalog.demo.catalog-impl=org.format.olympia.iceberg.OlympiaIcebergCatalog \
  --conf spark.sql.catalog.demo.warehouse=s3://my-bucket
```

### Olympia Iceberg REST Catalog

To configure an Iceberg REST catalog with Olympia in Spark, you should:

- Start your Olympia IRC server
- Add Olympia Spark Iceberg runtime package to the Spark classpath
- Use the [Spark Iceberg connector configuration for IRC](https://iceberg.apache.org/docs/nightly/spark-configuration/#catalog-configuration).

For example, to start a Spark shell session with a Olympia IRC catalog named `demo` that is running at `http://localhost:8000`:

```shell
spark-shell \
  --jars iceberg-spark-runtime-3.5_2.12.jar \
  --conf spark.sql.extensions=org.apache.spark.iceberg.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.demo=org.apache.spark.iceberg.SparkCatalog \
  --conf spark.sql.catalog.demo.type=rest \
  --conf spark.sql.catalog.demo.uri=http://localhost:8000
```

!!! failure
    The IRC integration cannot work with the Olympia Spark SQL extensions at this moment 
    because of missing transaction features in IRC.
    It has no notion of a transaction start, and cannot track a transaction across operations.


## Using SQL Extensions

Olympia provides the following Spark SQL extensions for its Spark connector:

### BEGIN TRANSACTION

Begin a transaction.

```sql
BEGIN [ TRANSACTION ]
```

### COMMIT TRANSACTION

Commit a transaction.
This command can only be used after executing a `BEGIN TRANSACTION`

```sql
COMMIT [ TRANSACTION ]
```

### ROLLBACK TRANSACTION

Rollback a transaction.
This command can only be used after executing a `BEGIN TRANSACTION`

```sql
ROLLBACK [ TRANSACTION ]
```


## Using System Namespace

The Olympia Spark Iceberg connector offers the same system namespace support as the Iceberg catalog integration
to perform operations like create catalog and list distributed transactions.
See [Using System Namespace in Iceberg Catalog](./iceberg.md#using-system-namespace) for more details.

For examples:

```sql
-- create catalog
CREATE DATABASE sys;
       
SHOW DATABASES IN sys
---------
|name   |
---------    
|dtxns  |
     
-- list distributed transactions in the catalog
SHOW DATABASES IN sys.dtxns
------------
|name      |
------------    
|dtxn_123  |
|dtxn_456  |
```

## Using Distributed Transaction

The Spark Iceberg connector for Olympia offers the same distributed transaction support
as the Iceberg catalog integration using multi-level namespace.
See [Using Distributed Transaction in Iceberg Catalog](./iceberg.md#using-distributed-transaction) for more details.

For examples:

```sql
-- create a transaction with ID 1234
CREATE DATABASE system.dtxns.dtxn_1234
       WITH DBPROPERTIES ('isolation-level'='serializable')
       
-- list tables in transaction of ID 1234 under namespace ns1
SHOW TABLES IN sys.dtxns.dtxn_1234.ns1;
------
|name|
------     
|t1  |

SELECT * FROM sys.dtxns.dtxn_1234.ns1.t1;
-----------
|id |data |
-----------
|1  |abc  |
|2  |def  |

INSERT INTO sys.dtxns.dtxn_1234.ns1.t1 VALUES (3, 'ghi');

-- commit transaction with ID 1234
ALTER DATABASE sys.dtxns.dtxn_1234
      SET DBPROPERTIES ('commit' = 'true');
```
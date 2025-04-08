# Iceberg Catalog Integration

Olympia provides an implementation of the pluggable Java `Catalog` API standard in Apache Iceberg
 with class path `org.format.olympia.iceberg.OlympiaIcebergCatalog`.

See related Iceberg documentation for how to use it [standalone](https://iceberg.apache.org/docs/nightly/java-api-quickstart/) or
with various query engines like [Apache Spark](https://iceberg.apache.org/docs/nightly/spark-configuration/#catalog-configuration),
[Apache Flink](https://iceberg.apache.org/docs/nightly/flink/#catalog-configuration),
and [Apache Hive](https://iceberg.apache.org/docs/nightly/hive/#custom-iceberg-catalogs).

## Catalog Properties

The Olympia Iceberg catalog exposes the following catalog properties:

| Property Name       | Description                                                                                                                                                         | Required? | Default |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|---------|
| warehouse           | The Iceberg catalog warehouse location, should be set to the root location for the catalog storage                                                                  | Yes       |         |
| storage.type        | Type of storage. If not set, the type is inferred from the root URI scheme.                                                                                         | No        |         |
| storage.ops.<key\>  | Any property configuration for a specific type of storage operation.                                                                                                | No        |         |
| system.ns-name      | Name of the system namespace                                                                                                                                        | No        | sys     |
| dtxn.parent-ns-name | Name of the parent namespace for all distributed transactions. This parent namespace is within the system namespace and hold all distributed transaction namespaces | No        | dtxns   |
| dtxn.ns-prefix      | The prefix for a namespace to represent a distributed transaction                                                                                                   | No        | dtxn_   |

For example, a user can initialize an Olympia Iceberg catalog Java instance with:

```java
import io.Olympia.iceberg.OlympiaIcebergCatalog;
import io.Olympia.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogProperties;
import org.apache.iceberg.catalog.SupportsNamespaces;

Catalog catalog = new OlympiaIcebergCatalog();
catalog.initialize(
        ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION, // "warehouse"
                "s3://my-bucket"));

SupportsNamespaces nsCatalog = (SupportsNamespace) catalog;
```

## Operation Behavior

When using Olympia through Iceberg catalog, 
all the operations will first begin a transaction and then perform the operation.
If the operation modifies the object, it will commit the transaction to the catalog.

When using Iceberg transactions to perform multiple operations,
Olympia will begin a transaction, perform all the operations and then commit the transaction to the catalog.
For example:

```java
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
        
Table table = catalog.loadTable(TableIdentifier.of("ns1", "t1"));
Transaction txn = table.newTransaction();

txn.updateSchema()
    .addColumn("region", Types.StringType.get())
    .commit();

txn.updateSpec()
    .addField("region")
    .commit();

txn.commitTransaction();
```

In this sequence, an Olympia transaction will hold the schema and partition spec update,
and commit both changes to catalog in a single transaction.

## Using System Namespace

The system namespace is a special namespace with name determined by `system.ns-name` that exists
if an Olympia catalog is initialized at the `warehouse` location.
It contains information about the distributed transactions that are available to use in the catalog.

### Create a new Olympia catalog

If there is no Olympia catalog at the configured `warehouse` location, the system namespace will not exist.
The act of creating this system namespace represents creating the catalog.
At creation time, catalog definition fields can be supplied through the namespace properties.

For example:

```java
import org.apache.iceberg.Namespace;

nsCatalog.createNamespace(
        Namespace.of("sys"), 
        ImmutableMap.of("namespace_name_max_size_bytes", "256"));
```

## Using Distributed Transaction

You can use the Olympia transaction semantics through Iceberg multi-level namespace.

### Distributed transactions namespace

Under the system namespace, there is always a namespace with name determined by `dtxn.parent-ns-name`.
This is the parent namespace that holds all the distributed transactions.
Each distributed transaction is represented by a namespace with prefix determined by `dtxn.ns-prefix`,
followed by the transaction ID.

For example, if there are 2 distributed transactions with IDs `123` and `456`,
you should see the following namespace hierarchy:

```
└── sys
    └── dtxns
        ├── dtxn_123
        └── dtxn_456
```

### List all transactions

Users can list all the distributed transactions currently in the catalog
by doing a namespace listing of the parent namespace of all distributed transactions:

```java
nsCatalog.listNamespace(Namespace.of("sys", "dtxns"));
```

### Begin a transaction

If you create a namespace with a prefix matching the `dtxn.ns-prefix`,
and the namespace is within the system namespace, and also under the parent namespace `dtx.parent-ns-prefix`,
then it is considered as beginning a distributed transaction.

The namespace properties are used to provide runtime override options for the transaction. The following options are supported:

| Option Name     | Description                                                   |
|-----------------|---------------------------------------------------------------|
| isolation-level | The isolation level of this transaction                       |
| ttl-millis      | The duration for which a transaction is valid in milliseconds |

The act of creating such a namespace means to create a distributed transaction that is persisted in the catalog.
For example, consider a user creating a transaction with ID `123` with isolation level as `SERIALIZABLE`:

```java
nsCatalog.createNamespace(
        Namespace.of("sys", "dtxns", "dtxn_123"), 
        ImmutableMap.of("isolation-level", "serializable"));
```

### Using the transaction

After creation, a user can access the specific isolated version of the catalog under the namespace.
For example, consider an Olympia catalog with namespace `ns1` and table `t1`,
then the user should see a namespace `sys.dtxns.dtxn_123.ns1`
and a table `sys.dtxns.dtxn_123.ns1.t1` which the user can read and write to:

```java
assertThat(catalog.listNamespaces(Namespace.of("sys", "dtxns", "dtxn_123")))
        .containsExactly(Namespace.of("sys", "dtxns", "dtxn_123", "ns1"));

assertThat(catalog.listTables(Namespace.of("sys", "dtxns", "dtxn_123", "ns1")))
        .containsExactly(TableIdentifier.of("sys", "dtxns", "dtxn_123", "ns1", "t1"));
```

### Commit a transaction

In order to commit this transaction, set the namesapce property `commit` to `true`:

```java
nsCatalog.setProperties(
        Namespace.of("sys", "dtxns", "dtxn_123"), 
        ImmutbaleMap.of("commit", "true"));
```

### Rollback a transaction

In order to rollback a transaction, perform a drop namespace:

```java
nsCatalog.dropNamespace(
        Namespace.of("sys", "dtxns", "dtxn_123"));
```

## Iceberg REST Catalog

Olympia provides the ability to build an Iceberg REST Catalog (IRC) server
following the [IRC open catalog standard](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml).

The Olympia-backed IRC offers the same operation behavior as the Iceberg catalog integration
for using system namespace and distributed transaction.

## Apache Gravitino IRC Server

The easiest way to start an Olympia-backed IRC server is to use the Apache Gravitino IRC Server.
You can run the Gravitino Iceberg REST server integrated with Olympia backend in two ways.

### Using the Prebuilt Docker Image (Recommended)

Pull the docker image from official Olympia docker account
```
docker pull olympiaformat/olympia-gravitino-irc:latest
```

Run the Gravitino IRC Server container with port mapping
```
docker run -d -p 9001:9001 --name olympia-gravitino-irc olympiaformat/olympia-gravitino-irc
```

### Using Apache Gravitino Installation (Manual Setup)

Follow the [Apache Gravitino instructions](https://gravitino.apache.org/docs/0.8.0-incubating/how-to-install)
for downloading and installing the Gravitino software.
After the standard installation process, add the `olympia-core-0.0.1.jar` and `olympia-s3-0.0.1.jar` to your Java classpath or
copy those jars into Gravitino’s `lib/` directory .

#### Configuration

Update `gravitino-iceberg-rest-server.conf` with the following configuration:

| Configuration Item                          | Description                                                                       | Value                                              |
|---------------------------------------------|-----------------------------------------------------------------------------------|----------------------------------------------------|
| gravitino.iceberg-rest.catalog-backend      | The Catalog backend of the Gravitino Iceberg REST catalog service                 | custom                                             |
| gravitino.iceberg-rest.catalog-backend-impl | The Catalog backend implementation of the Gravitino Iceberg REST catalog service. | io.Olympia.iceberg.OlympiaIcebergCatalog           |
| gravitino.iceberg-rest.catalog-backend-name | The catalog backend name passed to underlying Iceberg catalog backend.            | any name you like, e.g. `olympia`                  |
| gravitino.iceberg-rest.uri                  | Iceberg REST catalog server address                                               | For local development, use `http://127.0.0.1:9001` |
| gravitino.iceberg-rest.warehouse            | Olympia catalog storage root location                                             | Any file path. Ex: `/tmp/olympia`                  |
| gravitino.iceberg-rest.<key\>               | Any other catalog properties                                                      |                                                    |

#### Running the server

To start the Gravitino Iceberg REST server
```
./bin/gravitino-iceberg-rest-server.sh start
```

To stop the Gravitino Iceberg REST server
```
./bin/gravitino-iceberg-rest-server.sh stop
```

Follow the [Apache Gravitino instructions](https://gravitino.apache.org/docs/0.8.0-incubating/iceberg-rest-service#starting-the-iceberg-rest-server)
for more detailed instructions on starting the IRC server and exploring the namespaces, tables and distributed transactions in the catalog.

#### Examples

List all IRC configurations:

```shell
curl -X GET "http://127.0.0.1:9001/iceberg/v1/config"
```

Create catalog:

```shell
curl -X POST "http://127.0.0.1:9001/iceberg/v1/namespaces" \
     -H "Content-Type: application/json" \
     -d '{
           "namespace": ["sys"],
           "properties": {}
         }'
```

List namespaces:

```shell
curl -X GET "http://127.0.0.1:9001/iceberg/v1/namespaces"
```
---
title: YugabyteDB gRPC connector transformers
headerTitle: YugabyteDB gRPC connector transformers
linkTitle: gRPC connector transformers
description: YugabyteDB gRPC connector transformers for Change Data Capture in YugabyteDB.
menu:
  preview:
    parent: debezium-connector-yugabytedb
    identifier: yugabytedb-grpc-connector-transformers
    weight: 50
type: docs
---

The connector comes bundled with some SMTs which can ease in the data flow and help achieve the right message format as per the requirement. The following transformers are bundled with the connector jar file available on [GitHub releases](https://github.com/yugabyte/debezium-connector-yugabytedb/releases):

* YBExtractNewRecordState
* PGCompatible

{{< note title="Important" >}}

The above mentioned transformers are only expected to be used with logical replication when the [plugin](../using-logical-replication/key-concepts#output-plugin) being used is `yboutput`.

{{ < /note >}}

Note that for maintaining simplicity, only the `before` and `after` fields of the `payload` of the message published by the connector will be mentioned in the following examples. Any information pertaining to the record schema, if same as the standard Debezium connector for Postgres, will be skipped.

Consider a table created using the following statement:

```sql
CREATE TABLE test (id INT PRIMARY KEY, name TEXT, aura INT);
```

The following DML statements will be used to demonstrate payload in case of individual replica identities:

```sql
-- statement 1
INSERT INTO test VALUES (1, 'Vaibhav', 9876);

-- statement 2
UPDATE test SET aura = 9999 WHERE id = 1;

-- statement 3
UPDATE test SET name = 'Vaibhav Kushwaha', aura = 10 WHERE id = 1;

-- statement 4
UPDATE test SET aura = NULL WHERE id = 1;

-- statement 5
DELETE FROM test WHERE id = 1;
```

## YBExtractNewRecordState

**Transformer class:** `io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState`

### PGCompatible

**Transformer class:** `io.debezium.connector.yugabytedb.transforms.PGCompatible`

By default, the YugabyteDB CDC service publishes events with a schema that only includes columns that have been modified. The source connector then sends the value as `null` for columns that are missing in the payload. Each column payload includes a `set` field that is used to signal if a column has been set to `null` because it wasn't present in the payload from YugabyteDB.

However, some sink connectors may not understand the preceding format. `PGCompatible` transforms the payload to a format that is compatible with the format of the standard change data events. Specifically, it transforms column schema and value to remove the set field and collapse the payload such that it only contains the data type schema and value.

PGCompatible differs from `YBExtractNewRecordState` by recursively modifying all the fields in a payload.

Following are the examples how the payload would look like for each [before image mode](../using-yugabytedb-grpc-replication/cdc-get-started/#before-image-modes).

### CHANGE

```
-- statement 1
"before":null,"after":{"id":1,"name":"Vaibhav","aura":9876}

-- statement 2
"before":null,"after":{"id":1,"name":null,"aura":9999}

-- statement 3
"before":null,"after":{"id":1,"name":"Vaibhav Kushwaha","aura":10}

-- statement 4
"before":null,"after":{"id":1,"name":null,"aura":null}

-- statement 5
"before":{"id":1,"name":null,"aura":null},"after":null
```

Do note that for statement 2 and 4, the columns which were not updated as a part of the `UPDATE` statement, they're `null` in the output field.

### FULL_ROW_NEW_IMAGE

```
-- statement 1
"before":null,"after":{"id":1,"name":"Vaibhav","aura":9876}

-- statement 2
"before":null,"after":{"id":1,"name":"Vaibhav","aura":9999}

-- statement 3
"before":null,"after":{"id":1,"name":"Vaibhav Kushwaha","aura":10}

-- statement 4
"before":null,"after":{"id":1,"name":"Vaibhav Kushwaha","aura":null}

-- statement 5
"before":{"id":1,"name":"Vaibhav Kushwaha","aura":null},"after":null
```

### ALL

```
-- statement 1
"before":null,"after":{"id":1,"name":"Vaibhav","aura":9876}

-- statement 2
"before":{"id":1,"name":"Vaibhav","aura":9876},"after":{"id":1,"name":"Vaibhav","aura":9999}

-- statement 3
"before":{"id":1,"name":"Vaibhav","aura":9999},"after":{"id":1,"name":"Vaibhav Kushwaha","aura":10}

-- statement 4
"before":{"id":1,"name":"Vaibhav Kushwaha","aura":10},"after":{"id":1,"name":"Vaibhav Kushwaha","aura":null}

-- statement 5
"before":{"id":1,"name":"Vaibhav Kushwaha","aura":null},"after":null
```

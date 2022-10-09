---
title: Drivers and ORMs for YugabyteDB
headerTitle: Drivers and ORMs
linkTitle: Drivers and ORMs
description: Connect your applications with supported drivers and ORMs
headcontent: Drivers and ORMs for YugabyteDB
image: /images/section_icons/sample-data/s_s1-sampledata-3x.png
aliases:
  - /drivers-orms/
menu:
  preview:
    identifier: drivers-orms
    parent: develop
    weight: 570
type: indexpage
showRightNav: true
---

The [Yugabyte Structured Query Language (YSQL) API](../api/ysql/) builds upon and extends a fork of the query layer from PostgreSQL, with the intent of supporting most PostgreSQL functionality. Client applications can use the [PostgreSQL drivers](https://www.postgresql.org/download/products/2-drivers-and-interfaces/) to read and write data into YugabyteDB databases.

The [Yugabyte Cloud Query Language (YCQL) API](../api/ycql/) is a semi-relational SQL API with roots in the [Cassandra Query Language (CQL)](https://cassandra.apache.org/doc/latest/cassandra/cql/index.html).

YSQL- and YCQL-compatible drivers are listed in the compatibility matrix below.

## Smart drivers

In addition to the compatible upstream PostgreSQL drivers, YugabyteDB also supports [smart drivers](smart-drivers/), which extend the PostgreSQL drivers to enable client applications to connect to YugabyteDB clusters without the need for external load balancers.

{{< note title="YugabyteDB Managed" >}}

To take advantage of smart driver load balancing features when connecting to clusters in YugabyteDB Managed, applications using smart drivers must be deployed in a VPC that has been peered with the cluster VPC. For applications that access the cluster from a non-peered network, use the upstream PostgreSQL driver instead; in this case, the cluster performs the load balancing. Applications that use smart drivers from non-peered networks fall back to the upstream driver behaviour automatically. For information on using smart drivers with YugabyteDB Managed, refer to [Using smart drivers with YugabyteDB Managed](smart-drivers/#using-smart-drivers-with-yugabytedb-managed).

{{< /note >}}

## Supported libraries

The following libraries are officially supported by YugabyteDB.

### Java

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [YugabyteDB JDBC Smart Driver](java/yugabyte-jdbc/) [Recommended] | Full | [CRUD Example](java/yugabyte-jdbc/) |
| [PostgreSQL JDBC Driver](java/postgres-jdbc/) | Full | [CRUD Example](java/postgres-jdbc/) |
| [YugabyteDB Java Driver for YCQL (3.10)](java/ycql/) | Full | [CRUD Example](java/ycql) |
| [YugabyteDB Java Driver for YCQL (4.6)](java/ycql-4.6/) | Full | [CRUD Example](java/ycql-4.6) |
| [Ebean](java/ebean/) | Full | [CRUD Example](java/ebean/) |
| [Hibernate](java/hibernate/) | Full | [CRUD Example](java/hibernate/) |
| [Spring Data YugabyteDB](../integrations/spring-framework/sdyb/) | Full | [CRUD Example](../integrations/spring-framework/sdyb/#examples) |
| [Spring Data JPA](../integrations/spring-framework/sd-jpa/) | Full | [Hello World](../develop/build-apps/java/ysql-spring-data/) |
<!-- | Micronaut | Beta |  | -->
<!-- | Quarkus | Beta |  | -->
<!-- | MyBatis | Full |  | -->

### Go

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [YugabyteDB PGX Smart Driver](go/yb-pgx/) [Recommended] | Full | [CRUD Example](go/yb-pgx/) |
| [PGX Driver](go/pgx/) | Full | [CRUD Example](go/pgx/) |
| [PQ Driver](go/pq/) | Full | [CRUD Example](go/pq/) |
| [YugabyteDB Go Driver for YCQL](go/ycql/) | Full | [CRUD Example](go/ycql) |
| [GORM](go/gorm/) | Full | [CRUD Example](go/gorm/) |
| [PG](go/pg/) | Full | [CRUD Example](go/pg/) |

### C

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [libpq C Driver](c/ysql/) | Full | [CRUD Example](c/ysql/) |

### C++

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [libpqxx C++ Driver](cpp/ysql/) | Full | [CRUD Example](cpp/ysql/) |
| [YugabyteDB C++ Driver for YCQL](cpp/ycql/) | Full | [CRUD Example](cpp/ycql/) |

### Node.js

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [YugabyteDB node-postgres Smart Driver](nodejs/yugabyte-node-driver/) [Recommended] | Full | [CRUD Example](nodejs/yugabyte-node-driver/) |
| [PostgreSQL node-postgres Driver](nodejs/postgres-node-driver/) | Full | [CRUD Example](nodejs/postgres-node-driver/) |
| [Sequelize](nodejs/sequelize/) | Full | [CRUD Example](nodejs/sequelize/) |
| [Prisma](nodejs/prisma/) | Full | [CRUD Example](nodejs/prisma/)

### C#

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [PostgreSQL Npgsql Driver](csharp/postgres-npgsql/) | Full | [CRUD Example](csharp/postgres-npgsql/) |
| [YugabyteDB C# Driver for YCQL](csharp/ycql/) | Full | [CRUD Example](csharp/ycql/) |
| [Entity Framework](csharp/entityframework/) | Full | [CRUD Example](csharp/entityframework/) |

### Python

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [YugabyteDB Psycopg2 Smart Driver](python/yugabyte-psycopg2/) [Recommended] | Full | [CRUD Example](python/yugabyte-psycopg2/) |
| [PostgreSQL Psycopg2 Driver](python/postgres-psycopg2/) | Full | [CRUD Example](python/postgres-psycopg2/) |
| aiopg | Full | [Hello World](../develop/build-apps/python/ysql-aiopg/) |
| [Django](python/django/) | Full | [CRUD Example](python/django/) |
| [SQLAlchemy](python/sqlalchemy/) | Full | [CRUD Example](python/sqlalchemy/) |

### Rust

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [Diesel](rust/diesel/) | Full | [CRUD example](rust/diesel/) |

### Ruby

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [Pg Gem Driver](ruby/ysql-pg/) | Full | [CRUD example](ruby/ysql-pg/) |
| [YugabyteDB Ruby Driver for YCQL](ruby/ycql/) | Full | [CRUD example](ruby/ycql/) |
| [YugabyteDB Ruby Driver for YCQL](ruby/ycql/) | Full | [CRUD example](ruby/ycql/) |
| [ActiveRecord ORM](ruby/activerecord/) | Full | [CRUD example](ruby/activerecord/) |

### PHP

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [php-pgsql Driver](php/ysql/) | Full | [CRUD example](php/ysql/) |

### Scala

| Driver/ORM | Support Level | Example apps |
| :--------- | :------------ | :----------- |
| [YugabyteDB Java Driver for YCQL](scala/ycql/) | Full | [CRUD example](scala/ycql/) |
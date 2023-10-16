---
title: Integrations
headerTitle: Integrations
linkTitle: Integrations
description: Integrate popular third party tools with YugabyteDB, including Presto, Prisma, Sequelize, Spring, Flyway, Django, Hasura, Kafka.
headcontent: Use YugabyteDB with popular third-party integrations
image: /images/section_icons/develop/api-icon.png
type: indexpage
showRightNav: true
---

YugabyteDB is wire compatible with PostgreSQL, that makes most PostgreSQL client drivers, ORM frameworks, and any other types of third-party database tools designed for PostgreSQL compatible with YugabyteDB. 
YugabyteDB has partnered with open-source projects, vendors to guarantee following popular PostgreSQL tools.

| Status | Descrption | 
| :--- | :--- | 
| Full    | For the tools with Full status, compatibility with the vast majority of the tool's features will be maintained. These tools are regularly tested against the latest version documented.| 
| Partial | These tools will eventually have full support from YugabyteDB. Although these tool’s core functions such as connecting and performing simple database operations are compatible with YugabyteDB, but full integration may require additional steps, lack support for all features, or exhibit unexpected behavior. | 

## Choose your integration

### Drivers and ORMs

{{< readfile "../drivers-orms/include-drivers-orms-list.md" >}}

### Schema migration

| Tool | Support | Tutorial |
| :--- | :--- | :--- |
| Liquibase | Full | [Liquibase](liquibase/) |
| Flyway    | Partial | [Flyway](flyway/) |
| Prisma    | Full | [Prisma](prisma/) |
| Schema Evolution Manager | Partial | [Schema Evolution Manager](schema-evolution-mgr/) |

### Data migration

| Tool | Support | Tutorial |
| :--- | :--- | :--- |
| PGmigrate | Partial | [PGmigrate](pgmigrate/) |
| YSQL Loader (pgloader) | Full | [YSQL Loader](ysql-loader/) |

### Data integration (CDC)

| Tool | Support | Tutorial |
| :--- | :--- | :--- |
| Apache Beam    | Partial | [Apache Beam](apache-beam/) |
| Apache Flink   | Partial | [Apache Flink](apache-flink/) |
| Apache Kafka   | Full | [Apache Kafka](apache-kafka/) |
| Akka Persistence | Partial | [Akka Persistence](akka-ysql/) |
| Confluent      | Full | [Confluent Cloud](../explore/change-data-capture/cdc-tutorials/cdc-confluent-cloud/) |
| Debezium       | Full | [Debezium](cdc/debezium/) |
| Hevo Data      | Partial | [Hevo Data](hevodata/) |
| Kinesis Data Streams | Full | [Kinesis](kinesis/) |
| RabbitMQ       | Partial | [RabbitMQ](rabbitmq/) |
| Synapse        | Full | [Synapse](../explore/change-data-capture/cdc-tutorials/cdc-azure-event-hub/) |

### GUI clients

| Tool | Support | Tutorial |
| :--- | :--- | :--- |
| Apache Superset   | Full | [Apache Superset](../tools/superset/) |
| Arctype   | Full | [Arctype](../tools/arctype/) |
| DBeaver   | Full | [DBeaver](../tools/dbeaver-ysql/) |
| DbSchema  | Full | [DbSchema](../tools/dbschema/) |
| Metabase  | Full | [Metabase](../tools/metabase/) |
| pgAdmin   | Full | [pgAdmin](../tools/pgadmin/) |
| SQL Workbench/J | Full | [SQL Workbench/J](../tools/sql-workbench/) |
| TablePlus | Full | [TablePlus](../tools/tableplus/) |

### Application frameworks

| Tool | Support | Tutorial |
| :--- | :--- | :--- |
| AtomicJar Testcontainers | Partial | [AtomicJar Testcontainers](atomicjar/) |
| Django | Full | [Django REST Framework](django-rest-framework/) |
| Hasura | Full | [Hasura](hasura/) |
| Spring | Full | [Spring](spring-framework/) |

### Development platforms

| IDE | Support | Tutorial |
| :--- | :--- | :--- |
| Caspio | Partial | [Caspio](caspio/) |
| Retool | Partial | [Retool](retool/) |
| Superblocks | Partial | [Superblocks](superblocks/) |
| Visual Studio Code | Partial | [Cassandra Workbench](../tools/visualstudioworkbench/) |

### Data discovery and metadata

| Tool | Support | Tutorial |
| :--- | :--- | :--- |
| Dataedo     | Partial | [Dataedo](dataedo/) |
| Datahub     | Partial | [Datahub](datahub/) |
| DQ Analyzer | Partial | [Ataccama DQ Analyzer](ataccama/) |
| Metacat     | Partial | [Metacat](metacat/) |

### Security

| Tool | Support | Tutorial |
| :--- | :--- | :--- |
| Hashicorp Vault      | Full | [Hashicorp Vault](hashicorp-vault/) |
| WSO2 Identity Server | Full | [WSO2 Identity Server](wso2/) |

### Applications powered by YugabyteDB

| Tool | Support | Tutorial |
| :--- | :--- | :--- |
| Camunda | Partial | [Camunda](camunda/) |

### Other

| Tool | Latest tested version | Support | Tutorial |
| :--- | :--- | :--- | :--- |
| Apache Atlas |      | Partial | [Apache Atlas](atlas-ycql/) |
| Apache Spark | 3.30 | Full | [Apache Spark](apache-spark/) |
| Jaeger       |      | Full | [Jaeger](jaeger/) |
| JanusGraph   |      | Full | [JanusGraph](janusgraph/) |
| KairosDB     |      | Full | [KairosDB](kairosdb/) |
| Mirantis MKE |      | Partial | [Mirantis](mirantis/) |
| Presto       |      | Partial | [Presto](presto/) |

<!--
<ul class="nav yb-pills">

  <li>
    <a href="akka-ysql/">
      <img src="/images/section_icons/develop/ecosystem/akka-icon.png">
      Akka Persistence
    </a>
  </li>

  <li>
    <a href="atlas-ycql/">
      <img src="/images/section_icons/develop/ecosystem/atlas-icon.png">
      Apache Atlas
    </a>
  </li>
  <li>
    <a href="apache-beam/">
      <img src="/images/section_icons/develop/ecosystem/beam.png">
      Apache Beam
    </a>
  </li>
  <li>
    <a href="apache-flink/">
      <img src="/images/section_icons/develop/ecosystem/apache-flink.png">
      Apache Flink
    </a>
  </li>

  <li>
    <a href="apache-kafka/">
      <img src="/images/section_icons/develop/ecosystem/apache-kafka-icon.png">
      Apache Kafka
    </a>
  </li>

  <li>
    <a href="apache-spark/">
      <img src="/images/section_icons/develop/ecosystem/apache-spark.png">
      Apache Spark
    </a>
  </li>

  <li>
    <a href="ataccama/">
      <img src="/images/section_icons/develop/ecosystem/ataccama.png">
      Ataccama DQ Analyzer
    </a>
  </li>

  <li>
    <a href="atomicjar/">
      <img src="/images/section_icons/develop/ecosystem/atomicjar-icon.png">
      AtomicJar Testcontainers
    </a>
  </li>

  <li>
    <a href="camunda/">
      <img src="/images/section_icons/develop/ecosystem/camunda.png">
      Camunda
    </a>
  </li>

  <li>
    <a href="caspio/">
      <img src="/images/section_icons/develop/ecosystem/caspio.png">
      Caspio
    </a>
  </li>

   <li>
    <a href="datahub/">
      <img src="/images/section_icons/develop/ecosystem/datahub.png">
      Datahub
    </a>
  </li>

  <li>
    <a href="dataedo/">
      <img src="/images/section_icons/develop/ecosystem/dataedo.png">
      Dataedo
    </a>
  </li>

  <li>
    <a href="cdc/debezium/">
      <img src="/images/section_icons/develop/ecosystem/debezium.png">
      Debezium
    </a>
  </li>

  <li>
    <a href="django-rest-framework/">
      <img src="/images/section_icons/develop/ecosystem/django-icon.png">
      Django
    </a>
  </li>

  <li>
    <a href="flyway/">
      <img src="/images/section_icons/develop/ecosystem/flyway.png">
      Flyway
    </a>
  </li>

  <li>
    <a href="gorm/">
      <img src="/images/section_icons/develop/ecosystem/gorm-icon.png">
      GORM
    </a>
  </li>

  <li>
    <a href="hashicorp-vault/">
      <img src="/images/section_icons/develop/ecosystem/hashicorp-vault.png">
      Hashicorp Vault
    </a>
  </li>
  <li>
    <a href="hasura/">
      <img src="/images/section_icons/develop/ecosystem/hasura.png">
      Hasura
    </a>
  </li>

   <li>
    <a href="hevodata/">
      <img src="/images/section_icons/develop/ecosystem/hevodata.png">
      Hevo Data
    </a>
  </li>

  <li>
    <a href="jaeger/">
      <img src="/images/section_icons/develop/ecosystem/jaeger.png">
      Jaeger
    </a>
  </li>
  <li>
    <a href="janusgraph/">
      <img src="/images/section_icons/develop/ecosystem/janusgraph.png">
      JanusGraph
    </a>
  </li>

  <li>
    <a href="kairosdb/">
      <img src="/images/section_icons/develop/ecosystem/kairosdb.png">
      KairosDB
    </a>
  </li>

  <li>
    <a href="kinesis/">
      <img src="/images/section_icons/develop/ecosystem/kinesis.png">
      Kinesis Data Streams
    </a>
  </li>

  <li>
    <a href="liquibase/">
      <img src="/images/section_icons/develop/ecosystem/liquibase.png">
      Liquibase
    </a>
  </li>

  <li>
    <a href="metabase/">
      <img src="/images/section_icons/develop/ecosystem/metabase.png">
      Metabase
    </a>
  </li>

  <li>
    <a href="metacat/">
      <img src="/images/section_icons/develop/ecosystem/metacat.png">
      Metacat
    </a>
  </li>

   <li>
    <a href="mirantis/">
      <img src="/images/section_icons/develop/ecosystem/mirantis.png">
      Mirantis MKE
    </a>
  </li>
   <li>
    <a href="pgmigrate/">
      <img src="/images/section_icons/develop/ecosystem/pgmigrate.png">
      PGmigrate
    </a>
  </li>
  <li>
    <a href="presto/">
      <img src="/images/section_icons/develop/ecosystem/presto-icon.png">
      Presto
    </a>
  </li>

  <li>
    <a href="prisma/">
      <img src="/images/develop/graphql/prisma/prisma.png">
      Prisma
    </a>
  </li>

  <li>
    <a href="rabbitmq/">
      <img src="/images/section_icons/develop/ecosystem/rabbitmq.png">
      RabbitMQ
    </a>
  </li>

  <li>
    <a href="retool/">
      <img src="/images/section_icons/develop/ecosystem/retool.png">
      Retool
    </a>
  </li>

  <li>
    <a href="schema-evolution-mgr/">
      Schema Evolution Manager
    </a>
  </li>

  <li>
    <a href="sequelize/">
      <img src="/images/section_icons/develop/ecosystem/sequelize.png">
      Sequelize
    </a>
  </li>

  <li>
    <a href="spring-framework/">
      <img src="/images/section_icons/develop/ecosystem/spring.png">
      Spring
    </a>
  </li>

  <li>
    <a href="sqlalchemy/">
      <img src="/images/section_icons/develop/ecosystem/sqlalchemy.png">
      SQLAlchemy
    </a>
  </li>

  <li>
    <a href="superblocks/">
      <img src="/images/section_icons/develop/ecosystem/superblocks.png">
      Superblocks
    </a>
  </li>

  <li>
    <a href="wso2/">
      <img src="/images/section_icons/develop/ecosystem/wso2.png">
      WSO2 Identity Server
    </a>
  </li>

  <li>
    <a href="ysql-loader/">
      <i class="icon-postgres"></i>
      YSQL Loader
    </a>
  </li>

</ul>
-->

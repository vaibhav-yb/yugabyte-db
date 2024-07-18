---
title: Advanced Configurations for CDC using Logical Replication
headerTitle: Advanced Configurations for CDC using Logical Replication
linkTitle: Advanced Configurations
description: Advanced Configurations for Logical Replication.
headcontent: Advanced Configurations for CDC using Logical Replication
menu:
  preview:
    parent: explore-change-data-capture-logical-replication
    identifier: advanced-configurations
    weight: 40
type: docs
---

## GFLAGS

The following `tserver` flags can be used to tune logical replication deployment configuration.

- [ysql_yb_default_replica_identity](../../../../reference/configuration/yb-tserver/#ysql_yb_default_replica_identity)
- [cdcsdk_enable_dynamic_table_support](../../../../reference/configuration/yb-tserver/#cdcsdk_enable_dynamic_table_support)
- [cdcsdk_publication_list_refresh_interval_secs](../../../../reference/configuration/yb-tserver/#cdcsdk_publication_list_refresh_interval_secs)
- [cdcsdk_max_consistent_records](../../../../reference/configuration/yb-tserver/#cdcsdk_max_consistent_records)
- [cdcsdk_wal_getchanges_resp_max_size_bytes](../../../../reference/configuration/yb-tserver/#cdcsdk_wal_getchanges_resp_max_size_bytes)

## Retention of Resources

<!-- Content to be shared by Anand -->
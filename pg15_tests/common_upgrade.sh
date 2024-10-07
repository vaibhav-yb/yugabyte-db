# This file is sourced by each upgrade test. It contains shared code, such as
# functions and variables.

# Upgrade tests use a 3-node cluster and need to reference the second and third nodes.
pghost2=127.0.0.$((ip_start + 1))
pghost3=127.0.0.$((ip_start + 2))

# TEST_always_return_consensus_info_for_succeeded_rpc=false is needed to upgrade a release build to
# debug.
# On MacOS, pg_client_use_shared_memory harms initdb performance significantly.
common_pg15_flags="TEST_always_return_consensus_info_for_succeeded_rpc=false,pg_client_use_shared_memory=false"
# yb_enable_expression_pushdown=false is needed because the expression pushdown rewriter is not yet
# implemented.
common_tserver_flags='"ysql_pg_conf_csv=yb_enable_expression_pushdown=false"'

pg11_enable_db_catalog_flag="allowed_preview_flags_csv=ysql_enable_db_catalog_version_mode,ysql_enable_db_catalog_version_mode=true"

# Downloads, runs, and pushds the directory for pg11.
# Sets $pg11path to the pg11 directory.
run_and_pushd_pg11() {
  prefix="/tmp"
  ybversion_pg11="2.20.2.2"
  ybbuild="b1"
  if [[ $OSTYPE = linux* ]]; then
    arch="linux-x86_64"
    tarbin="tar"
  fi
  if [[ $OSTYPE = darwin* ]]; then
    arch="darwin-x86_64"
    tarbin="gtar"
  fi
  ybfilename_pg11="yugabyte-$ybversion_pg11-$ybbuild-$arch.tar.gz"

  if [ ! -f "$prefix"/"$ybfilename_pg11" ]; then
    curl "https://downloads.yugabyte.com/releases/$ybversion_pg11/$ybfilename_pg11" \
      -o "$prefix"/"$ybfilename_pg11"
  fi

  "$tarbin" xzf "$prefix"/"$ybfilename_pg11" --skip-old-files -C "$prefix"

  if [[ $OSTYPE = linux* ]]; then
    "$prefix/yugabyte-$ybversion_pg11/bin/post_install.sh"
  fi

  pg11path="$prefix/yugabyte-$ybversion_pg11"
  pushd "$pg11path"
  yb_ctl_destroy_create --rf=3
  ysqlsh <<EOT
  SET yb_non_ddl_txn_for_sys_tables_allowed=true;
  SELECT yb_fix_catalog_version_table(true);
  SET yb_non_ddl_txn_for_sys_tables_allowed = false;
EOT
  yb_ctl restart \
  --tserver_flags="$common_tserver_flags,$pg11_enable_db_catalog_flag" \
  --master_flags="$pg11_enable_db_catalog_flag"
}

upgrade_masters() {
  for i in {1..3}; do
    yb_ctl restart_node $i --master \
      --master_flags="TEST_online_pg11_to_pg15_upgrade=true,master_join_existing_universe=true,$common_pg15_flags"
  done
}

run_ysql_catalog_upgrade() {
  echo run_ysql_catalog_upgrade starting at $(date +"%r")
  build/latest/bin/yb-admin --init_master_addrs=127.0.0.200:7100 --timeout_ms=300000 \
    ysql_major_version_catalog_upgrade
  echo run_ysql_catalog_upgrade finished at $(date +"%r")
}

# Restarts the masters in a mode that's aware of the PG11 to PG15 upgrade process, then runs ysql
# major upgrade to populate the pg15 catalog.
# Must be run while the current directory is a pg15 directory, typically the directory for the
# source code being built.
upgrade_masters_run_ysql_catalog_upgrade() {
  upgrade_masters
  run_ysql_catalog_upgrade
}

# Restart node 2 using PG15 binaries.
# Due to historic reasons we first restart the 2nd node instead of the 1st.
restart_node_2_in_pg15() {
  yb_ctl restart_node 2 --tserver_flags="$common_tserver_flags,$common_pg15_flags"
}

run_preflight_checks() {
  build/latest/postgres/bin/pg_upgrade --old-datadir "$data_dir/node-1/disk-1/pg_data" \
    --old-host "$PGHOST" --old-port 5433 --username "yugabyte" --check
}

verify_simple_table_mixed_cluster() {
  # Insert from PG15
  diff <(ysqlsh 2 <<EOT | sed 's/ *$//'
SHOW server_version_num;
INSERT INTO t VALUES (15);
SELECT * FROM t ORDER BY a;
EOT
) - <<EOT
 server_version_num
--------------------
 150002
(1 row)

INSERT 0 1
 a
----
  1
  2
 15
(3 rows)

EOT

  # Insert from PG11, and note the PG15 insertion is visible
  diff <(ysqlsh <<EOT | sed 's/ *$//'
SHOW server_version_num;
INSERT INTO t VALUES (11);
SELECT * FROM t ORDER BY a;
EOT
) - <<EOT
 server_version_num
--------------------
 110002
(1 row)

INSERT 0 1
 a
----
  1
  2
 11
 15
(4 rows)

EOT
}

verify_simple_table_after_finalize() {
  diff <(ysqlsh <<EOT | sed 's/ *$//'
SHOW server_version_num;
SELECT * FROM t ORDER BY a;
CREATE INDEX ON t (a);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM t WHERE a = 11;
SELECT COUNT(*) FROM t WHERE a = 11;
EOT
) - <<EOT
 server_version_num
--------------------
 150002
(1 row)

 a
----
  1
  2
 11
 15
(4 rows)

CREATE INDEX
                QUERY PLAN
------------------------------------------
 Finalize Aggregate
   ->  Index Only Scan using t_a_idx on t
         Index Cond: (a = 11)
         Partial Aggregate: true
(4 rows)

 count
-------
     1
(1 row)

EOT
}

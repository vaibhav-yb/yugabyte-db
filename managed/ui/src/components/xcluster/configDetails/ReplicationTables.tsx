import React, { useState } from 'react';
import { BootstrapTable, TableHeaderColumn } from 'react-bootstrap-table';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import { useDispatch, useSelector } from 'react-redux';
import { toast } from 'react-toastify';
import { Dropdown, MenuItem } from 'react-bootstrap';

import { closeDialog, openDialog } from '../../../actions/modal';
import {
  editXClusterConfigTables,
  fetchTablesInUniverse,
  fetchTaskUntilItCompletes
} from '../../../actions/xClusterReplication';
import { formatSchemaName } from '../../../utils/Formatters';
import { YBButton } from '../../common/forms/fields';
import {
  formatBytes,
  CurrentTableReplicationLag,
  augmentTablesWithXClusterDetails
} from '../ReplicationUtils';
import DeleteReplicactionTableModal from './DeleteReplicactionTableModal';
import { ReplicationLagGraphModal } from './ReplicationLagGraphModal';
import { YBLabelWithIcon } from '../../common/descriptors';
import ellipsisIcon from '../../common/media/more.svg';
import { api } from '../../../redesign/helpers/api';
import { XClusterModalName, XClusterTableStatus } from '../constants';
import { YBErrorIndicator, YBLoading } from '../../common/indicators';
import { XClusterTableStatusLabel } from '../XClusterTableStatusLabel';

import { TableType, TableTypeLabel, YBTable } from '../../../redesign/helpers/dtos';
import { XClusterConfig, XClusterTable } from '../XClusterTypes';

import styles from './ReplicationTables.module.scss';

interface props {
  xClusterConfig: XClusterConfig;
}

const TABLE_MIN_PAGE_SIZE = 10;

export function ReplicationTables({ xClusterConfig }: props) {
  const [deleteTableDetails, setDeleteTableDetails] = useState<YBTable>();
  const [openTableLagGraphDetails, setOpenTableLagGraphDetails] = useState<YBTable>();

  const dispatch = useDispatch();
  const { visibleModal } = useSelector((state: any) => state.modal);
  const queryClient = useQueryClient();

  const showAddTablesToClusterModal = () => {
    dispatch(openDialog(XClusterModalName.ADD_TABLE_TO_CONFIG));
  };

  const sourceUniverseTablesQuery = useQuery<YBTable[]>(
    ['universe', xClusterConfig.sourceUniverseUUID, 'tables'],
    () => fetchTablesInUniverse(xClusterConfig.sourceUniverseUUID).then((respone) => respone.data)
  );

  const sourceUniverseQuery = useQuery(['universe', xClusterConfig.sourceUniverseUUID], () =>
    api.fetchUniverse(xClusterConfig.sourceUniverseUUID)
  );

  const removeTableFromXCluster = useMutation(
    (replication: XClusterConfig) => {
      return editXClusterConfigTables(replication.uuid, replication.tables);
    },
    {
      onSuccess: (resp, replication) => {
        fetchTaskUntilItCompletes(resp.data.taskUUID, (err: boolean) => {
          if (!err) {
            queryClient.invalidateQueries(['Xcluster', replication.uuid]);
            dispatch(closeDialog());
            toast.success(`"${deleteTableDetails?.tableName}" table removed successfully`);
          } else {
            toast.error(
              <span className="alertMsg">
                <i className="fa fa-exclamation-circle" />
                <span>Task Failed.</span>
                <a href={`/tasks/${resp.data.taskUUID}`} target="_blank" rel="noopener noreferrer">
                  View Details
                </a>
              </span>
            );
          }
        });
      },
      onError: (err: any) => {
        toast.error(err.response.data.error);
      }
    }
  );

  if (
    sourceUniverseTablesQuery.isLoading ||
    sourceUniverseTablesQuery.isIdle ||
    sourceUniverseQuery.isLoading ||
    sourceUniverseQuery.isIdle
  ) {
    return <YBLoading />;
  }
  if (sourceUniverseTablesQuery.isError || sourceUniverseQuery.isError) {
    return <YBErrorIndicator />;
  }

  const tablesInConfig = augmentTablesWithXClusterDetails(
    sourceUniverseTablesQuery.data,
    xClusterConfig.tableDetails
  );
  const isActiveTab = window.location.search === '?tab=tables';
  const sourceUniverse = sourceUniverseQuery.data;

  return (
    <div className={styles.rootContainer}>
      <div className={styles.headerSection}>
        <span className={styles.infoText}>Tables selected for Replication</span>
        <div className={styles.actionBar}>
          <YBButton
            onClick={showAddTablesToClusterModal}
            btnIcon="fa fa-plus"
            btnText="Add Tables"
          />
        </div>
      </div>
      <div className={styles.replicationTable}>
        <BootstrapTable
          data={tablesInConfig}
          tableBodyClass={styles.table}
          trClassName="tr-row-style"
          pagination={tablesInConfig && tablesInConfig.length > TABLE_MIN_PAGE_SIZE}
        >
          <TableHeaderColumn dataField="tableUUID" isKey={true} hidden />
          <TableHeaderColumn dataField="tableName">Table Name</TableHeaderColumn>
          <TableHeaderColumn
            dataField="pgSchemaName"
            dataFormat={(cell: string, row: YBTable) => formatSchemaName(row.tableType, cell)}
          >
            Schema Name
          </TableHeaderColumn>
          <TableHeaderColumn
            dataField="tableType"
            dataFormat={(cell: TableType) => TableTypeLabel[cell]}
          >
            Table Type
          </TableHeaderColumn>
          <TableHeaderColumn dataField="keySpace">Keyspace</TableHeaderColumn>
          <TableHeaderColumn dataField="sizeBytes" dataFormat={(cell) => formatBytes(cell)}>
            Size
          </TableHeaderColumn>
          <TableHeaderColumn
            dataField="status"
            dataFormat={(cell: XClusterTableStatus, row: XClusterTable) => (
              <XClusterTableStatusLabel
                status={cell}
                tableUUID={row.tableUUID}
                nodePrefix={sourceUniverse.universeDetails.nodePrefix}
                universeUUID={sourceUniverse.universeUUID}
              />
            )}
          >
            Status
          </TableHeaderColumn>
          <TableHeaderColumn
            dataFormat={(_cell, row) => (
              <span className="lag-text">
                <CurrentTableReplicationLag
                  tableUUID={row.tableUUID}
                  nodePrefix={sourceUniverse.universeDetails.nodePrefix}
                  queryEnabled={isActiveTab}
                  sourceUniverseUUID={xClusterConfig.sourceUniverseUUID}
                  xClusterConfigStatus={xClusterConfig.status}
                />
              </span>
            )}
          >
            Current lag
          </TableHeaderColumn>
          <TableHeaderColumn
            columnClassName={styles.tableActionColumn}
            width="160px"
            dataField="action"
            dataFormat={(_, row) => (
              <>
                <YBButton
                  className={styles.actionButton}
                  btnIcon="fa fa-line-chart"
                  onClick={(e: any) => {
                    setOpenTableLagGraphDetails(row);
                    dispatch(openDialog(XClusterModalName.TABLE_REPLICATION_LAG_GRAPH));
                    e.currentTarget.blur();
                  }}
                />
                <Dropdown id={`${styles.tableActionColumn}_dropdown`} pullRight>
                  <Dropdown.Toggle noCaret className={styles.actionButton}>
                    <img src={ellipsisIcon} alt="more" className="ellipsis-icon" />
                  </Dropdown.Toggle>
                  <Dropdown.Menu>
                    <MenuItem
                      onClick={() => {
                        setDeleteTableDetails(row);
                        dispatch(openDialog(XClusterModalName.REMOVE_TABLE_FROM_CONFIG));
                      }}
                    >
                      <YBLabelWithIcon className={styles.dropdownMenuItem} icon="fa fa-times">
                        Remove Table
                      </YBLabelWithIcon>
                    </MenuItem>
                  </Dropdown.Menu>
                </Dropdown>
              </>
            )}
          ></TableHeaderColumn>
        </BootstrapTable>
      </div>
      {openTableLagGraphDetails && (
        <ReplicationLagGraphModal
          tableDetails={openTableLagGraphDetails}
          replicationUUID={xClusterConfig.uuid}
          universeUUID={sourceUniverse.universeUUID}
          nodePrefix={sourceUniverse.universeDetails.nodePrefix}
          queryEnabled={isActiveTab}
          visible={visibleModal === XClusterModalName.TABLE_REPLICATION_LAG_GRAPH}
          onHide={() => {
            dispatch(closeDialog());
          }}
        />
      )}
      <DeleteReplicactionTableModal
        deleteTableName={deleteTableDetails?.tableName ?? ''}
        onConfirm={() => {
          removeTableFromXCluster.mutate({
            ...xClusterConfig,
            tables: xClusterConfig.tables.filter((t) => t !== deleteTableDetails?.tableUUID)
          });
        }}
        onCancel={() => {
          dispatch(closeDialog());
        }}
      />
    </div>
  );
}

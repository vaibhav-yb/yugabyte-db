import axios from 'axios';
import moment from 'moment';

import { ROOT_URL } from '../config';
import { XClusterConfig, Metrics } from '../components/xcluster';
import { getCustomerEndpoint } from './common';
import { MetricName, XClusterConfigState } from '../components/xcluster/constants';

// TODO: Move this out of the /actions folder since these functions aren't Redux actions.

export function getUniverseInfo(universeUUID: string) {
  const cUUID = localStorage.getItem('customerId');
  return axios.get(`${ROOT_URL}/customers/${cUUID}/universes/${universeUUID}`);
}

export function fetchUniversesList() {
  const cUUID = localStorage.getItem('customerId');
  return axios.get(`${ROOT_URL}/customers/${cUUID}/universes`);
}

export function fetchTablesInUniverse(universeUUID: string | undefined) {
  if (universeUUID) {
    const customerId = localStorage.getItem('customerId');
    return axios.get(`${ROOT_URL}/customers/${customerId}/universes/${universeUUID}/tables`);
  }
  return Promise.reject('Querying universe tables failed: No universe UUID provided.');
}

export function createXClusterReplication(
  targetUniverseUUID: string,
  sourceUniverseUUID: string,
  name: string,
  tables: string[],
  bootstrapParams?: {
    tables: string[];
    backupRequestParams: any;
  }
) {
  const customerId = localStorage.getItem('customerId');
  return axios.post(`${ROOT_URL}/customers/${customerId}/xcluster_configs`, {
    sourceUniverseUUID,
    targetUniverseUUID,
    name,
    tables,
    ...(bootstrapParams !== undefined && { bootstrapParams })
  });
}

export function restartXClusterConfig(
  xClusterUUID: string,
  tables: string[],
  bootstrapParams: { backupRequestParams: any }
) {
  const customerId = localStorage.getItem('customerId');
  return axios.post(`${ROOT_URL}/customers/${customerId}/xcluster_configs/${xClusterUUID}`, {
    tables,
    bootstrapParams
  });
}

export function isBootstrapRequired(sourceUniverseUUID: string, tableUUIDs: string[]) {
  const customerId = localStorage.getItem('customerId');
  return Promise.all(
    tableUUIDs.map((tableUUID) => {
      return axios
        .post<{ [tableUUID: string]: boolean }>(
          `${ROOT_URL}/customers/${customerId}/universes/${sourceUniverseUUID}/need_bootstrap`,
          { tables: [tableUUID] }
        )
        .then((response) => response.data);
    })
  );
}

export function isCatchUpBootstrapRequired(
  xClusterConfigUUID: string | undefined,
  tableUUIDs: string[] | undefined
) {
  const customerId = localStorage.getItem('customerId');
  if (tableUUIDs && xClusterConfigUUID) {
    return Promise.all(
      tableUUIDs.map((tableUUID) => {
        return axios
          .post<{ [tableUUID: string]: boolean }>(
            `${ROOT_URL}/customers/${customerId}/xcluster_configs/${xClusterConfigUUID}/need_bootstrap`,
            { tables: [tableUUID] }
          )
          .then((response) => response.data);
      })
    );
  }
  const errorMsg = xClusterConfigUUID ? 'No table UUIDs provided' : 'No xCluster UUID provided';
  return Promise.reject(`Querying bootstrap requirement failed: ${errorMsg}.`);
}

export function fetchXClusterConfig(uuid: string) {
  const customerId = localStorage.getItem('customerId');
  return axios
    .get<XClusterConfig>(`${ROOT_URL}/customers/${customerId}/xcluster_configs/${uuid}`)
    .then((response) => response.data);
}

export function editXClusterState(replication: XClusterConfig, state: XClusterConfigState) {
  const customerId = localStorage.getItem('customerId');
  return axios.put(`${ROOT_URL}/customers/${customerId}/xcluster_configs/${replication.uuid}`, {
    status: state
  });
}

export function editXclusterName(replication: XClusterConfig) {
  const customerId = localStorage.getItem('customerId');
  return axios.put(`${ROOT_URL}/customers/${customerId}/xcluster_configs/${replication.uuid}`, {
    name: replication.name
  });
}

export function editXClusterConfigTables(
  xClusterUUID: string,
  tables: string[],
  bootstrapParams?: {
    tables: string[];
    backupRequestParams: any;
  }
) {
  const customerId = localStorage.getItem('customerId');
  return axios.put(`${ROOT_URL}/customers/${customerId}/xcluster_configs/${xClusterUUID}`, {
    tables: tables,
    ...(bootstrapParams !== undefined && { bootstrapParams })
  });
}

export function deleteXclusterConfig(uuid: string, isForceDelete: boolean) {
  const customerId = localStorage.getItem('customerId');
  return axios.delete(
    `${ROOT_URL}/customers/${customerId}/xcluster_configs/${uuid}?isForceDelete=${isForceDelete}`
  );
}

export function queryLagMetricsForUniverse(
  nodePrefix: string | undefined,
  replicationUUID: string
) {
  const DEFAULT_GRAPH_FILTER = {
    start: moment().utc().subtract('1', 'hour').format('X'),
    end: moment().utc().format('X'),
    nodePrefix,
    metrics: [MetricName.TSERVER_ASYNC_REPLICATION_LAG_METRIC],
    xClusterConfigUuid: replicationUUID
  };

  const customerUUID = localStorage.getItem('customerId');
  return axios
    .post<Metrics<'tserver_async_replication_lag_micros'>>(
      `${ROOT_URL}/customers/${customerUUID}/metrics`,
      DEFAULT_GRAPH_FILTER
    )
    .then((response) => response.data);
}

export function queryLagMetricsForTable(
  tableId: string,
  nodePrefix: string | undefined,
  start = moment().utc().subtract('1', 'hour').format('X'),
  end = moment().utc().format('X')
) {
  const DEFAULT_GRAPH_FILTER = {
    start,
    end,
    tableId,
    nodePrefix,
    metrics: [MetricName.TSERVER_ASYNC_REPLICATION_LAG_METRIC]
  };
  const customerUUID = localStorage.getItem('customerId');
  return axios
    .post<Metrics<'tserver_async_replication_lag_micros'>>(
      `${ROOT_URL}/customers/${customerUUID}/metrics`,
      DEFAULT_GRAPH_FILTER
    )
    .then((response) => response.data);
}

export function fetchUniverseDiskUsageMetric(
  nodePrefix: string,
  start = moment().utc().subtract('1', 'hour').format('X'),
  end = moment().utc().format('X')
) {
  const graphFilter = {
    start,
    end,
    nodePrefix,
    metrics: [MetricName.DISK_USAGE]
  };
  const customerUUID = localStorage.getItem('customerId');
  return axios
    .post<Metrics<'disk_usage'>>(`${ROOT_URL}/customers/${customerUUID}/metrics`, graphFilter)
    .then((response) => response.data);
}

export function fetchTaskProgress(taskUUID: string) {
  return axios.get(`${getCustomerEndpoint()}/tasks/${taskUUID}`);
}

const DEFAULT_TASK_REFETCH_INTERVAL = 1000;
type callbackFunc = (err: boolean, data: any) => void;

export function fetchTaskUntilItCompletes(
  taskUUID: string,
  callback: callbackFunc,
  onTaskStarted?: () => void,
  interval = DEFAULT_TASK_REFETCH_INTERVAL
) {
  let taskRunning = false;
  async function retryTask() {
    try {
      const resp = await fetchTaskProgress(taskUUID);
      const { percent, status } = resp.data;
      if (percent > 0 && taskRunning === false) {
        onTaskStarted && onTaskStarted();
        taskRunning = true;
      }
      if (status === 'Failed' || status === 'Failure') {
        callback(true, resp);
      } else if (percent === 100) {
        callback(false, resp.data);
      } else {
        setTimeout(retryTask, interval);
      }
      // eslint-disable-next-line no-empty
    } catch {}
  }
  return retryTask();
}

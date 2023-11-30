import moment from 'moment';

import {
  CUSTOM_METRIC_TIME_RANGE_OPTION,
  DROPDOWN_DIVIDER,
  MetricName,
  METRIC_TIME_RANGE_OPTIONS,
  XClusterTableEligibility,
  XCLUSTER_SUPPORTED_TABLE_TYPES
} from './constants';

import { YBTable } from '../../redesign/helpers/dtos';
import { XClusterTableDetails } from './dtos';

/**
 * XCluster supported table type.
 */
export type XClusterTableType = typeof XCLUSTER_SUPPORTED_TABLE_TYPES[number];

export type XClusterTable = YBTable & Omit<XClusterTableDetails, 'tableId'>;

//------------------------------------------------------------------------------------
// Table Selection Types

/**
 * This type stores details of a table's eligibility for xCluster replication.
 */
export type EligibilityDetails =
  | {
      status: typeof XClusterTableEligibility.ELIGIBLE_UNUSED;
    }
  | {
      status: typeof XClusterTableEligibility.ELIGIBLE_IN_CURRENT_CONFIG;
      xClusterConfigName: string;
    }
  | { status: typeof XClusterTableEligibility.INELIGIBLE_IN_USE; xClusterConfigName: string }
  | { status: typeof XClusterTableEligibility.INELIGIBLE_NO_MATCH };

/**
 * YBTable with an EligibilityDetail field
 */
export interface XClusterTableCandidate extends YBTable {
  eligibilityDetails: EligibilityDetails;
}

/**
 * Holds list of tables for a keyspace and provides extra metadata.
 */
export interface NamespaceItem {
  uuid: string;
  name: string;
  tableEligibilityCount: {
    ineligible: number;
    eligibleInCurrentConfig: number;
  };
  sizeBytes: number;
  tables: XClusterTableCandidate[];
}

/**
 * Structure for organizing tables by table type first and keyspace/database name second.
 */
export type ReplicationItems = Record<
  XClusterTableType,
  { namespaces: Record<string, NamespaceItem>; tableCount: number }
>;
//------------------------------------------------------------------------------------

// TODO: Move the metric types to dtos.ts or another more appropriate file.

export interface MetricTrace {
  instanceName?: string;
  name: string;
  type: string;
  x: number[];
  y: string[] | number[];
  mode?: string;
  line?: {
    dash: string;
    width: number;
  };
}

export type Metrics<MetricNameType extends MetricName> = {
  [metricName in MetricNameType]: {
    data: MetricTrace[];
    directURLs: string[];
    layout: {
      title: string;
      xaxis: {
        alias: { [x: string]: string };
        type: string;
      };
      yaxis: {
        alias: { [x: string]: string };
        ticksuffix: string;
      };
    };
    queryKey: string;
  };
};

// Time range selector types.

export type MetricTimeRangeOption = Exclude<
  typeof METRIC_TIME_RANGE_OPTIONS[number],
  typeof DROPDOWN_DIVIDER
>;

export type StandardMetricTimeRangeOption = Exclude<
  MetricTimeRangeOption,
  typeof CUSTOM_METRIC_TIME_RANGE_OPTION
>;

export interface MetricTimeRange {
  startMoment: moment.Moment;
  endMoment: moment.Moment;
}

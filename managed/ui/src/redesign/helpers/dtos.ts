import { YBTableRelationType } from './constants';
import { DeepPartial } from './types';

export interface PlacementAZ {
  uuid: string;
  name: string;
  replicationFactor: number;
  subnet: string;
  numNodesInAZ: number;
  isAffinitized: boolean;
}

export interface PlacementRegion {
  uuid: string;
  code: string;
  name: string;
  azList: PlacementAZ[];
}

export interface PlacementCloud {
  uuid: string;
  code: string;
  regionList: PlacementRegion[];
}

export enum CloudType {
  unknown = 'unknown',
  aws = 'aws',
  gcp = 'gcp',
  azu = 'azu',
  docker = 'docker',
  onprem = 'onprem',
  kubernetes = 'kubernetes',
  cloud = 'cloud-1',
  other = 'other'
}

// PublicCloudConstants.java
export enum StorageType {
  IO1 = 'IO1',
  GP2 = 'GP2',
  GP3 = 'GP3',
  Scratch = 'Scratch',
  Persistent = 'Persistent',
  StandardSSD_LRS = 'StandardSSD_LRS',
  Premium_LRS = 'Premium_LRS',
  UltraSSD_LRS = 'UltraSSD_LRS'
}

export interface DeviceInfo {
  volumeSize: number;
  numVolumes: number;
  diskIops: number | null;
  throughput: number | null;
  storageClass: 'standard'; // hardcoded in DeviceInfo.java
  mountPoints: string | null;
  storageType: StorageType | null;
}

// UniverseTaskParams.java
export interface CommunicationPorts {
  masterHttpPort: number;
  masterRpcPort: number;
  tserverHttpPort: number;
  tserverRpcPort: number;
  redisServerHttpPort: number;
  redisServerRpcPort: number;
  yqlServerHttpPort: number;
  yqlServerRpcPort: number;
  ysqlServerHttpPort: number;
  ysqlServerRpcPort: number;
  nodeExporterPort: number;
}

export type FlagsArray = { name: string; value: string }[];
export type FlagsObject = Record<string, string>;

// UniverseDefinitionTaskParams.java
export interface UserIntent {
  universeName: string;
  provider: string;
  providerType: CloudType;
  replicationFactor: number;
  regionList: string[];
  preferredRegion: string | null;
  instanceType: string;
  numNodes: number;
  ybSoftwareVersion: string | null;
  accessKeyCode: string | null;
  deviceInfo: DeviceInfo | null;
  assignPublicIP: boolean;
  useTimeSync: boolean;
  enableYSQL: boolean;
  enableYSQLAuth: boolean;
  enableYCQL: boolean;
  enableYCQLAuth: boolean;
  enableNodeToNodeEncrypt: boolean;
  enableClientToNodeEncrypt: boolean;
  enableVolumeEncryption: boolean;
  awsArnString: string;
  useHostname: boolean;
  // api returns tags as FlagsObject but when creating/editing the universe - it expects tags as FlagsArray
  masterGFlags: FlagsObject | FlagsArray;
  tserverGFlags: FlagsObject | FlagsArray;
  instanceTags: FlagsObject | FlagsArray;
}

export enum ClusterType {
  PRIMARY = 'PRIMARY',
  ASYNC = 'ASYNC'
}

export interface Cluster {
  placementInfo: {
    cloudList: PlacementCloud[];
  };
  clusterType: ClusterType;
  userIntent: UserIntent;
  uuid: string;
  index: number;
}

export interface Resources {
  azList: string[];
  ebsPricePerHour: number;
  memSizeGB: number;
  numCores: number;
  numNodes: number;
  pricePerHour: number;
  volumeCount: number;
  volumeSizeGB: number;
}

export interface UniverseConfig {
  disableAlertsUntilSecs: string;
  takeBackups: string;
}

export interface EncryptionAtRestConfig {
  readonly encryptionAtRestEnabled: boolean;
  readonly kmsConfigUUID: string | null; // KMS config Id in universe json
  readonly opType: 'ENABLE' | 'DISABLE' | 'UNDEFINED';
  configUUID?: string; // KMS config Id field for configure/create calls
  key_op?: 'ENABLE' | 'DISABLE' | 'UNDEFINED'; // operation field for configure/create calls
  type?: 'DATA_KEY' | 'CMK';
}

// NodeDetails.java
export enum NodeState {
  ToBeAdded = 'ToBeAdded',
  Provisioned = 'Provisioned',
  SoftwareInstalled = 'SoftwareInstalled',
  UpgradeSoftware = 'UpgradeSoftware',
  UpdateGFlags = 'UpdateGFlags',
  Live = 'Live',
  Stopping = 'Stopping',
  Starting = 'Starting',
  Stopped = 'Stopped',
  Unreachable = 'Unreachable',
  ToBeRemoved = 'ToBeRemoved',
  Removing = 'Removing',
  Removed = 'Removed',
  Adding = 'Adding',
  BeingDecommissioned = 'BeingDecommissioned',
  Decommissioned = 'Decommissioned'
}

// NodeDetails.java
export interface NodeDetails {
  nodeIdx: number;
  nodeName: string | null;
  nodeUuid: string | null;
  placementUuid: string;
  state: NodeState;
}

export interface UniverseDetails {
  currentClusterType?: ClusterType; // used in universe configure calls
  clusterOperation?: 'CREATE' | 'EDIT' | 'DELETE';
  allowInsecure: boolean;
  backupInProgress: boolean;
  capability: 'READ_ONLY' | 'EDITS_ALLOWED';
  clusters: Cluster[];
  communicationPorts: CommunicationPorts;
  cmkArn: string;
  deviceInfo: DeviceInfo | null;
  encryptionAtRestConfig: EncryptionAtRestConfig;
  extraDependencies: {
    installNodeExporter: boolean;
  };
  errorString: string | null;
  expectedUniverseVersion: number;
  importedState: 'NONE' | 'STARTED' | 'MASTERS_ADDED' | 'TSERVERS_ADDED' | 'IMPORTED';
  itestS3PackagePath: string;
  nextClusterIndex: number;
  nodeDetailsSet: NodeDetails[];
  nodePrefix: string;
  resetAZConfig: boolean;
  rootCA: string | null;
  xclusterInfo: {
    sourceRootCertDirPath: string;
    sourceXClusterConfigs: string[];
    targetXClusterConfigs: string[];
  };
  universeUUID: string;
  updateInProgress: boolean;
  updateSucceeded: boolean;
  userAZSelected: boolean;
}

export type UniverseConfigure = DeepPartial<UniverseDetails>;

export interface Universe {
  creationDate: string;
  name: string;
  resources: Resources;
  universeConfig: UniverseConfig;
  universeDetails: UniverseDetails;
  universeUUID: string;
  version: number;
}

export enum TableType {
  YQL_TABLE_TYPE = 'YQL_TABLE_TYPE',
  REDIS_TABLE_TYPE = 'REDIS_TABLE_TYPE',
  PGSQL_TABLE_TYPE = 'PGSQL_TABLE_TYPE'
}

export interface YBTable {
  isIndexTable: boolean;
  keySpace: string;
  pgSchemaName: string;
  relationType: YBTableRelationType;
  sizeBytes: number;
  tableName: string;
  tableType: TableType;
  tableUUID: string;
}

// Provider.java
export interface Provider {
  uuid: string;
  code: CloudType;
  name: string;
  active: boolean;
  customerUUID: string;
}

export interface AvailabilityZone {
  uuid: string;
  code: string;
  name: string;
  active: boolean;
  subnet: string;
}

// Region.java
export interface Region {
  uuid: string;
  code: string;
  name: string;
  ybImage: string;
  longitude: number;
  latitude: number;
  active: boolean;
  securityGroupId: string | null;
  details: string | null;
  zones: AvailabilityZone[];
}

// InstanceType.java
interface VolumeDetails {
  volumeSizeGB: number;
  volumeType: 'EBS' | 'SSD' | 'HDD' | 'NVME';
  mountPath: string;
}

interface InstanceTypeDetails {
  tenancy: 'Shared' | 'Dedicated' | 'Host' | null;
  volumeDetailsList: VolumeDetails[];
}
export interface InstanceType {
  active: boolean;
  providerCode: CloudType;
  instanceTypeCode: string;
  idKey: {
    providerCode: CloudType;
    instanceTypeCode: string;
  };
  numCores: number;
  memSizeGB: number;
  instanceTypeDetails: InstanceTypeDetails;
}

// AccessKey.java
export interface AccessKey {
  idKey: {
    keyCode: string;
    providerUUID: string;
  };
  keyInfo: {
    publicKey: string;
    privateKey: string;
    vaultPasswordFile: string;
    vaultFile: string;
    sshUser: string;
    sshPort: number;
    airGapInstall: boolean;
    passwordlessSudoAccess: boolean;
    provisionInstanceScript: string;
  };
}

// CertificateInfo.java
export interface Certificate {
  uuid: string;
  customerUUID: string;
  label: string;
  startDate: string;
  expiryDate: string;
  privateKey: string;
  certificate: string;
  certType: 'SelfSigned' | 'CustomCertHostPath';
}

// EncryptionAtRestController.java
export interface KmsConfig {
  credentials: {
    AWS_ACCESS_KEY_ID: string;
    AWS_REGION: string;
    AWS_SECRET_ACCESS_KEY: string;
    cmk_id: string;
  };
  metadata: {
    configUUID: string;
    in_use: boolean;
    name: string;
    provider: string;
  };
}

export interface HAPlatformInstance {
  uuid: string;
  config_uuid: string;
  address: string;
  is_leader: boolean;
  is_local: boolean;
  last_backup: string | null;
}

export interface HAConfig {
  uuid: string;
  cluster_key: string;
  last_failover: number;
  instances: HAPlatformInstance[];
}

export interface HAReplicationSchedule {
  frequency_milliseconds: number;
  is_running: boolean;
}

export const TableTypeLabel: Record<TableType, string> = {
  YQL_TABLE_TYPE: 'YCQL',
  PGSQL_TABLE_TYPE: 'YSQL',
  REDIS_TABLE_TYPE: 'REDIS'
} as const;

export interface MetricsData {
  type: string;
  metricsKey: string[];
  nodePrefixes: string;
  selectedUniverse: any;
  title: string;
  tableName?: string;
}

export interface GraphFilter {
  startMoment: any;
  endMoment: any;
  nodeName: string;
  nodePrefix: string;
  filterValue?: string;
  filterType?: string;
  currentSelectedRegion?: string;
  metricMeasure?: string;
  outlierType?: string;
  outlierNumNodes?: number;
  selectedRegionClusterUUID?: string | null;
  selectedRegionCode?: string | null;
  selectedZoneName?: string | null;
}

export interface MetricSettings {
  metric: string;
  splitTopNodes: number;
}

export interface MetricQueryParams {
  metricsWithSettings: MetricSettings[];
  start: string;
  end: string;
  nodePrefix: string;
  nodeNames: string[];
}
export interface CpuMeasureQueryData {
  maxNodeName: string;
  maxNodeValue: number;
  otherNodesAvgValue: number;
}
export interface CpuMeasureRecommendation {
  data: CpuMeasureQueryData;
  summary: React.ReactNode | string;
}

export interface CpuUsageRecommendation {
  summary: React.ReactNode | string;
}

export interface IndexSchemaQueryData {
  table_name: string;
  index_name: string;
  index_command: string;
}

export interface IndexSchemaRecommendation {
  data: IndexSchemaQueryData[];
  summary: React.ReactNode | string;
}

export interface NodeDistributionData {
  numSelect: number;
  numInsert: number;
  numUpdate: number;
  numDelete: number;
}

export interface QueryLoadData {
  maxNodeName: string;
  percentDiff: number;
  maxNodeDistribution: NodeDistributionData;
  otherNodesDistribution: NodeDistributionData;
}

export interface QueryLoadRecommendation {
  data: QueryLoadData;
  summary: React.ReactNode | string;
}

export enum RecommendationTypeEnum {
  All = 'All',
  SchemaSuggestion = 'SchemaSuggestion',
  QueryLoadSkew = 'QueryLoadSkew',
  IndexSuggestion = 'IndexSuggestion',
  ConnectionSkew = 'ConnectionSkew',
  CpuSkew = 'CpuSkew',
  CpuUsage = 'CpuUsage'
}

export interface RunTimeConfigData {
  configID: number;
  configKey: string;
  configValue: string;
  isConfigInherited: boolean;
  displayName: string;
  helpTxt: string;
  type: string;
  scope: string;
}

export enum RunTimeConfigScope {
  GLOBAL = 'GLOBAL',
  UNIVERSE = 'UNIVERSE',
  PROVIDER = 'PROVIDER',
  CUSTOMER = 'CUSTOMER'
}

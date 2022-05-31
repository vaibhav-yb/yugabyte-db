// Copyright (c) YugaByte, Inc.

import React, { Component } from 'react';
import { Row, Col } from 'react-bootstrap';
import { Field, FieldArray } from 'redux-form';
import { browserHistory } from 'react-router';
import _ from 'lodash';
import {
  isDefinedNotNull,
  isNonEmptyObject,
  isNonEmptyString,
  areIntentsEqual,
  isEmptyObject,
  isNonEmptyArray,
  trimSpecialChars,
  normalizeToValidPort
} from '../../../utils/ObjectUtils';
import {
  YBTextInput,
  YBTextInputWithLabel,
  YBSelectWithLabel,
  YBMultiSelectWithLabel,
  YBNumericInputWithLabel,
  YBRadioButtonBarWithLabel,
  YBToggle,
  YBUnControlledNumericInput,
  YBControlledNumericInputWithLabel,
  YBPassword
} from '../../../components/common/forms/fields';
import { getPromiseState } from '../../../utils/PromiseUtils';
import AZSelectorTable from './AZSelectorTable';
import './UniverseForm.scss';
import AZPlacementInfo from './AZPlacementInfo';
import GFlagArrayComponent from './GFlagArrayComponent';
import GFlagComponent from './GFlagComponent';
import {
  getPrimaryCluster,
  getReadOnlyCluster,
  getClusterByType,
  isKubernetesUniverse,
  getPlacementCloud
} from '../../../utils/UniverseUtils';
import pluralize from 'pluralize';
import { AZURE_INSTANCE_TYPE_GROUPS } from '../../../redesign/universe/wizard/fields/InstanceTypeField/InstanceTypeField';
import { isEphemeralAwsStorageInstance } from '../UniverseDetail/UniverseDetail';
import { fetchSupportedReleases } from '../../../actions/universe';

// Default instance types for each cloud provider
const DEFAULT_INSTANCE_TYPE_MAP = {
  aws: 'c5.large',
  gcp: 'n1-standard-1',
  kubernetes: 'small'
};

// Maps API storage types to UI display options
const API_UI_STORAGE_TYPES = {
  Scratch: 'Local Scratch',
  Persistent: 'Persistent',
  IO1: 'IO1',
  GP2: 'GP2',
  GP3: 'GP3',
  Premium_LRS: 'Premium',
  StandardSSD_LRS: 'Standard',
  UltraSSD_LRS: 'Ultra'
};

const DEFAULT_PORTS = {
  MASTER_HTTP_PORT: 7000,
  MASTER_RPC_PORT: 7100,
  TSERVER_HTTP_PORT: 9000,
  TSERVER_RPC_PORT: 9100,
  YEDIS_HTTP_PORT: 11000,
  YEDIS_RPC_PORT: 6379,
  YQL_HTTP_PORT: 12000,
  YQL_RPC_PORT: 9042,
  YSQL_HTTP_PORT: 13000,
  YSQL_RPC_PORT: 5433
};

const DEFAULT_STORAGE_TYPES = {
  AWS: 'GP3',
  GCP: 'Persistent',
  AZU: 'Premium_LRS'
};

const FORBIDDEN_GFLAG_KEYS = new Set(['NAME']);

export const EXPOSING_SERVICE_STATE_TYPES = {
  None: 'NONE',
  Exposed: 'EXPOSED',
  Unexposed: 'UNEXPOSED'
};

const IO1_DEFAULT_DISK_IOPS = 1000;
const IO1_MAX_DISK_IOPS = 64000;
const GP3_DEFAULT_DISK_IOPS = 3000;
const GP3_MAX_IOPS = 16000;
const GP3_DEFAULT_DISK_THROUGHPUT = 125;
const GP3_MAX_THROUGHPUT = 1000;
const GP3_IOPS_TO_MAX_DISK_THROUGHPUT = 4;

const UltraSSD_DEFAULT_DISK_IOPS = 3000;
const UltraSSD_DEFAULT_DISK_THROUGHPUT = 125;
const UltraSSD_MIN_DISK_IOPS = 100;
const UltraSSD_DISK_IOPS_MAX_PER_GB = 300;
const UltraSSD_IOPS_TO_MAX_DISK_THROUGHPUT = 4;
const UltraSSD_DISK_THROUGHPUT_CAP = 2500;

const initialState = {
  universeName: '',
  instanceTypeSelected: '',
  azCheckState: true,
  providerSelected: '',
  regionList: [],
  numNodes: 3,
  nodeSetViaAZList: false,
  isAZUpdating: false,
  replicationFactor: 3,
  deviceInfo: {},
  placementInfo: {},
  ybSoftwareVersion: '',
  gflags: {},
  storageType: DEFAULT_STORAGE_TYPES['AWS'],
  accessKeyCode: '',
  // Maximum Number of nodes currently in use OnPrem case
  maxNumNodes: -1,
  assignPublicIP: true,
  hasInstanceTypeChanged: false,
  useTimeSync: true,
  enableYSQL: true,
  enableYSQLAuth: true,
  ysqlPassword: '',
  enableYCQL: true,
  enableYCQLAuth: true,
  ycqlPassword: '',
  enableIPV6: false,
  // By default, we don't want to expose the service.
  enableExposingService: EXPOSING_SERVICE_STATE_TYPES['Unexposed'],
  enableYEDIS: false,
  enableNodeToNodeEncrypt: true,
  enableClientToNodeEncrypt: true,
  enableEncryptionAtRest: false,
  useSystemd: false,
  customizePorts: false,
  // Geo-partitioning settings.
  defaultRegion: '',
  supportedReleases: []
};

const portValidation = (value) => (value && value < 65536 ? undefined : 'Invalid Port');

function getMinDiskIops(storageType, volumeSize) {
  return storageType === 'UltraSSD_LRS' ? Math.max(UltraSSD_MIN_DISK_IOPS, volumeSize) : 0;
}

function getMaxDiskIops(storageType, volumeSize) {
  switch (storageType) {
    case 'IO1':
      return IO1_MAX_DISK_IOPS;
    case 'UltraSSD_LRS':
      return volumeSize * UltraSSD_DISK_IOPS_MAX_PER_GB;
    default:
      return GP3_MAX_IOPS;
  }
}

export default class ClusterFields extends Component {
  constructor(props) {
    super(props);
    this.providerChanged = this.providerChanged.bind(this);
    this.numNodesChanged = this.numNodesChanged.bind(this);
    this.defaultRegionChanged = this.defaultRegionChanged.bind(this);
    this.instanceTypeChanged = this.instanceTypeChanged.bind(this);
    this.regionListChanged = this.regionListChanged.bind(this);
    this.getCurrentProvider = this.getCurrentProvider.bind(this);
    this.configureUniverseNodeList = this.configureUniverseNodeList.bind(this);
    this.handleUniverseConfigure = this.handleUniverseConfigure.bind(this);
    this.storageTypeChanged = this.storageTypeChanged.bind(this);
    this.numVolumesChanged = this.numVolumesChanged.bind(this);
    this.volumeSizeChanged = this.volumeSizeChanged.bind(this);
    this.diskIopsChanged = this.diskIopsChanged.bind(this);
    this.throughputChanged = this.throughputChanged.bind(this);
    this.setDeviceInfo = this.setDeviceInfo.bind(this);
    this.toggleAssignPublicIP = this.toggleAssignPublicIP.bind(this);
    this.toggleUseTimeSync = this.toggleUseTimeSync.bind(this);
    this.toggleEnableYSQL = this.toggleEnableYSQL.bind(this);
    this.toggleEnableYSQLAuth = this.toggleEnableYSQLAuth.bind(this);
    this.toggleEnableYCQL = this.toggleEnableYCQL.bind(this);
    this.toggleEnableYCQLAuth = this.toggleEnableYCQLAuth.bind(this);
    this.toggleEnableIPV6 = this.toggleEnableIPV6.bind(this);
    this.toggleEnableExposingService = this.toggleEnableExposingService.bind(this);
    this.toggleEnableYEDIS = this.toggleEnableYEDIS.bind(this);
    this.toggleEnableNodeToNodeEncrypt = this.toggleEnableNodeToNodeEncrypt.bind(this);
    this.toggleEnableClientToNodeEncrypt = this.toggleEnableClientToNodeEncrypt.bind(this);
    this.toggleEnableEncryptionAtRest = this.toggleEnableEncryptionAtRest.bind(this);
    this.handleAwsArnChange = this.handleAwsArnChange.bind(this);
    this.handleSelectAuthConfig = this.handleSelectAuthConfig.bind(this);
    this.numNodesChangedViaAzList = this.numNodesChangedViaAzList.bind(this);
    this.replicationFactorChanged = this.replicationFactorChanged.bind(this);
    this.softwareVersionChanged = this.softwareVersionChanged.bind(this);
    this.accessKeyChanged = this.accessKeyChanged.bind(this);
    this.hasFieldChanged = this.hasFieldChanged.bind(this);
    this.toggleCustomizePorts = this.toggleCustomizePorts.bind(this);
    this.toggleUseSystemd = this.toggleUseSystemd.bind(this);
    this.validateUserTags = this.validateUserTags.bind(this);
    this.validatePassword = this.validatePassword.bind(this);
    this.validateConfirmPassword = this.validateConfirmPassword.bind(this);
    this.tlsCertChanged = this.tlsCertChanged.bind(this);

    this.currentInstanceType = _.get(
      this.props.universe,
      'currentUniverse.data.universeDetails.clusters[0].userIntent.instanceType'
    );

    if (this.props.type === 'Async' && isNonEmptyObject(this.props.universe.currentUniverse.data)) {
      if (
        isDefinedNotNull(
          getReadOnlyCluster(this.props.universe.currentUniverse.data.universeDetails.clusters)
        )
      ) {
        this.state = {
          ...initialState,
          isReadOnlyExists: true,
          editNotAllowed: this.props.editNotAllowed,
          awsInstanceWithEphemeralStorage: false,
          gcpInstanceWithEphemeralStorage: false
        };
      } else {
        const {
          universe: {
            currentUniverse: {
              data: { universeDetails }
            }
          }
        } = this.props;

        const { userIntent } = getPrimaryCluster(universeDetails.clusters);

        this.state = {
          ...initialState,
          enableYSQL: userIntent.enableYSQL,
          enableYSQLAuth: userIntent.enableYSQLAuth,
          enableYCQL: userIntent.enableYCQL,
          enableYCQLAuth: userIntent.enableYCQLAuth,
          isReadOnlyExists: false,
          editNotAllowed: false,
          useSystemd: userIntent.useSystemd,
          enableEncryptionAtRest:
            universeDetails.encryptionAtRestConfig &&
            universeDetails.encryptionAtRestConfig.encryptionAtRestEnabled
        };
      }
    } else {
      let tempState = this.props.formValues[this.props.clusterType];
      if (tempState) {
        tempState = {
          ...initialState,
          enableYSQL: tempState.enableYSQL,
          enableYSQLAuth: tempState.enableYSQLAuth,
          enableYCQL: tempState.enableYCQL,
          enableYCQLAuth: tempState.enableYCQLAuth,
          isReadOnlyExists:
            tempState.provider &&
            this.props.type === 'Create' &&
            this.props.clusterType === 'async',
          useSystemd: tempState.useSystemd,
          enableEncryptionAtRest: tempState.enableEncryptionAtRest
        };
      }
      this.state = tempState ? tempState : initialState;
    }
  }

  portsCustomized = (communicationPorts) => {
    return (
      communicationPorts.masterHttpPort !== DEFAULT_PORTS.MASTER_HTTP_PORT ||
      communicationPorts.masterRpcPort !== DEFAULT_PORTS.MASTER_RPC_PORT ||
      communicationPorts.tserverHttpPort !== DEFAULT_PORTS.TSERVER_HTTP_PORT ||
      communicationPorts.tserverRpcPort !== DEFAULT_PORTS.TSERVER_RPC_PORT ||
      communicationPorts.redisServerHttpPort !== DEFAULT_PORTS.YEDIS_HTTP_PORT ||
      communicationPorts.redisServerRpcPort !== DEFAULT_PORTS.YEDIS_RPC_PORT ||
      communicationPorts.yqlServerHttpPort !== DEFAULT_PORTS.YQL_HTTP_PORT ||
      communicationPorts.yqlServerRpcPort !== DEFAULT_PORTS.YQL_RPC_PORT ||
      communicationPorts.ysqlServerHttpPort !== DEFAULT_PORTS.YSQL_HTTP_PORT ||
      communicationPorts.ysqlServerRpcPort !== DEFAULT_PORTS.YSQL_RPC_PORT
    );
  };

  UNSAFE_componentWillMount() {
    const { formValues, clusterType, updateFormField, type } = this.props;
    const {
      universe: {
        currentUniverse: {
          data: { universeDetails }
        }
      }
    } = this.props;
    // Set default software version in case of create
    if (
      isNonEmptyArray(this.props.softwareVersions) &&
      !isNonEmptyString(this.state.ybSoftwareVersion)
    ) {
      let currentSoftwareVersion = this.props.softwareVersions[0];
      if (type === 'Create') {
        // Use primary cluster software version even for read replica
        if (formValues.primary?.ybSoftwareVersion) {
          currentSoftwareVersion = formValues.primary.ybSoftwareVersion;
        }
      } else {
        // when adding read replica post universe creation
        currentSoftwareVersion = getPrimaryCluster(universeDetails.clusters).userIntent
          .ybSoftwareVersion;
      }
      this.setState({ ybSoftwareVersion: currentSoftwareVersion });
      updateFormField(`${clusterType}.ybSoftwareVersion`, currentSoftwareVersion);
    }

    if (type === 'Create') {
      updateFormField('primary.masterHttpPort', DEFAULT_PORTS.MASTER_HTTP_PORT);
      updateFormField('primary.masterRpcPort', DEFAULT_PORTS.MASTER_RPC_PORT);
      updateFormField('primary.tserverHttpPort', DEFAULT_PORTS.TSERVER_HTTP_PORT);
      updateFormField('primary.tserverRpcPort', DEFAULT_PORTS.TSERVER_RPC_PORT);
      updateFormField('primary.redisHttpPort', DEFAULT_PORTS.YEDIS_HTTP_PORT);
      updateFormField('primary.redisRpcPort', DEFAULT_PORTS.YEDIS_RPC_PORT);
      updateFormField('primary.yqlHttpPort', DEFAULT_PORTS.YQL_HTTP_PORT);
      updateFormField('primary.yqlRpcPort', DEFAULT_PORTS.YQL_RPC_PORT);
      updateFormField('primary.ysqlHttpPort', DEFAULT_PORTS.YSQL_HTTP_PORT);
      updateFormField('primary.ysqlRpcPort', DEFAULT_PORTS.YSQL_RPC_PORT);
    } else if (type === 'Edit') {
      const { communicationPorts } = universeDetails;
      const customPorts = this.portsCustomized(communicationPorts);
      updateFormField('primary.customizePorts', customPorts);
      this.setState({ customizePorts: customPorts });
      updateFormField('primary.masterHttpPort', communicationPorts.masterHttpPort);
      updateFormField('primary.masterRpcPort', communicationPorts.masterRpcPort);
      updateFormField('primary.tserverHttpPort', communicationPorts.tserverHttpPort);
      updateFormField('primary.tserverRpcPort', communicationPorts.tserverRpcPort);
      updateFormField('primary.redisHttpPort', communicationPorts.redisServerHttpPort);
      updateFormField('primary.redisRpcPort', communicationPorts.redisServerRpcPort);
      updateFormField('primary.yqlHttpPort', communicationPorts.yqlServerHttpPort);
      updateFormField('primary.yqlRpcPort', communicationPorts.yqlServerRpcPort);
      updateFormField('primary.ysqlHttpPort', communicationPorts.ysqlServerHttpPort);
      updateFormField('primary.ysqlRpcPort', communicationPorts.ysqlServerRpcPort);
    }

    if (isNonEmptyObject(formValues['primary']) && clusterType !== 'primary') {
      this.setState({ universeName: formValues['primary'].universeName });
      updateFormField(`${clusterType}.universeName`, formValues['primary'].universeName);
    }

    // This flag will prevent configure from being fired on component load
    if (formValues && isNonEmptyObject(formValues[clusterType])) {
      this.setState({ nodeSetViaAZList: true });
    }

    const isEditReadOnlyFlow = type === 'Async';
    if (type === 'Edit' || isEditReadOnlyFlow) {
      const primaryCluster = getPrimaryCluster(universeDetails.clusters);
      const readOnlyCluster = getReadOnlyCluster(universeDetails.clusters);
      const userIntent =
        clusterType === 'async'
          ? readOnlyCluster
            ? readOnlyCluster && {
                ...readOnlyCluster.userIntent,
                universeName: primaryCluster.userIntent.universeName
              }
            : primaryCluster && {
                ...primaryCluster.userIntent,
                universeName: primaryCluster.userIntent.universeName
              }
          : primaryCluster && primaryCluster.userIntent;
      const providerUUID = userIntent && userIntent.provider;
      const encryptionAtRestEnabled =
        universeDetails.encryptionAtRestConfig &&
        universeDetails.encryptionAtRestConfig.encryptionAtRestEnabled;

      if (clusterType === 'async' && primaryCluster) {
        // setting form fields when adding a Read replica
        this.updateFormFields({
          'async.assignPublicIP': userIntent.assignPublicIP,
          'async.useTimeSync': userIntent.useTimeSync,
          'async.enableYSQL': userIntent.enableYSQL,
          'async.enableYSQLAuth': userIntent.enableYSQLAuth,
          'async.enableYCQL': userIntent.enableYCQL,
          'async.enableYCQLAuth': userIntent.enableYCQLAuth,
          'async.enableYEDIS': userIntent.enableYEDIS,
          'async.enableNodeToNodeEncrypt': userIntent.enableNodeToNodeEncrypt,
          'async.enableClientToNodeEncrypt': userIntent.enableClientToNodeEncrypt,
          'async.enableEncryptionAtRest': encryptionAtRestEnabled,
          'async.useSystemd': userIntent.useSystemd,
          'async.tlsCertificateId': universeDetails.rootCA
        });
      }
      if (userIntent && providerUUID) {
        const storageType =
          userIntent.deviceInfo === null ? null : userIntent.deviceInfo.storageType;
        this.setState({
          isKubernetesUniverse: isKubernetesUniverse(this.props.universe.currentUniverse.data),
          providerSelected: providerUUID,
          instanceTypeSelected: userIntent.instanceType,
          numNodes: userIntent.numNodes,
          replicationFactor: userIntent.replicationFactor,
          ybSoftwareVersion: userIntent.ybSoftwareVersion,
          assignPublicIP: userIntent.assignPublicIP,
          useTimeSync: userIntent.useTimeSync,
          enableYSQL: userIntent.enableYSQL,
          enableYSQLAuth: userIntent.enableYSQLAuth,
          enableYCQL: userIntent.enableYCQL,
          enableYCQLAuth: userIntent.enableYCQLAuth,
          enableIPV6: userIntent.enableIPV6,
          enableExposingService: userIntent.enableExposingService,
          enableYEDIS: userIntent.enableYEDIS,
          enableNodeToNodeEncrypt: userIntent.enableNodeToNodeEncrypt,
          enableClientToNodeEncrypt: userIntent.enableClientToNodeEncrypt,
          enableEncryptionAtRest:
            universeDetails.encryptionAtRestConfig &&
            universeDetails.encryptionAtRestConfig.encryptionAtRestEnabled,
          accessKeyCode: userIntent.accessKeyCode,
          deviceInfo: userIntent.deviceInfo,
          storageType: storageType,
          regionList: userIntent.regionList,
          volumeType: storageType === null ? 'SSD' : 'EBS', //TODO(wesley): fixme - establish volumetype/storagetype relationship
          useSystemd: userIntent.useSystemd
        });
      }

      this.props.getRegionListItems(providerUUID);
      if (
        clusterType === 'primary' &&
        isNonEmptyObject(primaryCluster?.placementInfo?.cloudList[0])
      ) {
        const value = primaryCluster.placementInfo.cloudList[0].defaultRegion;
        this.setState({ defaultRegion: value });
        updateFormField(`primary.defaultRegion`, value);
      }

      if (primaryCluster.userIntent.providerType === 'onprem') {
        this.props.fetchNodeInstanceList(providerUUID);
      }
      // If Edit Case Set Initial Configuration
      this.props.getExistingUniverseConfiguration(_.cloneDeep(universeDetails));
    } else {
      // Repopulate the form fields when switching back to the view
      if (formValues && isNonEmptyObject(formValues[clusterType])) {
        this.setState({
          providerType: formValues[clusterType].providerType,
          providerSelected: formValues[clusterType].provider,
          numNodes: formValues[clusterType].numNodes ? formValues[clusterType].numNodes : 3,
          replicationFactor: formValues[clusterType].replicationFactor
            ? Number(formValues[clusterType].replicationFactor)
            : 3
        });
        if (isNonEmptyString(formValues[clusterType].provider)) {
          this.props.getRegionListItems(formValues[clusterType].provider);
          this.setState({ instanceTypeSelected: formValues[clusterType].instanceType });

          if (formValues[clusterType].assignPublicIP) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ assignPublicIP: formValues['primary'].assignPublicIP });
          }
          if (formValues[clusterType].useTimeSync) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ useTimeSync: formValues['primary'].useTimeSync });
          }
          if (formValues[clusterType].enableYSQL) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ enableYSQL: formValues['primary'].enableYSQL });
          }
          if (formValues[clusterType].enableYSQLAuth) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ enableYSQLAuth: formValues['primary'].enableYSQLAuth });
          }
          if (formValues[clusterType].enableYCQL) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ enableYCQL: formValues['primary'].enableYCQL });
          }
          if (formValues[clusterType].enableYCQLAuth) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ enableYCQLAuth: formValues['primary'].enableYCQLAuth });
          }
          if (formValues[clusterType].enableIPV6) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ enableIPV6: formValues['primary'].enableIPV6 });
          }
          if (formValues[clusterType].enableExposingService) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ enableExposingService: formValues['primary'].enableExposingService });
          }
          if (formValues[clusterType].enableYEDIS) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({ enableYEDIS: formValues['primary'].enableYEDIS });
          }
          if (formValues[clusterType].enableNodeToNodeEncrypt) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({
              enableNodeToNodeEncrypt: formValues['primary'].enableNodeToNodeEncrypt
            });
          }
          if (formValues[clusterType].enableClientToNodeEncrypt) {
            // We would also default to whatever primary cluster's state for this one.
            this.setState({
              enableClientToNodeEncrypt: formValues['primary'].enableClientToNodeEncrypt
            });
          }
          if (formValues[clusterType].useSystemd) {
            this.setState({ useSystemd: formValues['primary'].useSystemd });
          }
          if (formValues[clusterType].enableEncryptionAtRest) {
            this.setState({ enableEncryptionAtRest: formValues['primary'].enableEncryptionAtRest });
          }
        }
      } else {
        // Initialize the form values if not exists
        updateFormField(`${clusterType}.numNodes`, 3);
        updateFormField(`${clusterType}.replicationFactor`, 3);
      }
    }
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    const {
      universe: { currentUniverse },
      cloud: { nodeInstanceList, instanceTypes },
      clusterType,
      formValues
    } = nextProps;

    const currentFormValues = formValues[clusterType];
    let providerSelected = this.state.providerSelected;
    if (isNonEmptyObject(currentFormValues) && isNonEmptyString(currentFormValues.provider)) {
      providerSelected = currentFormValues.provider;
    }

    if (
      nextProps.cloud.instanceTypes.data !== this.props.cloud.instanceTypes.data &&
      isNonEmptyArray(nextProps.cloud.instanceTypes.data) &&
      providerSelected
    ) {
      if (
        nextProps.type === 'Create' ||
        (nextProps.type === 'Async' && !this.state.isReadOnlyExists)
      ) {
        let instanceTypeSelected = null;
        const currentProviderCode = this.getCurrentProvider(providerSelected).code;
        instanceTypeSelected = DEFAULT_INSTANCE_TYPE_MAP[currentProviderCode];
        // If we have the default instance type in the cloud instance types then we
        // use it, otherwise we pick the first one in the list and use it.
        const hasInstanceType = instanceTypes.data.find((it) => {
          return (
            it.providerCode === currentProviderCode && it.instanceTypeCode === instanceTypeSelected
          );
        });
        if (!hasInstanceType) {
          instanceTypeSelected = instanceTypes.data[0].instanceTypeCode;
        }

        const instanceTypeSelectedData = instanceTypes.data.find(function (item) {
          return item.instanceTypeCode === formValues[clusterType].instanceType;
        });

        if (isNonEmptyObject(instanceTypeSelectedData)) {
          instanceTypeSelected = formValues[clusterType].instanceType;
        }

        this.props.updateFormField(`${clusterType}.instanceType`, instanceTypeSelected);
        this.setState({ instanceTypeSelected: instanceTypeSelected });
        this.setDeviceInfo(instanceTypeSelected, instanceTypes.data);
      }
    }

    const currentProvider = this.getCurrentProvider(providerSelected);
    // Set default storageType once API call has completed, defaults to AWS provider if current provider is not GCP
    if (
      typeof currentProvider !== 'undefined' &&
      currentProvider.code === 'gcp' &&
      isNonEmptyArray(nextProps.cloud.gcpTypes.data) &&
      !isNonEmptyArray(this.props.cloud.gcpTypes.data)
    ) {
      this.props.updateFormField(`${clusterType}.storageType`, DEFAULT_STORAGE_TYPES['GCP']);
      this.setState({ storageType: DEFAULT_STORAGE_TYPES['GCP'] });
    } else if (
      isNonEmptyArray(nextProps.cloud.ebsTypes) &&
      !isNonEmptyArray(this.props.cloud.ebsTypes)
    ) {
      this.props.updateFormField(`${clusterType}.storageType`, DEFAULT_STORAGE_TYPES['AWS']);
      this.setState({ storageType: DEFAULT_STORAGE_TYPES['AWS'] });
    }

    // Form Actions on Create Universe Success
    if (
      getPromiseState(this.props.universe.createUniverse).isLoading() &&
      getPromiseState(nextProps.universe.createUniverse).isSuccess()
    ) {
      this.props.reset();
      this.props.fetchUniverseMetadata();
      this.props.fetchCustomerTasks();
      if (this.context.prevPath) {
        browserHistory.push(this.context.prevPath);
      } else {
        browserHistory.push('/universes');
      }
    }
    // Form Actions on Edit Universe Success
    if (
      (getPromiseState(this.props.universe.editUniverse).isLoading() &&
        getPromiseState(nextProps.universe.editUniverse).isSuccess()) ||
      (getPromiseState(this.props.universe.addReadReplica).isLoading() &&
        getPromiseState(nextProps.universe.addReadReplica).isSuccess()) ||
      (getPromiseState(this.props.universe.editReadReplica).isLoading() &&
        getPromiseState(nextProps.universe.editReadReplica).isSuccess()) ||
      (getPromiseState(this.props.universe.deleteReadReplica).isLoading() &&
        getPromiseState(nextProps.universe.deleteReadReplica).isSuccess())
    ) {
      this.props.fetchCurrentUniverse(currentUniverse.data.universeUUID);
      this.props.fetchUniverseMetadata();
      this.props.fetchCustomerTasks();
      this.props.fetchUniverseTasks(currentUniverse.data.universeUUID);
      browserHistory.push(this.props.location.pathname);
    }
    // Form Actions on Configure Universe Success
    if (
      getPromiseState(this.props.universe.universeConfigTemplate).isLoading() &&
      getPromiseState(nextProps.universe.universeConfigTemplate).isSuccess()
    ) {
      this.props.fetchUniverseResources(nextProps.universe.universeConfigTemplate.data);
    }
    // If nodeInstanceList changes, fetch number of available nodes
    if (
      getPromiseState(nodeInstanceList).isSuccess() &&
      getPromiseState(this.props.cloud.nodeInstanceList).isLoading()
    ) {
      let numNodesAvailable = nodeInstanceList.data.reduce((acc, val) => {
        if (!val.inUse) {
          acc++;
        }
        return acc;
      }, 0);
      // Add Existing nodes in Universe userIntent to available nodes for calculation in case of Edit
      if (
        this.props.type === 'Edit' ||
        (nextProps.type === 'Async' && !this.state.isReadOnlyExists)
      ) {
        const cluster = getClusterByType(
          currentUniverse.data.universeDetails.clusters,
          clusterType
        );
        if (isDefinedNotNull(cluster)) {
          numNodesAvailable += cluster.userIntent.numNodes;
        }
      }
      this.setState({ maxNumNodes: numNodesAvailable });
    }
  }

  componentDidMount() {
    const {
      cloud,
      clusterType,
      updateFormField,
      type,
      formValues,
      universe: { currentUniverse }
    } = this.props;
    if (!formValues[clusterType] && isNonEmptyArray(cloud.providers?.data)) {
      // AC: Editing Read-Replica is type 'Async'. We should change this at some point
      if (type === 'Edit' || type === 'Async') {
        let currentCluster =
          type === 'Edit'
            ? getPrimaryCluster(currentUniverse.data.universeDetails.clusters)
            : getReadOnlyCluster(currentUniverse.data.universeDetails.clusters);
        if (!currentCluster)
          //init primary cluster as current cluster (creation of first read replica) -
          currentCluster = getPrimaryCluster(currentUniverse.data.universeDetails.clusters);

        const currentProviderUuid = currentCluster.userIntent.provider;
        updateFormField(`${clusterType}.provider`, currentProviderUuid);
        if (type === 'Async') this.providerChanged(currentProviderUuid);
      } else {
        const firstProviderUuid = cloud.providers.data[0]?.uuid;
        updateFormField(`${clusterType}.provider`, firstProviderUuid);
        this.providerChanged(firstProviderUuid);
      }
    } else if (type === 'Create' && clusterType === 'async' && formValues['primary']?.provider) {
      const providerUUID = formValues['primary'].provider;
      updateFormField(`${clusterType}.provider`, providerUUID);
      this.providerChanged(providerUUID);
    }
    this.props.fetchRunTimeConfigs();
  }

  componentDidUpdate(prevProps, prevState) {
    const {
      universe: { currentUniverse, universeConfigTemplate },
      formValues,
      clusterType,
      setPlacementStatus,
      toggleDisableSubmit,
      type
    } = this.props;
    let currentProviderUUID = this.state.providerSelected;

    if (
      isNonEmptyObject(formValues[clusterType]) &&
      isNonEmptyString(formValues[clusterType].provider)
    ) {
      currentProviderUUID = formValues[clusterType].provider;
    }
    const currentProvider = this.getCurrentProvider(currentProviderUUID);

    const configureIntentValid = () => {
      return (
        !_.isEqual(this.state, prevState) &&
        isNonEmptyObject(currentProvider) &&
        isNonEmptyArray(formValues[clusterType].regionList) &&
        (prevState.maxNumNodes !== -1 || currentProvider.code !== 'onprem') &&
        ((currentProvider.code === 'onprem' && this.state.numNodes <= this.state.maxNumNodes) ||
          currentProvider.code !== 'onprem') &&
        this.state.numNodes >= this.state.replicationFactor &&
        !this.state.nodeSetViaAZList
      );
    };

    // Fire Configure only if either provider is not on-prem or maxNumNodes is not -1 if on-prem
    if (configureIntentValid()) {
      toggleDisableSubmit(false);
      if (isNonEmptyObject(currentUniverse.data)) {
        if (!this.hasFieldChanged()) {
          const placementStatusObject = {
            error: {
              type: 'noFieldsChanged',
              numNodes: this.state.numNodes,
              maxNumNodes: this.state.maxNumNodes
            }
          };
          setPlacementStatus(placementStatusObject);
        }
      }
      this.configureUniverseNodeList(!_.isEqual(this.state.regionList, prevState.regionList));
    } else if (currentProvider && currentProvider.code === 'onprem') {
      toggleDisableSubmit(false);
      if (
        isNonEmptyArray(this.state.regionList) &&
        currentProvider &&
        this.state.instanceTypeSelected &&
        this.state.numNodes > this.state.maxNumNodes
      ) {
        const placementStatusObject = {
          error: {
            type: 'notEnoughNodesConfigured',
            numNodes: this.state.numNodes,
            maxNumNodes: this.state.maxNumNodes
          }
        };
        setPlacementStatus(placementStatusObject);
        toggleDisableSubmit(true);
      } else if (isNonEmptyObject(currentUniverse.data)) {
        const primaryCluster = currentUniverse.data.universeDetails.clusters.find(
          (x) => x.clusterType === 'PRIMARY'
        );
        const provider = primaryCluster.placementInfo.cloudList.find(
          (c) => c.uuid === currentProvider.uuid
        );
        const replication = primaryCluster.userIntent.replicationFactor;
        if (provider) {
          const numAzs = provider.regionList.reduce(
            (acc, current) => acc + current.azList.length,
            0
          );
          setPlacementStatus({
            replicationFactor: replication,
            numUniqueAzs: numAzs,
            numUniqueRegions: provider.regionList.length
          });
        }
      }
    }
    //hook from parent universeForm to check if any fields was changed
    const nodeDetailsSet =
      getPromiseState(currentUniverse).isSuccess() &&
      getPromiseState(universeConfigTemplate).isSuccess()
        ? universeConfigTemplate.data.nodeDetailsSet
        : [];
    if (type === 'Edit' || (this.props.type === 'Async' && this.state.isReadOnlyExists)) {
      this.props.handleHasFieldChanged(
        this.hasFieldChanged() ||
          !_.isEqual(currentUniverse.data.universeDetails.nodeDetailsSet, nodeDetailsSet)
      );
    } else {
      this.props.handleHasFieldChanged(true);
    }

    this.doAuthAndVolumeSizeValidation();
  }

  numNodesChangedViaAzList(value) {
    const { updateFormField, clusterType } = this.props;
    this.setState({ nodeSetViaAZList: true, numNodes: value });
    updateFormField(`${clusterType}.numNodes`, value);
  }

  setDeviceInfo(instanceTypeCode, instanceTypeList) {
    const { updateFormField, clusterType } = this.props;
    const instanceTypeSelectedData = instanceTypeList.find(function (item) {
      return item.instanceTypeCode === instanceTypeCode;
    });
    const volumesList = instanceTypeSelectedData.instanceTypeDetails.volumeDetailsList;
    const volumeDetail = volumesList[0];
    let mountPoints = null;
    if (instanceTypeSelectedData.providerCode === 'onprem') {
      mountPoints = instanceTypeSelectedData.instanceTypeDetails.volumeDetailsList
        .map(function (item) {
          return item.mountPath;
        })
        .join(',');
    }
    if (volumeDetail) {
      let storageType = this.state.deviceInfo.storageType
        ? this.state.deviceInfo.storageType
        : DEFAULT_STORAGE_TYPES[instanceTypeSelectedData.providerCode.toUpperCase()];
      if (
        instanceTypeSelectedData.providerCode === 'aws' &&
        isEphemeralAwsStorageInstance(instanceTypeCode)
      ) {
        storageType = null;
      }

      const deviceInfo = {
        volumeSize: volumeDetail.volumeSizeGB,
        numVolumes: volumesList.length,
        mountPoints: mountPoints,
        storageType: storageType,
        storageClass: 'standard',
        diskIops: null,
        throughput: null
      };
      updateFormField(`${clusterType}.volumeSize`, volumeDetail.volumeSizeGB);
      updateFormField(`${clusterType}.numVolumes`, volumesList.length);
      updateFormField(`${clusterType}.diskIops`, volumeDetail.diskIops);
      updateFormField(`${clusterType}.throughput`, volumeDetail.throughput);
      updateFormField(`${clusterType}.storageType`, deviceInfo.storageType);
      updateFormField(`${clusterType}.storageClass`, deviceInfo.storageClass);
      updateFormField(`${clusterType}.mountPoints`, mountPoints);
      this.initThroughputAndIops(deviceInfo);
      this.setState({ deviceInfo: deviceInfo, volumeType: volumeDetail.volumeType });
    }
  }

  softwareVersionChanged(value) {
    const { updateFormField, clusterType } = this.props;
    this.setState({ ybSoftwareVersion: value });
    updateFormField(`${clusterType}.ybSoftwareVersion`, value);
  }

  storageTypeChanged(storageValue) {
    const { updateFormField, clusterType } = this.props;
    const currentDeviceInfo = _.clone(this.state.deviceInfo);
    currentDeviceInfo.storageType = storageValue;
    this.initThroughputAndIops(currentDeviceInfo);
    if (storageValue === 'Scratch') {
      this.setState({ gcpInstanceWithEphemeralStorage: true });
    }
    updateFormField(`${clusterType}.storageType`, storageValue);
    this.setState({ deviceInfo: currentDeviceInfo });
  }

  initThroughputAndIops(currentDeviceInfo) {
    const { updateFormField, clusterType } = this.props;
    if (currentDeviceInfo.storageType === 'IO1') {
      currentDeviceInfo.diskIops = IO1_DEFAULT_DISK_IOPS;
      currentDeviceInfo.throughput = null;
      updateFormField(`${clusterType}.diskIops`, IO1_DEFAULT_DISK_IOPS);
    } else if (currentDeviceInfo.storageType === 'GP3') {
      currentDeviceInfo.diskIops = GP3_DEFAULT_DISK_IOPS;
      currentDeviceInfo.throughput = GP3_DEFAULT_DISK_THROUGHPUT;
      updateFormField(`${clusterType}.diskIops`, GP3_DEFAULT_DISK_IOPS);
      updateFormField(`${clusterType}.throughput`, GP3_DEFAULT_DISK_THROUGHPUT);
    } else if (currentDeviceInfo.storageType === 'UltraSSD_LRS') {
      currentDeviceInfo.diskIops = UltraSSD_DEFAULT_DISK_IOPS;
      currentDeviceInfo.throughput = UltraSSD_DEFAULT_DISK_THROUGHPUT;
      updateFormField(`${clusterType}.diskIops`, currentDeviceInfo.diskIops);
      updateFormField(`${clusterType}.throughput`, currentDeviceInfo.throughput);
    } else {
      currentDeviceInfo.diskIops = null;
      currentDeviceInfo.throughput = null;
    }
  }

  numVolumesChanged(val) {
    const { updateFormField, clusterType } = this.props;
    updateFormField(`${clusterType}.numVolumes`, val);
    this.setState({ deviceInfo: { ...this.state.deviceInfo, numVolumes: val } });
  }

  checkVolumeSizeRestrictions() {
    const {
      validateVolumeSizeUnchanged,
      clusterType,
      universe: { currentUniverse }
    } = this.props;
    const { hasInstanceTypeChanged, deviceInfo } = this.state;

    if (
      !validateVolumeSizeUnchanged ||
      hasInstanceTypeChanged ||
      isEmptyObject(currentUniverse.data) ||
      isEmptyObject(currentUniverse.data.universeDetails)
    ) {
      return true;
    }
    const curCluster = getClusterByType(currentUniverse.data.universeDetails.clusters, clusterType);
    if (
      !isEmptyObject(curCluster) &&
      Number(curCluster.userIntent.deviceInfo.volumeSize) !== Number(deviceInfo.volumeSize)
    ) {
      return false;
    }
    return true;
  }

  volumeSizeChanged(val) {
    const { updateFormField, clusterType } = this.props;
    const {
      deviceInfo: { storageType, diskIops }
    } = this.state;
    updateFormField(`${clusterType}.volumeSize`, val);
    this.setState({ deviceInfo: { ...this.state.deviceInfo, volumeSize: val } });
    if (storageType === 'UltraSSD_LRS') {
      this.diskIopsChanged(diskIops);
    }
  }

  getThroughputByIops(currentIops, currentThroughput) {
    const {
      deviceInfo: { storageType }
    } = this.state;
    if (storageType === 'GP3') {
      if (
        (currentIops > GP3_DEFAULT_DISK_IOPS || currentThroughput > GP3_DEFAULT_DISK_THROUGHPUT) &&
        currentIops / currentThroughput < GP3_IOPS_TO_MAX_DISK_THROUGHPUT
      ) {
        return Math.min(
          GP3_MAX_THROUGHPUT,
          Math.max(currentIops / GP3_IOPS_TO_MAX_DISK_THROUGHPUT, GP3_DEFAULT_DISK_THROUGHPUT)
        );
      }
    } else if (storageType === 'UltraSSD_LRS') {
      const maxThroughput = Math.min(
        currentIops / UltraSSD_IOPS_TO_MAX_DISK_THROUGHPUT,
        UltraSSD_DISK_THROUGHPUT_CAP
      );
      return Math.max(0, Math.min(maxThroughput, currentThroughput));
    }

    return currentThroughput;
  }

  diskIopsChanged(val) {
    const { updateFormField, clusterType } = this.props;
    const {
      deviceInfo: { storageType, volumeSize, throughput, diskIops }
    } = this.state;
    const maxDiskIops = getMaxDiskIops(storageType, volumeSize);
    const minDiskIops = getMinDiskIops(storageType, volumeSize);

    const actualVal = Math.max(minDiskIops, Math.min(maxDiskIops, val));
    updateFormField(`${clusterType}.diskIops`, actualVal);
    this.setState({ deviceInfo: { ...this.state.deviceInfo, diskIops: actualVal } });

    if (
      (storageType === 'IO1' || storageType === 'GP3' || storageType === 'UltraSSD_LRS') &&
      diskIops !== actualVal
    ) {
      //resetting throughput
      this.throughputChanged(throughput);
    }
  }

  throughputChanged(val) {
    const { updateFormField, clusterType } = this.props;
    const {
      deviceInfo: { diskIops }
    } = this.state;
    var actualVal = this.getThroughputByIops(Number(diskIops), val);

    updateFormField(`${clusterType}.throughput`, actualVal);
    this.setState({ deviceInfo: { ...this.state.deviceInfo, throughput: actualVal } });
  }

  defaultRegionChanged(val) {
    const { updateFormField, clusterType } = this.props;
    this.setState({ nodeSetViaAZList: false });
    updateFormField(`${clusterType}.defaultRegion`, val);
    this.setState({ defaultRegion: val });
  }

  toggleUseTimeSync(event) {
    const { updateFormField, clusterType } = this.props;
    updateFormField(`${clusterType}.useTimeSync`, event.target.checked);
    this.setState({ useTimeSync: event.target.checked });
  }

  toggleAssignPublicIP(event) {
    const { updateFormField, clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      updateFormField('primary.assignPublicIP', event.target.checked);
      updateFormField('async.assignPublicIP', event.target.checked);
      this.setState({ assignPublicIP: event.target.checked });
    }
  }

  doAuthAndVolumeSizeValidation() {
    const { toggleDisableSubmit } = this.props;
    const { enableYSQL, enableYCQL } = this.state;
    const authCheck = enableYSQL || enableYCQL;
    toggleDisableSubmit(!authCheck || !this.checkVolumeSizeRestrictions());
  }

  updateFormFields(obj) {
    const { updateFormField } = this.props;
    for (const x in obj) {
      updateFormField(x, obj[x]);
    }
  }

  toggleEnableYSQL(event) {
    const { clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      this.updateFormFields({
        'primary.enableYSQL': event.target.checked,
        'async.enableYSQL': event.target.checked,
        'primary.enableYSQLAuth': event.target.checked,
        'async.enableYSQLAuth': event.target.checked,
        'primary.ysqlPassword': '',
        'primary.ysqlConfirmPassword': ''
      });
      this.setState({
        enableYSQL: event.target.checked,
        enableYSQLAuth: event.target.checked,
        ysqlPassword: '',
        ysqlConfirmPassword: ''
      });
    }
  }

  toggleEnableYSQLAuth(event) {
    const { clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      this.updateFormFields({
        'primary.enableYSQLAuth': event.target.checked,
        'async.enableYSQLAuth': event.target.checked,
        'primary.ysqlPassword': '',
        'primary.ysqlConfirmPassword': ''
      });
      this.setState({
        enableYSQLAuth: event.target.checked,
        ysqlPassword: '',
        ysqlConfirmPassword: ''
      });
    }
  }

  toggleEnableYCQL(event) {
    const { clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      this.updateFormFields({
        'primary.enableYCQL': event.target.checked,
        'async.enableYCQL': event.target.checked,
        'primary.enableYCQLAuth': event.target.checked,
        'async.enableYCQLAuth': event.target.checked,
        'primary.ycqlPassword': '',
        'primary.ycqlConfirmPassword': ''
      });
      this.setState({
        enableYCQL: event.target.checked,
        enableYCQLAuth: event.target.checked,
        ycqlPassword: '',
        ycqlConfirmPassword: ''
      });
    }
  }

  toggleEnableYCQLAuth(event) {
    const { clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      this.updateFormFields({
        'primary.enableYCQLAuth': event.target.checked,
        'async.enableYCQLAuth': event.target.checked,
        'primary.ycqlPassword': '',
        'primary.ycqlConfirmPassword': ''
      });
      this.setState({
        enableYCQLAuth: event.target.checked,
        ysqlPassword: '',
        ysqlConfirmPassword: ''
      });
    }
  }

  toggleEnableIPV6(event) {
    const { updateFormField, clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      updateFormField('primary.enableIPV6', event.target.checked);
      updateFormField('async.enableIPV6', event.target.checked);
      this.setState({ enableIPV6: event.target.checked });
    }
  }

  toggleEnableExposingService(event) {
    const { updateFormField, clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      updateFormField(
        'primary.enableExposingService',
        event.target.checked
          ? EXPOSING_SERVICE_STATE_TYPES['Exposed']
          : EXPOSING_SERVICE_STATE_TYPES['Unexposed']
      );
      updateFormField(
        'async.enableExposingService',
        event.target.checked
          ? EXPOSING_SERVICE_STATE_TYPES['Exposed']
          : EXPOSING_SERVICE_STATE_TYPES['Unexposed']
      );
      this.setState({
        enableExposingService: event.target.checked
          ? EXPOSING_SERVICE_STATE_TYPES['Exposed']
          : EXPOSING_SERVICE_STATE_TYPES['Unexposed']
      });
    }
  }

  toggleEnableYEDIS(event) {
    const { updateFormField, clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      updateFormField('primary.enableYEDIS', event.target.checked);
      updateFormField('async.enableYEDIS', event.target.checked);
      this.setState({ enableYEDIS: event.target.checked });
    }
  }

  toggleEnableNodeToNodeEncrypt(event) {
    const { updateFormField, clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      updateFormField('primary.enableNodeToNodeEncrypt', event.target.checked);
      updateFormField('async.enableNodeToNodeEncrypt', event.target.checked);
      this.setState({ enableNodeToNodeEncrypt: event.target.checked });
    }
  }

  toggleEnableClientToNodeEncrypt(event) {
    const { updateFormField, clusterType } = this.props;
    // Right now we only let primary cluster to update this flag, and
    // keep the async cluster to use the same value as primary.
    if (clusterType === 'primary') {
      updateFormField('primary.enableClientToNodeEncrypt', event.target.checked);
      updateFormField('async.enableClientToNodeEncrypt', event.target.checked);
      this.setState({ enableClientToNodeEncrypt: event.target.checked });
    }
  }

  toggleEnableEncryptionAtRest(event) {
    const { clusterType, getKMSConfigs, cloud } = this.props;
    const toggleValue = event.target.checked;

    if (clusterType === 'primary') {
      this.updateFormFields({
        'primary.enableEncryptionAtRest': toggleValue,
        'async.enableEncryptionAtRest': toggleValue
      });
      this.setState({ enableEncryptionAtRest: toggleValue });
      /*
       * Check if toggle is set to true and fetch list of KMS configs if
       * the PromiseState is not success. Note that if returned data is
       * an empty array, it is considered EMPTY and not SUCCESS, so if
       * field is toggled a subsequent time, we will fetch again.
       */
      if (toggleValue && !getPromiseState(cloud.authConfig).isSuccess()) {
        getKMSConfigs();
      }
    }
  }

  toggleCustomizePorts(event) {
    this.setState({ customizePorts: event.target.checked });
  }

  toggleUseSystemd(event) {
    const { clusterType } = this.props;

    if (clusterType === 'primary') {
      this.updateFormFields({
        'primary.useSystemd': event.target.checked,
        'async.useSystemd': event.target.checked
      });

      this.setState({
        useSystemd: event.target.checked
      });
    }
  }

  handleAwsArnChange(event) {
    const { updateFormField } = this.props;
    updateFormField('primary.awsArnString', event.target.value);
  }

  handleSelectAuthConfig(value) {
    const { updateFormField, clusterType } = this.props;
    updateFormField(`${clusterType}.selectEncryptionAtRestConfig`, value);
    this.setState({ selectEncryptionAtRestConfig: value });
  }

  replicationFactorChanged = (value) => {
    const {
      updateFormField,
      clusterType,
      universe: {
        currentUniverse: { data }
      }
    } = this.props;
    const clusterExists = isDefinedNotNull(data.universeDetails)
      ? isEmptyObject(getClusterByType(data.universeDetails.clusters, clusterType))
      : null;
    const self = this;

    if (!clusterExists) {
      this.setState({ nodeSetViaAZList: false, replicationFactor: value }, function () {
        if (self.state.numNodes <= value) {
          self.setState({ numNodes: value });
          updateFormField(`${clusterType}.numNodes`, value);
        }
      });
    }
    updateFormField(`${clusterType}.replicationFactor`, value);
  };

  hasFieldChanged = () => {
    const {
      universe: { currentUniverse },
      clusterType,
      getCurrentUserIntent
    } = this.props;
    if (
      isEmptyObject(currentUniverse.data) ||
      isEmptyObject(currentUniverse.data.universeDetails)
    ) {
      return true;
    }
    const currentCluster = getClusterByType(
      currentUniverse.data.universeDetails.clusters,
      clusterType
    );
    const existingIntent = isNonEmptyObject(currentCluster)
      ? _.clone(currentCluster.userIntent, true)
      : null;
    const currentIntent = getCurrentUserIntent(clusterType);

    return !areIntentsEqual(existingIntent, currentIntent);
  };

  handleUniverseConfigure(universeTaskParams) {
    const {
      universe: { universeConfigTemplate, currentUniverse },
      formValues,
      clusterType
    } = this.props;
    const instanceType = formValues[clusterType].instanceType;
    const regionList = formValues[clusterType].regionList;
    const verifyIntentConditions = function () {
      return isNonEmptyArray(regionList) && isNonEmptyString(instanceType);
    };

    if (verifyIntentConditions()) {
      if (
        isNonEmptyObject(currentUniverse.data) &&
        isNonEmptyObject(currentUniverse.data.universeDetails) &&
        isDefinedNotNull(
          getClusterByType(currentUniverse.data.universeDetails.clusters, clusterType)
        )
      ) {
        // cluster set: main edit flow
        const oldCluster = getClusterByType(
          currentUniverse.data.universeDetails.clusters,
          clusterType
        );
        const newCluster = getClusterByType(universeTaskParams.clusters, clusterType);
        if (
          isNonEmptyObject(oldCluster) &&
          isNonEmptyObject(newCluster) &&
          areIntentsEqual(oldCluster.userIntent, newCluster.userIntent)
        ) {
          this.props.getExistingUniverseConfiguration(currentUniverse.data.universeDetails);
        } else {
          this.props.submitConfigureUniverse(universeTaskParams, currentUniverse.data.universeUUID);
        }
      } else {
        // Create flow
        if (isEmptyObject(universeConfigTemplate.data) || universeConfigTemplate.data == null) {
          this.props.submitConfigureUniverse(universeTaskParams);
        } else {
          const currentClusterConfiguration = getClusterByType(
            universeConfigTemplate.data.clusters,
            clusterType
          );
          if (!isDefinedNotNull(currentClusterConfiguration)) {
            this.props.submitConfigureUniverse(universeTaskParams);
          } else if (
            !areIntentsEqual(
              getClusterByType(universeTaskParams.clusters, clusterType).userIntent,
              currentClusterConfiguration.userIntent
            )
          ) {
            this.props.submitConfigureUniverse(universeTaskParams);
          }
        }
      }
    }
  }

  configureUniverseNodeList(regionsChanged) {
    const {
      universe: { universeConfigTemplate, currentUniverse },
      formValues,
      clusterType,
      getCurrentUserIntent,
      updateTaskParams,
      featureFlags
    } = this.props;
    const { hasInstanceTypeChanged } = this.state;
    const currentProviderUUID = this.state.providerSelected;
    let universeTaskParams = {};
    if (isNonEmptyObject(universeConfigTemplate.data)) {
      universeTaskParams = _.cloneDeep(universeConfigTemplate.data);
    }
    if (
      this.props.type === 'Async' &&
      !isDefinedNotNull(getReadOnlyCluster(currentUniverse.data.universeDetails.clusters))
    ) {
      universeTaskParams = _.cloneDeep(currentUniverse.data.universeDetails);
    }
    if (isNonEmptyObject(currentUniverse.data)) {
      universeTaskParams.universeUUID = currentUniverse.data.universeUUID;
      universeTaskParams.expectedUniverseVersion = currentUniverse.data.version;
    }

    const userIntent = getCurrentUserIntent(clusterType);
    if (
      hasInstanceTypeChanged !==
      (formValues[clusterType].instanceType !== this.currentInstanceType)
    ) {
      this.setState({ hasInstanceTypeChanged: !hasInstanceTypeChanged });
    }

    if (
      isNonEmptyObject(formValues[clusterType].instanceTags) &&
      currentProviderUUID &&
      ['aws', 'azu', 'gcp'].includes(this.getCurrentProvider(currentProviderUUID).code)
    ) {
      userIntent['instanceTags'] = formValues[clusterType].instanceTags;
    }

    this.props.cloud.providers.data.forEach(function (providerItem) {
      if (providerItem.uuid === formValues[clusterType].provider) {
        userIntent.providerType = providerItem.code;
      }
    });

    const isEdit =
      this.props.type === 'Edit' || (this.props.type === 'Async' && this.state.isReadOnlyExists);
    updateTaskParams(universeTaskParams, userIntent, clusterType, isEdit);
    universeTaskParams.userAZSelected = false;
    universeTaskParams.regionsChanged = regionsChanged;

    const allowGeoPartitioning =
      featureFlags.test['enableGeoPartitioning'] || featureFlags.released['enableGeoPartitioning'];
    universeTaskParams.allowGeoPartitioning = allowGeoPartitioning;

    const cluster = getClusterByType(universeTaskParams.clusters, clusterType);
    if (
      clusterType === 'primary' &&
      isNonEmptyObject(cluster.placementInfo) &&
      isNonEmptyArray(cluster.placementInfo.cloudList)
    ) {
      if (allowGeoPartitioning && this.state.defaultRegion !== '') {
        cluster.placementInfo.cloudList[0].defaultRegion = this.state.defaultRegion;
      } else {
        delete cluster.placementInfo.cloudList[0].defaultRegion;
      }
    }

    this.handleUniverseConfigure(universeTaskParams);
  }

  numNodesChanged(value) {
    const { updateFormField, clusterType } = this.props;
    this.setState({ numNodes: value, nodeSetViaAZList: false });
    updateFormField(`${clusterType}.numNodes`, value);
  }

  getCurrentProvider(providerUUID) {
    return this.props.cloud.providers.data.find((provider) => provider.uuid === providerUUID);
  }

  setDefaultProviderStorage = (providerData) => {
    this.storageTypeChanged(DEFAULT_STORAGE_TYPES[providerData.code.toUpperCase()]);
  };

  providerChanged = async (value) => {
    const {
      updateFormField,
      clusterType,
      type,
      universe: {
        currentUniverse: { data }
      }
    } = this.props;
    const providerUUID = value;
    const currentProviderData = this.getCurrentProvider(value) || {};
    if (type?.toUpperCase() === 'CREATE' && clusterType === 'primary') {
      const releaseArr = (await fetchSupportedReleases(value))?.data;
      this.setState({ supportedReleases: releaseArr, ybSoftwareVersion: releaseArr[0] });
      updateFormField(`${clusterType}.ybSoftwareVersion`, releaseArr[0]);
    }

    const targetCluster =
      clusterType !== 'primary'
        ? isNonEmptyObject(data) && getPrimaryCluster(data.universeDetails.clusters)
        : isNonEmptyObject(data) && getReadOnlyCluster(data.universeDetails.clusters);
    if (isEmptyObject(data) || isDefinedNotNull(targetCluster)) {
      this.props.updateFormField(`${clusterType}.regionList`, []);
      //If we have accesskeys for a current selected provider we set that in the state or we fallback to default value.
      let defaultAccessKeyCode = initialState.accessKeyCode;
      if (isNonEmptyArray(this.props.accessKeys.data)) {
        const providerAccessKeys = this.props.accessKeys.data.filter(
          (key) => key.idKey.providerUUID === value
        );
        if (isNonEmptyArray(providerAccessKeys)) {
          defaultAccessKeyCode = providerAccessKeys[0].idKey.keyCode;
        }
      }
      updateFormField(`${clusterType}.accessKeyCode`, defaultAccessKeyCode);

      this.setState({
        nodeSetViaAZList: false,
        regionList: [],
        providerSelected: providerUUID,
        deviceInfo: {},
        accessKeyCode: defaultAccessKeyCode,
        awsInstanceWithEphemeralStorage: false,
        gcpInstanceWithEphemeralStorage: false,
        isReadOnlyExists:
          providerUUID && this.props.type === 'Create' && this.props.clusterType === 'async'
      });

      if (
        currentProviderData.code === 'aws' &&
        this.props.runtimeConfigs &&
        getPromiseState(this.props.runtimeConfigs).isSuccess()
      ) {
        const default_aws_instance = this.props.runtimeConfigs.data.configEntries.find(
          (c) => c.key === 'yb.internal.default_aws_instance_type'
        );
        if (default_aws_instance?.value) {
          updateFormField(`${clusterType}.instanceType`, default_aws_instance.value);
        }
      }

      this.props.getRegionListItems(providerUUID, true);
      this.props.getInstanceTypeListItems(providerUUID);
    }

    if (currentProviderData.code === 'onprem') {
      this.props.fetchNodeInstanceList(value);
    }
    this.setState({
      isKubernetesUniverse: currentProviderData.code === 'kubernetes'
    });

    this.setDefaultProviderStorage(currentProviderData);
  };

  accessKeyChanged(value) {
    const { clusterType } = this.props;
    this.props.updateFormField(`${clusterType}.accessKeyCode`, value);
  }

  instanceTypeChanged(value) {
    const { updateFormField, clusterType } = this.props;
    const instanceTypeValue = value;
    this.setState({
      awsInstanceWithEphemeralStorage: isEphemeralAwsStorageInstance(instanceTypeValue)
    });
    updateFormField(`${clusterType}.instanceType`, instanceTypeValue);
    this.setState({ instanceTypeSelected: instanceTypeValue, nodeSetViaAZList: false });
    this.setDeviceInfo(instanceTypeValue, this.props.cloud.instanceTypes.data);
  }

  tlsCertChanged(value) {
    this.updateFormFields({
      'primary.tlsCertificateId': value,
      'async.tlsCertificateId': value
    });
    this.setState({
      tlsCertificateId: value
    });
  }

  regionListChanged(value) {
    const {
      formValues,
      clusterType,
      updateFormField,
      cloud: { providers }
    } = this.props;

    updateFormField(`${clusterType}.regionList`, value || []);
    this.setState({ nodeSetViaAZList: false, regionList: value || [] });

    const currentProvider = providers.data.find((a) => a.uuid === formValues[clusterType].provider);
    if (!isNonEmptyString(formValues[clusterType].instanceType)) {
      updateFormField(
        `${clusterType}.instanceType`,
        DEFAULT_INSTANCE_TYPE_MAP[currentProvider.code]
      );
    }
  }

  validateUserTags(value) {
    if (value.length) {
      const forbiddenEntry = value.find((x) =>
        FORBIDDEN_GFLAG_KEYS.has(x.name?.trim?.()?.toUpperCase())
      );
      return forbiddenEntry ? `User tag "${forbiddenEntry.name}" is not allowed.` : undefined;
    }
  }

  validatePassword(value, formValues, formikBag, fieldName) {
    const errorMsg =
      'Password must be 8 characters minimum and must contain at least 1 digit, 1 uppercase, 1 lowercase and one of the !@#$%^&* (special) characters.';
    const isAuthEnabled =
      formValues.primary[
        fieldName === 'primary.ysqlPassword' ? 'enableYSQLAuth' : 'enableYCQLAuth'
      ];
    if (!isAuthEnabled) {
      return undefined;
    }
    if (value) {
      const passwordValidationRegex = /^(?=.*[0-9])(?=.*[!@#$%^&*])(?=.*[a-z])(?=.*[A-Z])[a-zA-Z0-9!@#$%^&*]{8,256}$/;
      return passwordValidationRegex.test(value) ? undefined : errorMsg;
    }
    return errorMsg;
  }

  validateConfirmPassword(value, formValues, formikBag, fieldName) {
    const passwordValue =
      formValues.primary[
        fieldName === 'primary.ysqlConfirmPassword' ? 'ysqlPassword' : 'ycqlPassword'
      ];
    if (!_.isEmpty(passwordValue)) {
      return value === passwordValue ? undefined : 'Password should match';
    }
    return undefined;
  }

  /**
   * This method is used to disable the ClientToNodeTLS field initially.
   * Once the NodeToNode TLS is enabled, then ClientToNode TLS will be editable.
   * If ClientToNode TLS sets to enable and NodeToNode TLS sets to disable then
   * ClientToNode TLS will be disabled.
   *
   * @param isFieldReadOnly If true then readonly access.
   * @param enableNodeToNodeEncrypt NodeToNodeTLS state.
   */
  clientToNodeEncryptField(isFieldReadOnly, enableNodeToNodeEncrypt) {
    return isFieldReadOnly || !enableNodeToNodeEncrypt;
  }

  instanceTypeGroupsToOptions = (groups) => {
    return Object.entries(groups).map(([group, list]) => (
      <optgroup label={`${group} type instances`} key={group}>
        {list.map((item) => (
          <option key={item.instanceTypeCode} value={item.instanceTypeCode}>
            {item.instanceTypeCode} ({pluralize('core', item.numCores, true)}, {item.memSizeGB}GB
            RAM)
          </option>
        ))}
      </optgroup>
    ));
  };

  render() {
    const {
      clusterType,
      type,
      cloud,
      softwareVersions,
      accessKeys,
      universe,
      formValues,
      featureFlags
    } = this.props;
    const { hasInstanceTypeChanged } = this.state;
    const self = this;
    let gflagArray = <span />;
    let tagsArray = <span />;
    let universeProviderList = [];
    let currentProviderCode = '';

    let currentProviderUUID = self.state.providerSelected;
    let currentAccessKey = self.state.accessKeyCode;
    if (formValues[clusterType]) {
      if (formValues[clusterType].provider) currentProviderUUID = formValues[clusterType].provider;

      if (formValues[clusterType].accessKeyCode)
        currentAccessKey = formValues[clusterType].accessKeyCode;
    }

    // Populate the cloud provider list
    if (isNonEmptyArray(cloud.providers.data)) {
      universeProviderList = cloud.providers.data.reduce((providerOption, providerItem) => {
        if (this.props.type === 'Async' && providerItem.code === 'kubernetes') {
          return providerOption;
        }
        if (providerItem.uuid === currentProviderUUID) {
          currentProviderCode = providerItem.code;
        }
        return providerOption.concat(
          <option key={providerItem.uuid} value={providerItem.uuid}>
            {providerItem.name}
          </option>
        );
      }, []);
    }

    // Spot price and EBS types
    let storageTypeSelector = null;
    let deviceDetail = null;
    let iopsField = null;
    let throughputField = null;
    function volumeTypeFormat(num) {
      return num + ' GB';
    }
    const ebsTypesList =
      cloud.ebsTypes &&
      cloud.ebsTypes.sort().map(function (ebsType, idx) {
        return (
          <option key={ebsType} value={ebsType}>
            {API_UI_STORAGE_TYPES[ebsType]}
          </option>
        );
      });
    const gcpTypesList =
      cloud.gcpTypes.data &&
      cloud.gcpTypes.data.sort().map(function (gcpType, idx) {
        return (
          <option key={gcpType} value={gcpType}>
            {API_UI_STORAGE_TYPES[gcpType]}
          </option>
        );
      });
    const azuTypesList =
      cloud.azuTypes.data &&
      cloud.azuTypes.data.sort().map?.(function (azuType, idx) {
        return (
          <option key={azuType} value={azuType}>
            {API_UI_STORAGE_TYPES[azuType]}
          </option>
        );
      });

    let configList = cloud.authConfig.data ?? [];
    //feature flagging
    const isHCVaultEnabled = featureFlags.test.enableHCVault || featureFlags.released.enableHCVault;
    if (!isHCVaultEnabled)
      configList = configList.filter((config) => config.metadata.provider !== 'HASHICORP');
    //feature flagging
    const kmsConfigList = [
      <option value="" key={`kms-option-0`}>
        Select Configuration
      </option>,
      ...configList.map((config, index) => {
        const labelName = config.metadata.provider + ' - ' + config.metadata.name;
        return (
          <option value={config.metadata.configUUID} key={`kms-option-${index + 1}`}>
            {labelName}
          </option>
        );
      })
    ];
    const isFieldReadOnly =
      (isNonEmptyObject(universe.currentUniverse.data) &&
        (this.props.type === 'Edit' ||
          (this.props.type === 'Async' && this.state.isReadOnlyExists))) ||
      (this.props.type === 'Create' &&
        this.props.clusterType === 'async' &&
        this.state.isReadOnlyExists);
    const isSWVersionReadOnly =
      (isNonEmptyObject(universe.currentUniverse.data) &&
        (this.props.type === 'Edit' || this.props.type === 'Async')) ||
      (this.props.type === 'Create' && this.props.clusterType === 'async');
    const isReadOnlyOnEdit =
      isNonEmptyObject(universe.currentUniverse.data) &&
      (this.props.type === 'Edit' || (this.props.type === 'Async' && this.state.isReadOnlyExists));

    //Get list of cloud providers
    const providerNameField = (
      <Field
        name={`${clusterType}.provider`}
        type="select"
        component={YBSelectWithLabel}
        label="Provider"
        onInputChanged={this.providerChanged}
        options={universeProviderList}
        readOnlySelect={isReadOnlyOnEdit}
      />
    );

    const deviceInfo = this.state.deviceInfo;

    if (isNonEmptyObject(formValues[clusterType])) {
      const currentCluster = formValues[clusterType];
      if (isNonEmptyString(currentCluster.numVolumes)) {
        deviceInfo['numVolumes'] = currentCluster.numVolumes;
      }
      if (isNonEmptyString(currentCluster.volumeSize)) {
        deviceInfo['volumeSize'] = currentCluster.volumeSize;
      }
      if (isNonEmptyString(currentCluster.diskIops)) {
        deviceInfo['diskIops'] = currentCluster.diskIops;
      }
      if (isNonEmptyString(currentCluster.throughput)) {
        deviceInfo['throughput'] = currentCluster.throughput;
      }
      if (isNonEmptyObject(currentCluster.storageType)) {
        deviceInfo['storageType'] = currentCluster.storageType;
      }
      if (isNonEmptyObject(currentCluster.storageClass)) {
        deviceInfo['storageClass'] = currentCluster.storageClass;
      }
    }

    if (isNonEmptyObject(deviceInfo)) {
      const currentProvider = this.getCurrentProvider(self.state.providerSelected);
      if (
        (self.state.volumeType === 'EBS' ||
          self.state.volumeType === 'SSD' ||
          self.state.volumeType === 'NVME') &&
        isDefinedNotNull(currentProvider)
      ) {
        const isInAws = currentProvider.code === 'aws';
        const isInGcp = currentProvider.code === 'gcp';
        const isInAzu = currentProvider.code === 'azu';
        // We don't want to keep the volume fixed in case of Kubernetes or persistent GCP storage.
        const fixedVolumeInfo =
          (self.state.volumeType === 'SSD' || self.state.volumeType === 'NVME') &&
          currentProvider.code !== 'kubernetes' &&
          (!deviceInfo.storageType || deviceInfo.storageType === 'Scratch') &&
          currentProvider.code !== 'azu';
        const fixedNumVolumes =
          (self.state.volumeType === 'SSD' || self.state.volumeType === 'NVME') &&
          currentProvider.code !== 'kubernetes' &&
          currentProvider.code !== 'gcp' &&
          currentProvider.code !== 'azu';
        const isProvisionalIOType =
          deviceInfo.storageType === 'IO1' ||
          deviceInfo.storageType === 'GP3' ||
          deviceInfo.storageType === 'UltraSSD_LRS';
        if (isProvisionalIOType) {
          iopsField = (
            <Field
              name={`${clusterType}.diskIops`}
              component={YBNumericInputWithLabel}
              label="Provisioned IOPS"
              onInputChanged={self.diskIopsChanged}
              readOnly={isFieldReadOnly}
            />
          );
        }
        const isProvisionalThroughput =
          deviceInfo.storageType === 'GP3' || deviceInfo.storageType === 'UltraSSD_LRS';
        if (isProvisionalThroughput) {
          throughputField = (
            <Field
              name={`${clusterType}.throughput`}
              component={YBNumericInputWithLabel}
              label="Provisioned Throughput (MiB/sec)"
              onInputChanged={self.throughputChanged}
              readOnly={isFieldReadOnly}
            />
          );
        }
        const numVolumes = (
          <span className="volume-info-field volume-info-count">
            <Field
              name={`${clusterType}.numVolumes`}
              component={YBUnControlledNumericInput}
              label="Number of Volumes"
              onInputChanged={self.numVolumesChanged}
              readOnly={fixedNumVolumes || !hasInstanceTypeChanged}
            />
          </span>
        );
        const smartResizePossible =
          isDefinedNotNull(currentProvider) &&
          (currentProvider.code === 'aws' || currentProvider.code === 'gcp') &&
          !isEphemeralAwsStorageInstance(this.currentInstanceType) &&
          deviceInfo.storageType !== 'Scratch' &&
          clusterType !== 'async';

        const volumeSize = (
          <span className="volume-info-field volume-info-size">
            <Field
              name={`${clusterType}.volumeSize`}
              component={YBUnControlledNumericInput}
              label="Volume Size"
              valueFormat={volumeTypeFormat}
              onInputChanged={self.volumeSizeChanged}
              readOnly={fixedVolumeInfo || (!hasInstanceTypeChanged && !smartResizePossible)}
            />
          </span>
        );
        deviceDetail = (
          <span className="volume-info">
            {numVolumes}
            &times;
            {volumeSize}
          </span>
        );
        // Only for AWS EBS or GCP, show type option.
        if (isInAws && self.state.volumeType === 'EBS') {
          storageTypeSelector = (
            <span className="volume-info form-group-shrinked">
              <Field
                name={`${clusterType}.storageType`}
                component={YBSelectWithLabel}
                options={ebsTypesList}
                label="EBS Type"
                defaultValue={DEFAULT_STORAGE_TYPES['AWS']}
                onInputChanged={self.storageTypeChanged}
                readOnlySelect={isFieldReadOnly}
              />
            </span>
          );
        } else if (isInGcp) {
          storageTypeSelector = (
            <>
              <span className="volume-info form-group-shrinked">
                <Field
                  name={`${clusterType}.storageType`}
                  component={YBSelectWithLabel}
                  options={gcpTypesList}
                  label="Storage Type (SSD)"
                  defaultValue={DEFAULT_STORAGE_TYPES['GCP']}
                  onInputChanged={self.storageTypeChanged}
                  readOnlySelect={isFieldReadOnly}
                />
              </span>
              {this.state.gcpInstanceWithEphemeralStorage &&
                (featureFlags.test['pausedUniverse'] ||
                  featureFlags.released['pausedUniverse']) && (
                  <span className="gcp-ephemeral-storage-warning">
                    ! Selected instance type is with ephemeral storage, If you will pause this
                    universe your data will get lost.
                  </span>
                )}
            </>
          );
        } else if (isInAzu) {
          storageTypeSelector = (
            <span className="volume-info form-group-shrinked">
              <Field
                name={`${clusterType}.storageType`}
                component={YBSelectWithLabel}
                options={azuTypesList}
                label="Storage Type (SSD)"
                defaultValue={DEFAULT_STORAGE_TYPES['AZU']}
                onInputChanged={self.storageTypeChanged}
                readOnlySelect={isFieldReadOnly}
              />
            </span>
          );
        }
      }
    }

    let assignPublicIP = <span />;
    let useTimeSync = <span />;
    let enableYSQL = <span />;
    let enableYSQLAuth = <span />;
    let ysqlAuthPassword = <span />;
    let ycqlAuthPassword = <span />;
    let enableYCQL = <span />;
    let enableYCQLAuth = <span />;
    let enableYEDIS = <span />;
    let enableNodeToNodeEncrypt = <span />;
    let enableClientToNodeEncrypt = <span />;
    let selectTlsCert = <span />;
    let enableEncryptionAtRest = <span />;
    let selectEncryptionAtRestConfig = <span />;
    const currentProvider = this.getCurrentProvider(currentProviderUUID);
    const disableToggleOnChange = clusterType !== 'primary';
    if (
      isDefinedNotNull(currentProvider) &&
      (currentProvider.code === 'aws' ||
        currentProvider.code === 'gcp' ||
        currentProvider.code === 'azu' ||
        currentProvider.code === 'onprem' ||
        currentProvider.code === 'kubernetes')
    ) {
      enableYSQL = (
        <Field
          name={`${clusterType}.enableYSQL`}
          component={YBToggle}
          isReadOnly={isFieldReadOnly}
          disableOnChange={disableToggleOnChange}
          checkedVal={this.state.enableYSQL}
          onToggle={this.toggleEnableYSQL}
          label="Enable YSQL"
          subLabel="Enable the YSQL API endpoint to run postgres compatible workloads."
        />
      );
      enableYSQLAuth = (
        <Row>
          <Col sm={12} md={12} lg={6}>
            <div className="form-right-aligned-labels">
              <Field
                name={`${clusterType}.enableYSQLAuth`}
                component={YBToggle}
                isReadOnly={isFieldReadOnly}
                disableOnChange={disableToggleOnChange}
                checkedVal={this.state.enableYSQLAuth}
                onToggle={this.toggleEnableYSQLAuth}
                label="Enable YSQL Auth"
                subLabel="Enable the YSQL password authentication."
              />
            </div>
          </Col>
        </Row>
      );
      ysqlAuthPassword = (
        <Row>
          <Col sm={12} md={6} lg={6}>
            <div className="form-right-aligned-labels">
              <Field
                name={`${clusterType}.ysqlPassword`}
                component={YBPassword}
                validate={this.validatePassword}
                autoComplete="new-password"
                label="YSQL Auth Password"
                placeholder="Enter Password"
              />
            </div>
          </Col>
          <Col sm={12} md={6} lg={6}>
            <div className="form-right-aligned-labels">
              <Field
                name={`${clusterType}.ysqlConfirmPassword`}
                component={YBPassword}
                validate={this.validateConfirmPassword}
                autoComplete="new-password"
                label="Confirm Password"
                placeholder="Confirm Password"
              />
            </div>
          </Col>
        </Row>
      );

      enableYCQL = (
        <Field
          name={`${clusterType}.enableYCQL`}
          component={YBToggle}
          isReadOnly={isFieldReadOnly}
          disableOnChange={disableToggleOnChange}
          checkedVal={this.state.enableYCQL}
          onToggle={this.toggleEnableYCQL}
          label="Enable YCQL"
          subLabel="Enable the YCQL API endpoint to run cassandra compatible workloads."
        />
      );
      enableYCQLAuth = (
        <Row>
          <Col sm={12} md={12} lg={6}>
            <div className="form-right-aligned-labels">
              <Field
                name={`${clusterType}.enableYCQLAuth`}
                component={YBToggle}
                isReadOnly={isFieldReadOnly}
                disableOnChange={disableToggleOnChange}
                checkedVal={this.state.enableYCQLAuth}
                onToggle={this.toggleEnableYCQLAuth}
                label="Enable YCQL Auth"
                subLabel="Enable the YCQL password authentication."
              />
            </div>
          </Col>
        </Row>
      );
      ycqlAuthPassword = (
        <Row>
          <Col sm={12} md={6} lg={6}>
            <div className="form-right-aligned-labels">
              <Field
                name={`${clusterType}.ycqlPassword`}
                component={YBPassword}
                validate={this.validatePassword}
                autoComplete="new-password"
                label="YCQL Auth Password"
                placeholder="Enter Password"
              />
            </div>
          </Col>
          <Col sm={12} md={6} lg={6}>
            <div className="form-right-aligned-labels">
              <Field
                name={`${clusterType}.ycqlConfirmPassword`}
                component={YBPassword}
                validate={this.validateConfirmPassword}
                autoComplete="new-password"
                label="Confirm Password"
                placeholder="Confirm Password"
              />
            </div>
          </Col>
        </Row>
      );
      enableYEDIS = (
        <Field
          name={`${clusterType}.enableYEDIS`}
          component={YBToggle}
          isReadOnly={isFieldReadOnly}
          disableOnChange={disableToggleOnChange}
          checkedVal={this.state.enableYEDIS}
          onToggle={this.toggleEnableYEDIS}
          label="Enable YEDIS"
          subLabel="Enable the YEDIS API endpoint to run REDIS compatible workloads."
        />
      );
      enableNodeToNodeEncrypt = (
        <Field
          name={`${clusterType}.enableNodeToNodeEncrypt`}
          component={YBToggle}
          isReadOnly={isFieldReadOnly}
          disableOnChange={disableToggleOnChange}
          checkedVal={this.state.enableNodeToNodeEncrypt}
          onToggle={this.toggleEnableNodeToNodeEncrypt}
          label="Enable Node-to-Node TLS"
          subLabel="Enable encryption in transit for communication between the DB servers."
        />
      );
      enableClientToNodeEncrypt = (
        <Field
          name={`${clusterType}.enableClientToNodeEncrypt`}
          component={YBToggle}
          isReadOnly={isFieldReadOnly}
          disableOnChange={disableToggleOnChange}
          checkedVal={this.state.enableClientToNodeEncrypt}
          onToggle={this.toggleEnableClientToNodeEncrypt}
          label="Enable Client-to-Node TLS"
          subLabel="Enable encryption in transit for communication between clients and the DB servers."
        />
      );
      enableEncryptionAtRest = (
        <Field
          name={`${clusterType}.enableEncryptionAtRest`}
          component={YBToggle}
          isReadOnly={isFieldReadOnly}
          disableOnChange={disableToggleOnChange}
          checkedVal={this.state.enableEncryptionAtRest}
          onToggle={this.toggleEnableEncryptionAtRest}
          label="Enable Encryption at Rest"
          title="Upload encryption key file"
          subLabel="Enable encryption for data stored on the tablet servers."
        />
      );

      if (this.state.enableEncryptionAtRest) {
        selectEncryptionAtRestConfig = (
          <Field
            name={`${clusterType}.selectEncryptionAtRestConfig`}
            component={YBSelectWithLabel}
            label="Key Management Service Config"
            options={kmsConfigList}
            onInputChanged={this.handleSelectAuthConfig}
            readOnlySelect={isFieldReadOnly}
          />
        );
      }
    }

    {
      // Block scope for state variables
      const { enableClientToNodeEncrypt, enableNodeToNodeEncrypt } = this.state;
      if (
        isDefinedNotNull(currentProvider) &&
        (enableClientToNodeEncrypt || enableNodeToNodeEncrypt)
      ) {
        const tlsCertOptions = [];
        if (this.props.type === 'Create') {
          tlsCertOptions.push(
            <option key={'cert-option-0'} value={''}>
              Create new certificate
            </option>
          );
        }

        if (!_.isEmpty(this.props.userCertificates.data)) {
          this.props.userCertificates.data.forEach((cert, index) => {
            if (this.props.type === 'Create') {
              const disableOnPremCustomCerts =
                currentProvider.code !== 'onprem' && cert.certType === 'CustomCertHostPath';
              tlsCertOptions.push(
                <option
                  key={`cert-option-${index + 1}`}
                  value={cert.uuid}
                  disabled={disableOnPremCustomCerts}
                >
                  {cert.label}
                </option>
              );
            } else {
              const isCustomCertAndOnPrem =
                currentProvider.code === 'onprem' && cert.certType === 'CustomCertHostPath';
              tlsCertOptions.push(
                <option
                  key={`cert-option-${index + 1}`}
                  value={cert.uuid}
                  disabled={!isCustomCertAndOnPrem}
                >
                  {cert.label}
                </option>
              );
            }
          });
        }

        const isSelectReadOnly =
          (this.props.type === 'Edit' && currentProvider.code !== 'onprem') ||
          clusterType === 'async';
        selectTlsCert = (
          <Field
            name={`${clusterType}.tlsCertificateId`}
            component={YBSelectWithLabel}
            options={tlsCertOptions}
            onInputChanged={this.tlsCertChanged}
            readOnlySelect={isSelectReadOnly}
            label="Root Certificate"
          />
        );
      }
    }

    if (
      isDefinedNotNull(currentProvider) &&
      (currentProvider.code === 'aws' ||
        currentProvider.code === 'gcp' ||
        currentProvider.code === 'azu')
    ) {
      // Assign public ip would be only enabled for primary and that same
      // value will be used for async as well.
      assignPublicIP = (
        <Field
          name={`${clusterType}.assignPublicIP`}
          component={YBToggle}
          isReadOnly={isFieldReadOnly}
          disableOnChange={disableToggleOnChange}
          checkedVal={this.state.assignPublicIP}
          onToggle={this.toggleAssignPublicIP}
          label="Assign Public IP"
          subLabel="Assign a public IP to the DB servers for connections over the internet."
        />
      );
    }
    // Only enable Time Sync Service toggle for AWS/GCP/Azure.
    const currentAccessKeyInfo = accessKeys.data.find(
      (key) =>
        key.idKey.providerUUID === currentProviderUUID && key.idKey.keyCode === currentAccessKey
    );
    if (
      isDefinedNotNull(currentProvider) &&
      ['aws', 'gcp', 'azu'].includes(currentProvider.code) &&
      currentAccessKeyInfo?.keyInfo?.showSetUpChrony === false
    ) {
      const providerCode =
        currentProvider.code === 'aws' ? 'AWS' : currentProvider.code === 'gcp' ? 'GCP' : 'Azure';
      useTimeSync = (
        <Field
          name={`${clusterType}.useTimeSync`}
          component={YBToggle}
          isReadOnly={isReadOnlyOnEdit}
          checkedVal={this.state.useTimeSync}
          onToggle={this.toggleUseTimeSync}
          label={`Use ${providerCode} Time Sync`}
          subLabel={`Enable the ${providerCode} Time Sync functionality for the DB servers.`}
        />
      );
    }

    let universeRegionList = [];
    if (self.state.providerSelected) {
      universeRegionList =
        cloud.regions.data &&
        cloud.regions.data.map(function (regionItem) {
          return { value: regionItem.uuid, label: regionItem.name };
        });
    }

    const universeRegionListWithNone = this.state.regionList && [
      <option value="" key={`region-option-none`}>
        None
      </option>,
      isNonEmptyObject(cloud.regions.data) &&
        this.state.regionList.map?.(function (regionItem) {
          if (typeof regionItem === 'string') {
            const region = cloud.regions.data.find((region) => region.uuid === regionItem);
            regionItem = !isEmptyObject(region) ? { value: region.uuid, label: region.name } : null;
          }
          return isNonEmptyObject(regionItem) ? (
            <option key={regionItem.value} value={regionItem.value}>
              {regionItem.label}
            </option>
          ) : (
            <option value="" key={`region-option-unknown`}>
              Unknown region {regionItem}
            </option>
          );
        })
    ];

    let universeInstanceTypeList = <option />;

    if (currentProviderCode === 'aws') {
      const optGroups =
        this.props.cloud.instanceTypes &&
        isNonEmptyArray(this.props.cloud.instanceTypes.data) &&
        this.props.cloud.instanceTypes.data.reduce(function (groups, it) {
          const prefix = it.instanceTypeCode.substr(0, it.instanceTypeCode.indexOf('.'));
          groups[prefix] ? groups[prefix].push(it) : (groups[prefix] = [it]);
          return groups;
        }, {});
      if (isNonEmptyObject(optGroups)) {
        universeInstanceTypeList = Object.keys(optGroups).map(function (key, idx) {
          return (
            <optgroup label={`${key.toUpperCase()} type instances`} key={key + idx}>
              {optGroups[key]
                .sort((a, b) => /\d+(?!\.)/.exec(a) - /\d+(?!\.)/.exec(b))
                .map((item, arrIdx) => (
                  <option key={idx + arrIdx} value={item.instanceTypeCode}>
                    {item.instanceTypeCode} ({pluralize('core', item.numCores, true)},{' '}
                    {item.memSizeGB}GB RAM)
                  </option>
                ))}
            </optgroup>
          );
        });
      }
    } else if (currentProviderCode === 'gcp') {
      const groups = {};
      const list = cloud.instanceTypes?.data || [];
      _.sortBy(list, 'instanceTypeCode');

      list.forEach((item) => {
        const prefix = item.instanceTypeCode.split('-')[0].toUpperCase();
        groups[prefix] = groups[prefix] || [];
        groups[prefix].push(item);
      });

      universeInstanceTypeList = this.instanceTypeGroupsToOptions(groups);
    } else if (currentProviderCode === 'azu') {
      const groups = {};
      // clone as we'll mutate items by adding "processed" prop
      const list = _.cloneDeep(cloud.instanceTypes?.data) || [];
      _.sortBy(list, 'instanceTypeCode');

      for (const [group, regexp] of Object.entries(AZURE_INSTANCE_TYPE_GROUPS)) {
        list.forEach((item) => {
          if (regexp.test(item.instanceTypeCode)) {
            groups[group] = groups[group] || [];
            groups[group].push(item);
            item.processed = true;
          }
        });
      }

      // catch uncategorized instance types, if any
      list.forEach((item) => {
        if (!item.processed) {
          groups['Other'] = groups['Other'] || [];
          groups['Other'].push(item);
        }
      });

      universeInstanceTypeList = this.instanceTypeGroupsToOptions(groups);
    } else if (currentProviderCode === 'kubernetes') {
      universeInstanceTypeList =
        cloud.instanceTypes.data &&
        cloud.instanceTypes.data.map(function (instanceTypeItem) {
          return (
            <option
              key={instanceTypeItem.instanceTypeCode}
              value={instanceTypeItem.instanceTypeCode}
            >
              {instanceTypeItem.instanceTypeName || instanceTypeItem.instanceTypeCode} (
              {instanceTypeItem.numCores} {instanceTypeItem.numCores > 1 ? 'cores' : 'core'},{' '}
              {instanceTypeItem.memSizeGB}GB RAM)
            </option>
          );
        });
    } else {
      universeInstanceTypeList =
        cloud.instanceTypes.data &&
        cloud.instanceTypes.data.map(function (instanceTypeItem) {
          return (
            <option
              key={instanceTypeItem.instanceTypeCode}
              value={instanceTypeItem.instanceTypeCode}
            >
              {instanceTypeItem.instanceTypeCode}
            </option>
          );
        });
    }

    let placementStatus = null;
    let placementStatusOnprem = null;
    const cluster =
      clusterType === 'primary'
        ? getPrimaryCluster(_.get(self.props, 'universe.universeConfigTemplate.data.clusters', []))
        : getReadOnlyCluster(
            _.get(self.props, 'universe.universeConfigTemplate.data.clusters', [])
          );
    const placementCloud = getPlacementCloud(cluster);
    const regionAndProviderDefined =
      isNonEmptyArray(formValues[clusterType]?.regionList) &&
      isNonEmptyString(formValues[clusterType]?.provider);

    // For onprem provider type if numNodes < maxNumNodes then show the AZ error.
    if (self.props.universe.currentPlacementStatus && placementCloud && regionAndProviderDefined) {
      placementStatus = (
        <AZPlacementInfo
          placementInfo={self.props.universe.currentPlacementStatus}
          placementCloud={placementCloud}
          providerCode={currentProvider?.code}
        />
      );
    } else if (
      currentProvider?.code === 'onprem' &&
      !(this.state.numNodes <= this.state.maxNumNodes) &&
      self.props.universe.currentPlacementStatus &&
      regionAndProviderDefined
    ) {
      placementStatusOnprem = (
        <AZPlacementInfo
          placementInfo={self.props.universe.currentPlacementStatus}
          providerCode={currentProvider.code}
        />
      );
    }
    const configTemplate = self.props.universe.universeConfigTemplate;
    const clusters = _.get(configTemplate, 'data.clusters', []);
    const showPlacementStatus =
      configTemplate && clusterType === 'primary'
        ? !!getPrimaryCluster(clusters)
        : clusterType === 'async'
        ? !!getReadOnlyCluster(clusters)
        : false;
    const enableGeoPartitioning =
      featureFlags.test['enableGeoPartitioning'] || featureFlags.released['enableGeoPartitioning'];
    const azSelectorTable = (
      <div>
        <AZSelectorTable
          {...this.props}
          clusterType={clusterType}
          numNodesChangedViaAzList={this.numNodesChangedViaAzList}
          minNumNodes={this.state.replicationFactor}
          maxNumNodes={this.state.maxNumNodes}
          currentProvider={this.getCurrentProvider(currentProviderUUID)}
          isKubernetesUniverse={this.state.isKubernetesUniverse}
          enableGeoPartitioning={enableGeoPartitioning}
        />
        {showPlacementStatus && placementStatus}
      </div>
    );

    if (clusterType === 'primary' && this.state.ybSoftwareVersion) {
      gflagArray = (
        <Row>
          <Col md={12}>
            <h4>G-Flags</h4>
          </Col>
          <Col md={12}>
            <FieldArray
              component={GFlagComponent}
              name={`${clusterType}.gFlags`}
              dbVersion={this.state.ybSoftwareVersion}
              isReadOnly={isFieldReadOnly}
            />
          </Col>
        </Row>
      );
      if (['azu', 'aws', 'gcp'].includes(currentProviderCode)) {
        tagsArray = (
          <Row>
            <Col md={12}>
              <h4>User Tags</h4>
            </Col>
            <Col md={6}>
              <FieldArray
                component={GFlagArrayComponent}
                name={`${clusterType}.instanceTags`}
                flagType="tag"
                operationType="Create"
                isReadOnly={false}
                validate={this.validateUserTags}
              />
            </Col>
          </Row>
        );
      }
    }

    const softwareVersionOptions = (type?.toUpperCase() === 'CREATE' && clusterType === 'primary'
      ? this.state.supportedReleases
      : softwareVersions
    )?.map((item, idx) => (
      <option key={idx} value={item}>
        {item}
      </option>
    ));

    let accessKeyOptions = (
      <option key={1} value={this.state.accessKeyCode}>
        {this.state.accessKeyCode}
      </option>
    );
    if (_.isObject(accessKeys) && isNonEmptyArray(accessKeys.data)) {
      accessKeyOptions = accessKeys.data
        .filter((key) => key.idKey.providerUUID === currentProviderUUID)
        .map((item, idx) => (
          <option key={idx} value={item.idKey.keyCode}>
            {item.idKey.keyCode}
          </option>
        ));
    }
    let universeNameField = <span />;
    if (clusterType === 'primary') {
      universeNameField = (
        <Field
          name={`${clusterType}.universeName`}
          type="text"
          normalize={trimSpecialChars}
          component={YBTextInputWithLabel}
          label="Name"
          isReadOnly={isFieldReadOnly}
        />
      );
    }

    return (
      <div>
        <div className="form-section" data-yb-section="cloud-config">
          <Row>
            <Col md={6}>
              <h4 style={{ marginBottom: 40 }}>Cloud Configuration</h4>
              {this.state.isAZUpdating}
              <div className="form-right-aligned-labels">
                {universeNameField}
                {providerNameField}
                <Field
                  name={`${clusterType}.regionList`}
                  component={YBMultiSelectWithLabel}
                  options={universeRegionList}
                  label="Regions"
                  data-yb-field="regions"
                  isMulti={true}
                  selectValChanged={this.regionListChanged}
                  providerSelected={currentProviderUUID}
                />
                {clusterType === 'async'
                  ? [
                      <Field
                        key="numNodes"
                        name={`${clusterType}.numNodes`}
                        type="text"
                        component={YBControlledNumericInputWithLabel}
                        className={
                          getPromiseState(this.props.universe.universeConfigTemplate).isLoading()
                            ? 'readonly'
                            : ''
                        }
                        data-yb-field="nodes"
                        label={this.state.isKubernetesUniverse ? 'Pods' : 'Nodes'}
                        onInputChanged={this.numNodesChanged}
                        onLabelClick={this.numNodesClicked}
                        val={this.state.numNodes}
                        minVal={Number(this.state.replicationFactor)}
                      />,
                      <Field
                        key="replicationFactor"
                        name={`${clusterType}.replicationFactor`}
                        type="text"
                        component={YBRadioButtonBarWithLabel}
                        options={[1, 2, 3, 4, 5, 6, 7]}
                        label="Replication Factor"
                        initialValue={this.state.replicationFactor}
                        onSelect={this.replicationFactorChanged}
                        isReadOnly={isReadOnlyOnEdit}
                      />
                    ]
                  : null}
              </div>

              {clusterType !== 'async' && [
                <Row>
                  <div className="form-right-aligned-labels">
                    <Col lg={5}>
                      <Field
                        name={`${clusterType}.numNodes`}
                        type="text"
                        component={YBControlledNumericInputWithLabel}
                        className={
                          getPromiseState(this.props.universe.universeConfigTemplate).isLoading()
                            ? 'readonly'
                            : ''
                        }
                        label={this.state.isKubernetesUniverse ? 'Pods' : 'Nodes'}
                        onInputChanged={this.numNodesChanged}
                        onLabelClick={this.numNodesClicked}
                        val={this.state.numNodes}
                        minVal={Number(this.state.replicationFactor)}
                      />
                    </Col>
                    <Col lg={7} className="button-group-row">
                      <Field
                        name={`${clusterType}.replicationFactor`}
                        type="text"
                        component={YBRadioButtonBarWithLabel}
                        options={[1, 3, 5, 7]}
                        label="Replication Factor"
                        initialValue={this.state.replicationFactor}
                        onSelect={this.replicationFactorChanged}
                        isReadOnly={isFieldReadOnly}
                      />
                    </Col>
                  </div>
                </Row>,
                <Row>
                  <div className="form-right-aligned-labels">
                    {enableGeoPartitioning && (
                      <Field
                        name={`${clusterType}.defaultRegion`}
                        type="select"
                        component={YBSelectWithLabel}
                        label="Default Region"
                        initialValue={this.state.defaultRegion}
                        options={universeRegionListWithNone}
                        onInputChanged={this.defaultRegionChanged}
                        readOnlySelect={isFieldReadOnly}
                      />
                    )}
                  </div>
                </Row>
              ]}
            </Col>
            <Col md={6} className={'universe-az-selector-container'}>
              {placementStatusOnprem || (regionAndProviderDefined && azSelectorTable)}
            </Col>
          </Row>
        </div>
        <div className="form-section" data-yb-section="instance-config">
          <Row>
            <Col md={12}>
              <h4>Instance Configuration</h4>
            </Col>
          </Row>
          <Row>
            <Col sm={12} md={12} lg={6}>
              <div className="form-right-aligned-labels">
                <Field
                  name={`${clusterType}.instanceType`}
                  component={YBSelectWithLabel}
                  label="Instance Type"
                  options={universeInstanceTypeList}
                  onInputChanged={this.instanceTypeChanged}
                  readOnlySelect={getPromiseState(cloud.instanceTypes).isLoading()}
                />
              </div>
              {this.state.awsInstanceWithEphemeralStorage &&
                (featureFlags.test['pausedUniverse'] ||
                  featureFlags.released['pausedUniverse']) && (
                  <span className="aws-instance-with-ephemeral-storage-warning">
                    ! Selected instance type is with ephemeral storage, If you will pause this
                    universe your data will get lost.
                  </span>
                )}
            </Col>
            <Col sm={12} md={12} lg={6}>
              {deviceDetail && (
                <div className="form-right-aligned-labels">
                  <div className="form-inline-controls">
                    <div
                      className="form-group universe-form-instance-info"
                      data-yb-field="volumn-info"
                    >
                      <label className="form-item-label form-item-label-shrink">Volume Info</label>
                      {deviceDetail}
                    </div>
                  </div>
                  <div className="form-inline-controls">
                    <div className="form-group universe-form-instance-info">
                      {storageTypeSelector}
                    </div>
                  </div>
                </div>
              )}
              {!this.checkVolumeSizeRestrictions() && (
                <div className="has-error">
                  <div className="help-block standard-error">
                    Forbidden change. To perform smart resize only increase volume size
                  </div>
                </div>
              )}
            </Col>
          </Row>
          <Row>
            <Col sm={12} md={12} lg={6}>
              <div className="form-right-aligned-labels">
                {selectTlsCert}
                {assignPublicIP}
                {useTimeSync}
              </div>
            </Col>
            <Col sm={12} md={6} lg={4}>
              <div className="form-right-aligned-labels right-side-form-field">
                {iopsField}
                {throughputField}
                {selectEncryptionAtRestConfig}
              </div>
            </Col>
          </Row>
          {currentProviderUUID && !this.state.enableYSQL && !this.state.enableYCQL && (
            <div className="has-error">
              <div className="help-block standard-error">
                Enable at least one endpoint among YSQL and YCQL
              </div>
            </div>
          )}
          <Row>
            <Col sm={12} md={12} lg={6}>
              <div className="form-right-aligned-labels">{enableYSQL}</div>
            </Col>
          </Row>
          {this.state.enableYSQL && enableYSQLAuth}
          {clusterType === 'primary' &&
            type === 'Create' &&
            this.state.enableYSQL &&
            this.state.enableYSQLAuth &&
            ysqlAuthPassword}
          <Row>
            <Col sm={12} md={12} lg={6}>
              <div className="form-right-aligned-labels">{enableYCQL}</div>
            </Col>
          </Row>
          {this.state.enableYCQL && enableYCQLAuth}
          {clusterType === 'primary' &&
            type === 'Create' &&
            this.state.enableYCQL &&
            this.state.enableYCQLAuth &&
            ycqlAuthPassword}
          <Row>
            <Col sm={12} md={12} lg={6}>
              <div className="form-right-aligned-labels">
                {enableYEDIS}
                {enableNodeToNodeEncrypt}
                {enableClientToNodeEncrypt}
                {enableEncryptionAtRest}
                <Field name={`${clusterType}.mountPoints`} component={YBTextInput} type="hidden" />
              </div>
            </Col>
          </Row>
        </div>
        {isDefinedNotNull(currentProvider) && (
          <div className="form-section" data-yb-section="advanced">
            <Row>
              <Col md={12}>
                <h4>Advanced</h4>
              </Col>
              <Col sm={5} md={4}>
                <div className="form-right-aligned-labels">
                  <Field
                    name={`${clusterType}.ybSoftwareVersion`}
                    component={YBSelectWithLabel}
                    options={softwareVersionOptions}
                    label="DB Version"
                    onInputChanged={this.softwareVersionChanged}
                    readOnlySelect={isSWVersionReadOnly}
                  />
                </div>
              </Col>
              {!this.state.isKubernetesUniverse && (
                <Col sm={5} md={4}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.accessKeyCode`}
                      type="select"
                      component={YBSelectWithLabel}
                      label="Access Key"
                      onInputChanged={this.accessKeyChanged}
                      options={accessKeyOptions}
                      readOnlySelect={isFieldReadOnly}
                    />
                  </div>
                </Col>
              )}
            </Row>
            {isDefinedNotNull(currentProvider) && currentProvider.code === 'aws' && (
              <Row>
                <Col sm={5} md={4}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.awsArnString`}
                      type="text"
                      component={YBTextInputWithLabel}
                      label="Instance Profile ARN"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
              </Row>
            )}
            {isDefinedNotNull(currentProvider) && currentProvider.code === 'kubernetes' && (
              <Row>
                <Col md={12}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.enableIPV6`}
                      component={YBToggle}
                      isReadOnly={isFieldReadOnly}
                      disableOnChange={disableToggleOnChange}
                      checkedVal={this.state.enableIPV6}
                      onToggle={this.toggleEnableIPV6}
                      label="Enable IPV6"
                      subLabel="Use IPV6 networking for connections between the DB servers."
                    />
                  </div>
                </Col>
              </Row>
            )}
            {isDefinedNotNull(currentProvider) && currentProvider.code === 'kubernetes' && (
              <Row>
                <Col md={12}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.enableExposingService`}
                      component={YBToggle}
                      isReadOnly={isFieldReadOnly}
                      disableOnChange={disableToggleOnChange}
                      checkedVal={
                        this.state.enableExposingService === EXPOSING_SERVICE_STATE_TYPES['Exposed']
                      }
                      onToggle={this.toggleEnableExposingService}
                      label="Enable Public Network Access"
                      subLabel="Assign a load balancer or nodeport for connecting to the DB endpoints over the internet."
                    />
                  </div>
                </Col>
              </Row>
            )}
            {isDefinedNotNull(currentProvider) && (
              <Row>
                <Col md={12}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.useSystemd`}
                      component={YBToggle}
                      defaultChecked={false}
                      disableOnChange={disableToggleOnChange}
                      checkedVal={this.state.useSystemd}
                      onToggle={this.toggleUseSystemd}
                      label="Enable Systemd Services"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
              </Row>
            )}
            {isDefinedNotNull(currentProvider) && currentProvider.code !== 'kubernetes' && (
              <Row>
                <Col md={12}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.customizePorts`}
                      component={YBToggle}
                      defaultChecked={false}
                      disableOnChange={disableToggleOnChange}
                      checkedVal={this.state.customizePorts}
                      onToggle={this.toggleCustomizePorts}
                      label="Override Deployment Ports"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
              </Row>
            )}
            {this.state.customizePorts && (
              <Row>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.masterHttpPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="Master HTTP Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.masterRpcPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="Master RPC Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
              </Row>
            )}
            {this.state.customizePorts && (
              <Row>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.tserverHttpPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="Tserver HTTP Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.tserverRpcPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="Tserver RPC Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
              </Row>
            )}
            {this.state.customizePorts && this.state.enableYCQL && (
              <Row>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.yqlHttpPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="YCQL HTTP Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.yqlRpcPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="YCQL RPC Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
              </Row>
            )}
            {this.state.customizePorts && this.state.enableYSQL && (
              <Row>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.ysqlHttpPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="YSQL HTTP Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.ysqlRpcPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="YSQL RPC Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
              </Row>
            )}
            {this.state.customizePorts && this.state.enableYEDIS && (
              <Row>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.redisHttpPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="Yedis HTTP Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
                <Col sm={3}>
                  <div className="form-right-aligned-labels">
                    <Field
                      name={`${clusterType}.redisRpcPort`}
                      type="text"
                      component={YBTextInputWithLabel}
                      normalize={normalizeToValidPort}
                      validate={portValidation}
                      label="Yedis RPC Port"
                      isReadOnly={isFieldReadOnly}
                    />
                  </div>
                </Col>
              </Row>
            )}
          </div>
        )}
        <div className="form-section" data-yb-section="g-flags">
          {gflagArray}
        </div>
        {clusterType === 'primary' && <div className="form-section no-border">{tagsArray}</div>}
      </div>
    );
  }
}

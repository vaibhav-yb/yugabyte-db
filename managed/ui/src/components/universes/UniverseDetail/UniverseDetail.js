// Copyright (c) YugaByte, Inc.

import React, { Component } from 'react';
import { Link, withRouter, browserHistory } from 'react-router';
import { Grid, DropdownButton, MenuItem, Tab, Alert } from 'react-bootstrap';
import Measure from 'react-measure';
import { mouseTrap } from 'react-mousetrap';

import { CustomerMetricsPanel } from '../../metrics';
import { RollingUpgradeFormContainer } from '../../../components/common/forms';
import {
  UniverseFormContainer,
  UniverseStatusContainer,
  NodeDetailsContainer,
  DeleteUniverseContainer,
  UniverseAppsModal,
  UniverseConnectModal,
  UniverseOverviewContainerNew,
  EncryptionKeyModalContainer,
  ToggleUniverseStateContainer,
  ToggleBackupStateContainer
} from '../../universes';
import { YBLabelWithIcon } from '../../common/descriptors';
import { YBTabsWithLinksPanel } from '../../panels';
import { ListTablesContainer, ListBackupsContainer, ReplicationContainer } from '../../tables';
import { QueriesViewer } from '../../queries';
import { isEmptyObject, isNonEmptyObject } from '../../../utils/ObjectUtils';
import {
  isKubernetesUniverse,
  isPausableUniverse,
  isUniverseType
} from '../../../utils/UniverseUtils';
import { getPromiseState } from '../../../utils/PromiseUtils';
import { getPrimaryCluster } from '../../../utils/UniverseUtils';
import { hasLiveNodes } from '../../../utils/UniverseUtils';
import { YBLoading, YBErrorIndicator } from '../../common/indicators';
import { UniverseHealthCheckList } from './compounds/UniverseHealthCheckList';
import { UniverseTaskList } from './compounds/UniverseTaskList';
import { YBMenuItem } from './compounds/YBMenuItem';
import { MenuItemsContainer } from './compounds/MenuItemsContainer';
import {
  isNonAvailable,
  isDisabled,
  isNotHidden,
  getFeatureState
} from '../../../utils/LayoutUtils';
import { SecurityMenu } from '../SecurityModal/SecurityMenu';
import Replication from '../../xcluster/Replication';
import { UniverseLevelBackup } from '../../backupv2/Universe/UniverseLevelBackup';
import { UniverseSupportBundle } from '../UniverseSupportBundle/UniverseSupportBundle';
import { YBTag } from '../../common/YBTag';

import './UniverseDetail.scss';

const INSTANCE_WITH_EPHEMERAL_STORAGE_ONLY = ['i3', 'c5d', 'c6gd'];

export const isEphemeralAwsStorageInstance = (instanceType) => {
  return INSTANCE_WITH_EPHEMERAL_STORAGE_ONLY.includes(instanceType?.split?.('.')[0]);
};

class UniverseDetail extends Component {
  constructor(props) {
    super(props);
    this.showUpgradeMarker = this.showUpgradeMarker.bind(this);
    this.onEditUniverseButtonClick = this.onEditUniverseButtonClick.bind(this);
    this.state = {
      dimensions: {},
      showAlert: false,
      actionsDropdownOpen: false
    };
  }

  hasReadReplica = (universeInfo) => {
    const clusters = universeInfo.universeDetails.clusters;
    return clusters.some((cluster) => cluster.clusterType === 'ASYNC');
  };

  componentWillUnmount() {
    this.props.resetUniverseInfo();
    this.props.resetTablesList();
  }

  componentDidMount() {
    const {
      customer: { currentCustomer }
    } = this.props;
    if (isNonAvailable(currentCustomer.data.features, 'universes.details')) {
      if (isNonAvailable(currentCustomer.data.features, 'universes')) {
        browserHistory.push('/');
      } else {
        browserHistory.push('/universes/');
      }
    }

    this.props.bindShortcut(['ctrl+e'], this.onEditUniverseButtonClick);

    if (this.props.location.pathname !== '/universes/create') {
      let uuid = this.props.uuid;
      if (typeof this.props.universeSelectionId !== 'undefined') {
        uuid = this.props.universeUUID;
      }
      this.props.getUniverseInfo(uuid);

      if (isDisabled(currentCustomer.data.features, 'universes.details.health')) {
        // Get alerts instead of Health
        this.props.getAlertsList();
      } else {
        this.props.getHealthCheck(uuid);
      }
    }
    this.props.fetchRunTimeConfigs();
  }

  componentDidUpdate(prevProps) {
    const {
      universe: { currentUniverse },
      universeTables
    } = this.props;
    if (
      getPromiseState(currentUniverse).isSuccess() &&
      !getPromiseState(prevProps.universe.currentUniverse).isSuccess()
    ) {
      if (hasLiveNodes(currentUniverse.data) && !universeTables.length) {
        this.props.fetchUniverseTables(currentUniverse.data.universeUUID);
      }
    }
  }

  onResize(dimensions) {
    this.setState({ dimensions });
  }

  onEditUniverseButtonClick = () => {
    const location = Object.assign({}, browserHistory.getCurrentLocation());
    Object.assign(location, { pathname: `/universes/${this.props.uuid}/edit/primary` });
    browserHistory.push(location);
  };

  isCurrentUniverseDeleteTask = (uuid) => {
    return this.props.tasks.customerTaskList.filter(
      (task) => task.targetUUID === uuid && task.type === 'Delete' && task.target === 'Universe'
    );
  };

  getUniverseInfo = () => {
    const universeUUID = this.props.universe.currentUniverse.data.universeUUID;
    const currentUniverseTasks = this.isCurrentUniverseDeleteTask(universeUUID);
    if (currentUniverseTasks.length > 0) {
      browserHistory.push('/');
    } else {
      this.props.getUniverseInfo(universeUUID);
    }
  };

  showUpgradeMarker = () => {
    const {
      updateAvailable,
      universe: { rollingUpgrade },
      modal: { showModal, visibleModal }
    } = this.props;

    return (
      !getPromiseState(rollingUpgrade).isLoading() &&
      updateAvailable !== 0 &&
      !(showModal && visibleModal === 'softwareUpgradesModal')
    );
  };

  stripQueryParams = () => {
    browserHistory.replace(browserHistory.getCurrentLocation().pathname);
  };

  transitToDefaultRoute = () => {
    const currentLocation = this.props.location;
    currentLocation.query = currentLocation.query.tab ? { tab: currentLocation.query.tab } : {};
    this.props.router.push(currentLocation);
  };

  handleSubmitManageKey = (res) => {
    if (res.payload.isAxiosError) {
      this.setState({
        showAlert: true,
        alertType: 'danger',
        alertMessage: res.payload.message
      });
    } else {
      this.setState({
        showAlert: true,
        alertType: 'success',
        alertMessage:
          JSON.parse(res.payload.config.data).key_op === 'ENABLE'
            ? 'Encryption key has been set!'
            : 'Encryption-at-Rest has been disabled!'
      });
    }
    setTimeout(() => this.setState({ showAlert: false }), 3000);

    this.props.closeModal();
  };

  closeAlert = () => {
    this.setState({ showAlert: false });
  };

  render() {
    const {
      uuid,
      updateAvailable,
      modal,
      modal: { showModal, visibleModal },
      universe,
      tasks,
      universe: { currentUniverse },
      location: { query, pathname },
      showSoftwareUpgradesModal,
      showVMImageUpgradeModal,
      showTLSConfigurationModal,
      showRollingRestartModal,
      showUpgradeSystemdModal,
      showThirdpartyUpgradeModal,
      showRunSampleAppsModal,
      showSupportBundleModal,
      showGFlagsModal,
      showManageKeyModal,
      showDeleteUniverseModal,
      showToggleUniverseStateModal,
      showToggleBackupModal,
      updateBackupState,
      closeModal,
      customer,
      customer: { currentCustomer, currentUser, runtimeConfigs },
      params: { tab },
      featureFlags,
      providers,
      accessKeys
    } = this.props;
    const { showAlert, alertType, alertMessage } = this.state;
    const universePaused = universe?.currentUniverse?.data?.universeDetails?.universePaused;
    const updateInProgress = universe?.currentUniverse?.data?.universeDetails?.updateInProgress;
    const primaryCluster = getPrimaryCluster(
      universe?.currentUniverse?.data?.universeDetails?.clusters
    );
    const useSystemd = primaryCluster?.userIntent?.useSystemd;
    const isReadOnlyUniverse =
      getPromiseState(currentUniverse).isSuccess() &&
      currentUniverse.data.universeDetails.capability === 'READ_ONLY';

    const isProviderK8S =
      getPromiseState(currentUniverse).isSuccess() &&
      isUniverseType(currentUniverse.data, 'kubernetes');

    const providerUUID = primaryCluster?.userIntent?.provider;
    const provider = providers.data.find((provider) => provider.uuid === providerUUID);

    var onPremSkipProvisioning = false;
    if (provider && provider.code === 'onprem') {
      const onPremKey = accessKeys.data.find(
        (accessKey) => accessKey.idKey.providerUUID === provider.uuid
      );
      onPremSkipProvisioning = onPremKey?.keyInfo.skipProvisioning;
    }

    const type =
      pathname.indexOf('edit') < 0
        ? 'Create'
        : this.props.params.type
        ? this.props.params.type === 'primary'
          ? 'Edit'
          : 'Async'
        : 'Edit';

    if (pathname === '/universes/create') {
      return <UniverseFormContainer type="Create" />;
    }

    if (getPromiseState(currentUniverse).isLoading() || getPromiseState(currentUniverse).isInit()) {
      return <YBLoading />;
    } else if (isEmptyObject(currentUniverse.data)) {
      return <span />;
    }

    if (type === 'Async' || (isNonEmptyObject(query) && query.edit && query.async)) {
      if (isReadOnlyUniverse) {
        // not fully legit but mandatory fallback for manually edited query
        this.transitToDefaultRoute();
      } else {
        return <UniverseFormContainer type="Async" />;
      }
    }

    if (type === 'Edit' || (isNonEmptyObject(query) && query.edit)) {
      if (isReadOnlyUniverse) {
        // not fully legit but mandatory fallback for manually edited query
        this.transitToDefaultRoute();
      } else {
        return <UniverseFormContainer type="Edit" />;
      }
    }

    if (getPromiseState(currentUniverse).isError()) {
      return <YBErrorIndicator type="universe" uuid={uuid} />;
    }

    const width = this.state.dimensions.width;
    const universeInfo = currentUniverse.data;
    const nodePrefixes = [currentUniverse.data.universeDetails.nodePrefix];
    const isItKubernetesUniverse = isKubernetesUniverse(currentUniverse.data);

    let editTLSAvailability = getFeatureState(
      currentCustomer.data.features,
      'universes.details.overview.manageEncryption'
    );
    let manageKeyAvailability = getFeatureState(
      currentCustomer.data.features,
      'universes.details.overview.manageEncryption'
    );

    const defaultTab = isNotHidden(currentCustomer.data.features, 'universes.details.overview')
      ? 'overview'
      : 'overview';
    const activeTab = tab || defaultTab;
    const tabElements = [
      //common tabs for every universe
      ...[
        isNotHidden(currentCustomer.data.features, 'universes.details.overview') && (
          <Tab.Pane
            eventKey={'overview'}
            tabtitle="Overview"
            key="overview-tab"
            mountOnEnter={true}
            unmountOnExit={true}
            disabled={isDisabled(currentCustomer.data.features, 'universes.details.overview')}
          >
            <UniverseOverviewContainerNew
              width={width}
              universe={universe}
              updateAvailable={updateAvailable}
              showSoftwareUpgradesModal={showSoftwareUpgradesModal}
              tabRef={this.ybTabPanel}
            />
          </Tab.Pane>
        ),

        isNotHidden(currentCustomer.data.features, 'universes.details.tables') && (
          <Tab.Pane
            eventKey={'tables'}
            tabtitle="Tables"
            key="tables-tab"
            mountOnEnter={true}
            unmountOnExit={true}
            disabled={isDisabled(currentCustomer.data.features, 'universes.details.tables')}
          >
            <ListTablesContainer />
          </Tab.Pane>
        ),

        isNotHidden(currentCustomer.data.features, 'universes.details.nodes') && (
          <Tab.Pane
            eventKey={'nodes'}
            tabtitle={isItKubernetesUniverse ? 'Pods' : 'Nodes'}
            key="nodes-tab"
            mountOnEnter={true}
            unmountOnExit={true}
            disabled={isDisabled(currentCustomer.data.features, 'universes.details.nodes')}
          >
            <NodeDetailsContainer />
          </Tab.Pane>
        ),

        isNotHidden(currentCustomer.data.features, 'universes.details.metrics') && (
          <Tab.Pane
            eventKey={'metrics'}
            tabtitle="Metrics"
            key="metrics-tab"
            mountOnEnter={true}
            unmountOnExit={true}
            disabled={isDisabled(currentCustomer.data.features, 'universes.details.metrics')}
          >
            <div className="universe-detail-content-container">
              <CustomerMetricsPanel
                customer={customer}
                origin={'universe'}
                width={width}
                nodePrefixes={nodePrefixes}
                isKubernetesUniverse={isItKubernetesUniverse}
                visibleModal={visibleModal}
              />
            </div>
          </Tab.Pane>
        ),

        isNotHidden(currentCustomer.data.features, 'universes.details.queries') && (
          <Tab.Pane
            eventKey={'queries'}
            tabtitle="Queries"
            key="queries-tab"
            mountOnEnter={true}
            unmountOnExit={true}
            onExit={this.stripQueryParams}
            disabled={isDisabled(currentCustomer.data.features, 'universes.details.queries')}
          >
            <QueriesViewer />
          </Tab.Pane>
        ),

        isNotHidden(currentCustomer.data.features, 'universes.details.replication') && (
          <Tab.Pane
            eventKey={'replication'}
            tabtitle="Replication"
            key="replication-tab"
            mountOnEnter={true}
            unmountOnExit={true}
            disabled={isDisabled(currentCustomer.data.features, 'universes.details.replication')}
          >
            {featureFlags.released.enableXCluster || featureFlags.test.enableXCluster ? (
              <Replication currentUniverseUUID={currentUniverse.data.universeUUID} />
            ) : (
              <ReplicationContainer />
            )}
          </Tab.Pane>
        ),

        isNotHidden(currentCustomer.data.features, 'universes.details.tasks') && (
          <Tab.Pane
            eventKey={'tasks'}
            tabtitle="Tasks"
            key="tasks-tab"
            mountOnEnter={true}
            unmountOnExit={true}
            disabled={isDisabled(currentCustomer.data.features, 'universes.details.tasks')}
          >
            <UniverseTaskList
              universe={universe}
              tasks={tasks}
              isCommunityEdition={!!customer.INSECURE_apiToken}
              fetchCustomerTasks={this.props.fetchCustomerTasks}
              refreshUniverseData={this.getUniverseInfo}
              abortCurrentTask={this.props.abortCurrentTask}
              hideTaskAbortModal={this.props.hideTaskAbortModal}
              showTaskAbortModal={this.props.showTaskAbortModal}
              visibleModal={visibleModal}
            />
          </Tab.Pane>
        )
      ],
      //tabs relevant for non-imported universes only
      ...(isReadOnlyUniverse
        ? []
        : [
            isNotHidden(currentCustomer.data.features, 'universes.details.backups') && (
              <Tab.Pane
                eventKey={'backups'}
                tabtitle={
                  <>
                    Backups
                    {(featureFlags.test['backupv2'] || featureFlags.released['backupv2']) && (
                      <YBTag>Beta</YBTag>
                    )}
                  </>
                }
                key="backups-tab"
                mountOnEnter={true}
                unmountOnExit={true}
                disabled={isDisabled(currentCustomer.data.features, 'universes.details.backups')}
              >
                {featureFlags.test['backupv2'] || featureFlags.released['backupv2'] ? (
                  <UniverseLevelBackup />
                ) : (
                  <ListBackupsContainer currentUniverse={currentUniverse.data} />
                )}
              </Tab.Pane>
            ),

            isNotHidden(currentCustomer.data.features, 'universes.details.health') && (
              <Tab.Pane
                eventKey={'health'}
                tabtitle="Health"
                key="health-tab"
                mountOnEnter={true}
                unmountOnExit={true}
                disabled={isDisabled(currentCustomer.data.features, 'universes.details.heath')}
              >
                <UniverseHealthCheckList
                  universe={universe}
                  currentCustomer={currentCustomer}
                  currentUser={currentUser}
                />
              </Tab.Pane>
            )
          ])
    ].filter((element) => element);

    const currentBreadCrumb = (
      <div className="detail-label-small">
        <Link to="/universes">
          <YBLabelWithIcon>Universes</YBLabelWithIcon>
        </Link>
        <Link to={`/universes/${currentUniverse.data.universeUUID}`}>
          <YBLabelWithIcon icon="fa fa-angle-right fa-fw">
            {currentUniverse.data.name}
          </YBLabelWithIcon>
        </Link>
      </div>
    );

    const {
      data: {
        universeDetails: { nodeDetailsSet }
      }
    } = currentUniverse;

    const isEphemeralAwsStorage =
      nodeDetailsSet.find?.((node) => {
        return isEphemeralAwsStorageInstance(node.cloudInfo?.instance_type);
      }) !== undefined;

    /**
     * Handle the backup state toggle.
     * i.e open the confirmation model if backup is to be disabled.
     * else, Enable the backups.
     */
    const handleBackupToggle = () => {
      const takeBackups =
        currentUniverse.data.universeConfig &&
        currentUniverse.data.universeConfig?.takeBackups === 'true';
      takeBackups
        ? showToggleBackupModal()
        : updateBackupState(currentUniverse.data.universeUUID, !takeBackups);
    };

    const enableThirdpartyUpgrade =
        featureFlags.test['enableThirdpartyUpgrade'] || featureFlags.released['enableThirdpartyUpgrade'];

    return (
      <Grid id="page-wrapper" fluid={true} className={`universe-details universe-details-new`}>
        {showAlert && (
          <Alert bsStyle={alertType} onDismiss={this.closeAlert}>
            <h4>{alertType === 'success' ? 'Success' : 'Error'}</h4>
            <p>{alertMessage}</p>
          </Alert>
        )}
        {/* UNIVERSE NAME */}
        {currentBreadCrumb}
        <div className="universe-detail-status-container">
          <h2>{currentUniverse.data.name}</h2>
          <UniverseStatusContainer
            currentUniverse={currentUniverse.data}
            showLabelText={true}
            refreshUniverseData={this.getUniverseInfo}
          />
        </div>
        {isNotHidden(currentCustomer.data.features, 'universes.details.pageActions') && (
          <div className="page-action-buttons">
            {/* UNIVERSE EDIT */}
            <div className="universe-detail-btn-group">
              <UniverseConnectModal />

              <DropdownButton
                title="Actions"
                className={this.showUpgradeMarker() ? 'btn-marked' : ''}
                id="bg-nested-dropdown"
                pullRight
                onToggle={(isOpen) => this.setState({ actionsDropdownOpen: isOpen })}
              >
                <MenuItemsContainer
                  parentDropdownOpen={this.state.actionsDropdownOpen}
                  mainMenu={(showSubmenu) => (
                    <>
                      {!universePaused && (
                        <YBMenuItem
                          disabled={updateInProgress}
                          onClick={showSoftwareUpgradesModal}
                          availability={getFeatureState(
                            currentCustomer.data.features,
                            'universes.details.overview.upgradeSoftware'
                          )}
                        >
                          <YBLabelWithIcon icon="fa fa-arrow-up fa-fw">
                            Upgrade Software
                          </YBLabelWithIcon>
                          {this.showUpgradeMarker() && (
                            <span className="badge badge-pill badge-red pull-right">
                              {updateAvailable}
                            </span>
                          )}
                        </YBMenuItem>
                      )}
                      {!universePaused &&
                        runtimeConfigs &&
                        getPromiseState(runtimeConfigs).isSuccess() &&
                        runtimeConfigs.data.configEntries.find(
                          (c) => c.key === 'yb.upgrade.vmImage'
                        ).value === 'true' && (
                          <YBMenuItem disabled={updateInProgress} onClick={showVMImageUpgradeModal}>
                            <YBLabelWithIcon icon="fa fa-arrow-up fa-fw">
                              Upgrade VM Image
                            </YBLabelWithIcon>
                          </YBMenuItem>
                        )}
                      {!universePaused && !useSystemd && (
                        <YBMenuItem
                          disabled={updateInProgress || onPremSkipProvisioning}
                          onClick={showUpgradeSystemdModal}
                          availability={getFeatureState(
                            currentCustomer.data.features,
                            'universes.details.overview.systemdUpgrade'
                          )}
                        >
                          <YBLabelWithIcon icon="fa fa-wrench fa-fw">
                            Upgrade To Systemd
                          </YBLabelWithIcon>
                        </YBMenuItem>
                      )}
                      {!universePaused &&
                       enableThirdpartyUpgrade && (
                        <YBMenuItem
                          disabled={updateInProgress}
                          onClick={showThirdpartyUpgradeModal}
                          availability={getFeatureState(
                            currentCustomer.data.features,
                              'universes.details.overview.thirdpartyUpgrade'
                            )}
                          >
                            <YBLabelWithIcon icon="fa fa-wrench fa-fw">
                              Upgrade 3rd-party Software
                            </YBLabelWithIcon>
                          </YBMenuItem>
                      )}
                      {!isReadOnlyUniverse &&
                        !universePaused &&
                        isNotHidden(
                          currentCustomer.data.features,
                          'universes.details.overview.editUniverse'
                        ) && (
                          <YBMenuItem
                            to={`/universes/${uuid}/edit/primary`}
                            availability={getFeatureState(
                              currentCustomer.data.features,
                              'universes.details.overview.editUniverse'
                            )}
                          >
                            <YBLabelWithIcon icon="fa fa-pencil">Edit Universe</YBLabelWithIcon>
                          </YBMenuItem>
                        )}

                      {!universePaused && (
                        <YBMenuItem
                          disabled={updateInProgress}
                          onClick={showGFlagsModal}
                          availability={getFeatureState(
                            currentCustomer.data.features,
                            'universes.details.overview.editGFlags'
                          )}
                        >
                          <YBLabelWithIcon icon="fa fa-flag fa-fw">Edit Flags</YBLabelWithIcon>
                        </YBMenuItem>
                      )}

                      {!universePaused && (
                        <YBMenuItem
                          disabled={updateInProgress}
                          onClick={() => showSubmenu('security')}
                          availability={getFeatureState(
                            currentCustomer.data.features,
                            'universes.details.overview.manageEncryption'
                          )}
                        >
                          <YBLabelWithIcon icon="fa fa-key fa-fw">Edit Security</YBLabelWithIcon>
                          <span className="pull-right">
                            <i className="fa fa-chevron-right submenu-icon" />
                          </span>
                        </YBMenuItem>
                      )}

                      {!universePaused && (
                        <YBMenuItem
                          disabled={updateInProgress}
                          onClick={showRollingRestartModal}
                          availability={getFeatureState(
                            currentCustomer.data.features,
                            'universes.details.overview.restartUniverse'
                          )}
                        >
                          <YBLabelWithIcon icon="fa fa-refresh fa-fw">
                            Initiate Rolling Restart
                          </YBLabelWithIcon>
                        </YBMenuItem>
                      )}

                      {!isReadOnlyUniverse && !universePaused && !isProviderK8S && (
                        <YBMenuItem
                          disabled={updateInProgress}
                          to={`/universes/${uuid}/edit/async`}
                          availability={getFeatureState(
                            currentCustomer.data.features,
                            'universes.details.overview.readReplica'
                          )}
                        >
                          <YBLabelWithIcon icon="fa fa-copy fa-fw">
                            {this.hasReadReplica(universeInfo) ? 'Edit' : 'Add'} Read Replica
                          </YBLabelWithIcon>
                        </YBMenuItem>
                      )}

                      {!universePaused && (
                        <UniverseAppsModal
                          currentUniverse={currentUniverse.data}
                          modal={modal}
                          closeModal={closeModal}
                          button={
                            <YBMenuItem
                              disabled={updateInProgress}
                              onClick={showRunSampleAppsModal}
                            >
                              <YBLabelWithIcon icon="fa fa-terminal">
                                Run Sample Apps
                              </YBLabelWithIcon>
                            </YBMenuItem>
                          }
                        />
                      )}

                      {(featureFlags.test['supportBundle'] ||
                        featureFlags.released['supportBundle']) && (
                        <>
                          <MenuItem divider />
                          {!universePaused && (
                            <UniverseSupportBundle
                              currentUniverse={currentUniverse.data}
                              modal={modal}
                              closeModal={closeModal}
                              button={
                                <YBMenuItem
                                  disabled={updateInProgress}
                                  onClick={showSupportBundleModal}
                                >
                                  <YBLabelWithIcon icon="fa fa-file-archive-o">
                                    Support Bundles <YBTag>Beta</YBTag>
                                  </YBLabelWithIcon>
                                </YBMenuItem>
                              }
                            />
                          )}
                          <MenuItem divider />
                        </>
                      )}

                      {!universePaused && (
                        <YBMenuItem
                          disabled={updateInProgress}
                          onClick={handleBackupToggle}
                          availability={getFeatureState(
                            currentCustomer.data.features,
                            'universes.backup'
                          )}
                        >
                          <YBLabelWithIcon
                            icon={
                              currentUniverse.data.universeConfig.takeBackups === 'true'
                                ? 'fa fa-pause'
                                : 'fa fa-play'
                            }
                          >
                            {currentUniverse.data.universeConfig &&
                            currentUniverse.data.universeConfig.takeBackups === 'true'
                              ? 'Disable Backup'
                              : 'Enable Backup'}
                          </YBLabelWithIcon>
                        </YBMenuItem>
                      )}

                      <MenuItem divider />

                      {/* TODO:
                      1. For now, we're enabling the Pause Universe for providerType==='aws'
                      only. This functionality needs to be enabled for all the cloud
                      providers and once that's done this condition needs to be removed.
                      2. One more condition needs to be added which specifies the
                      current status of the universe. */}

                      {/*
                      Read-only users should not be given the rights to "Pause Universe"
                      */}

                      {isPausableUniverse(currentUniverse?.data) &&
                        !isEphemeralAwsStorage &&
                        (featureFlags.test['pausedUniverse'] ||
                          featureFlags.released['pausedUniverse']) && (
                          <YBMenuItem
                            onClick={showToggleUniverseStateModal}
                            availability={getFeatureState(
                              currentCustomer.data.features,
                              'universes.details.overview.pausedUniverse'
                            )}
                          >
                            <YBLabelWithIcon
                              icon={universePaused ? 'fa fa-play-circle-o' : 'fa fa-pause-circle-o'}
                            >
                              {universePaused ? 'Resume Universe' : 'Pause Universe'}
                            </YBLabelWithIcon>
                          </YBMenuItem>
                        )}

                      <YBMenuItem
                        onClick={showDeleteUniverseModal}
                        availability={getFeatureState(
                          currentCustomer.data.features,
                          'universes.details.overview.deleteUniverse'
                        )}
                      >
                        <YBLabelWithIcon icon="fa fa-trash-o fa-fw">
                          Delete Universe
                        </YBLabelWithIcon>
                      </YBMenuItem>
                    </>
                  )}
                  subMenus={{
                    security: (backToMainMenu) => (
                      <>
                        <SecurityMenu
                          backToMainMenu={backToMainMenu}
                          showTLSConfigurationModal={showTLSConfigurationModal}
                          editTLSAvailability={editTLSAvailability}
                          showManageKeyModal={showManageKeyModal}
                          manageKeyAvailability={manageKeyAvailability}
                        />
                      </>
                    )
                  }}
                />
              </DropdownButton>
            </div>
          </div>
        )}
        <RollingUpgradeFormContainer
          modalVisible={
            showModal &&
            (visibleModal === 'gFlagsModal' ||
              visibleModal === 'softwareUpgradesModal' ||
              visibleModal === 'vmImageUpgradeModal' ||
              visibleModal === 'tlsConfigurationModal' ||
              visibleModal === 'rollingRestart' ||
              visibleModal === 'thirdpartyUpgradeModal' ||
              visibleModal === 'systemdUpgrade')
          }
          onHide={closeModal}
        />
        <DeleteUniverseContainer
          visible={showModal && visibleModal === 'deleteUniverseModal'}
          onHide={closeModal}
          title="Delete Universe: "
          body="Are you sure you want to delete the universe? You will lose all your data!"
          type="primary"
        />

        <ToggleUniverseStateContainer
          visible={showModal && visibleModal === 'toggleUniverseStateForm'}
          onHide={closeModal}
          title={`${!universePaused ? 'Pause' : 'Resume'} Universe: `}
          type="primary"
          universePaused={universePaused}
        />
        <ToggleBackupStateContainer
          visible={showModal && visibleModal === 'toggleBackupModalForm'}
          onHide={closeModal}
          universe={currentUniverse.data}
          type="primary"
        />
        <EncryptionKeyModalContainer
          modalVisible={showModal && visibleModal === 'manageKeyModal'}
          onHide={closeModal}
          handleSubmitKey={this.handleSubmitManageKey}
          currentUniverse={currentUniverse}
          name={currentUniverse.data.name}
          uuid={currentUniverse.data.universeUUID}
        />
        <Measure onMeasure={this.onResize.bind(this)}>
          <YBTabsWithLinksPanel
            defaultTab={defaultTab}
            activeTab={activeTab}
            routePrefix={`/universes/${currentUniverse.data.universeUUID}/`}
            id={'universe-tab-panel'}
            className="universe-detail"
          >
            {tabElements}
          </YBTabsWithLinksPanel>
        </Measure>
      </Grid>
    );
  }
}

export default withRouter(mouseTrap(UniverseDetail));

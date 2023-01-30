import React, { useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import { toast } from 'react-toastify';
import _ from 'lodash';
import { FormikActions, FormikErrors, FormikProps } from 'formik';
import axios, { AxiosError } from 'axios';

import {
  createXClusterReplication,
  fetchTablesInUniverse,
  fetchTaskUntilItCompletes,
  fetchUniverseDiskUsageMetric,
  isBootstrapRequired
} from '../../../actions/xClusterReplication';
import { PARALLEL_THREADS_RANGE } from '../../backupv2/common/BackupUtils';
import { YBModalForm } from '../../common/forms';
import { YBErrorIndicator, YBLoading } from '../../common/indicators';
import { adaptTableUUID, parseFloatIfDefined } from '../ReplicationUtils';
import { ConfigureBootstrapStep } from './ConfigureBootstrapStep';
import { SelectTargetUniverseStep } from './SelectTargetUniverseStep';
import { YBButton, YBModal } from '../../common/forms/fields';
import { api } from '../../../redesign/helpers/api';
import { getPrimaryCluster, isYbcEnabledUniverse } from '../../../utils/UniverseUtils';
import { assertUnreachableCase } from '../../../utils/ErrorUtils';
import {
  XCLUSTER_CONFIG_NAME_ILLEGAL_PATTERN,
  BOOTSTRAP_MIN_FREE_DISK_SPACE_GB,
  XClusterConfigAction
} from '../constants';
import { TableSelect } from '../common/tableSelect/TableSelect';

import { TableType, Universe, YBTable } from '../../../redesign/helpers/dtos';
import { XClusterTableType } from '../XClusterTypes';

import styles from './CreateConfigModal.module.scss';

export interface CreateXClusterConfigFormValues {
  configName: string;
  targetUniverse: { label: string; value: Universe };
  tableUUIDs: string[];
  // Bootstrap fields
  storageConfig: { label: string; name: string; regions: any[]; value: string };
  parallelThreads: number;
}

export interface CreateXClusterConfigFormErrors {
  configName: string;
  targetUniverse: string;
  tableUUIDs: { title: string; body: string };
  // Bootstrap fields
  storageConfig: string;
  parallelThreads: string;
}

export interface CreateXClusterConfigFormWarnings {
  configName?: string;
  targetUniverse?: string;
  tableUUIDs?: { title: string; body: string };
  // Bootstrap fields
  storageConfig?: string;
  parallelThreads?: string;
}

interface ConfigureReplicationModalProps {
  onHide: Function;
  visible: boolean;
  sourceUniverseUUID: string;
}

const MODAL_TITLE = 'Configure Replication';

export enum FormStep {
  SELECT_TARGET_UNIVERSE = 'selectTargetUniverse',
  SELECT_TABLES = 'selectTables',
  CONFIGURE_BOOTSTRAP = 'configureBootstrap'
}

const FIRST_FORM_STEP = FormStep.SELECT_TARGET_UNIVERSE;
const DEFAULT_TABLE_TYPE = TableType.PGSQL_TABLE_TYPE;

const INITIAL_VALUES: Partial<CreateXClusterConfigFormValues> = {
  configName: '',
  tableUUIDs: [],
  // Bootstrap fields
  parallelThreads: PARALLEL_THREADS_RANGE.MIN
};

export const CreateConfigModal = ({
  onHide,
  visible,
  sourceUniverseUUID
}: ConfigureReplicationModalProps) => {
  const [currentStep, setCurrentStep] = useState<FormStep>(FIRST_FORM_STEP);
  const [bootstrapRequiredTableUUIDs, setBootstrapRequiredTableUUIDs] = useState<string[]>([]);
  const [isTableSelectionValidated, setIsTableSelectionValidated] = useState(false);
  const [formWarnings, setFormWarnings] = useState<CreateXClusterConfigFormWarnings>({});

  // The purpose of committedTargetUniverse is to store the targetUniverse field value prior
  // to the user submitting their target universe step.
  // This value updates whenever the user submits SelectTargetUniverseStep with a new
  // target universe.
  const [committedTargetUniverseUUID, setCommittedTargetUniverseUUID] = useState<string>();

  // Need to store this in CreateConfigModal.tsx to support navigating back to this step.
  const [tableType, setTableType] = useState<XClusterTableType>(DEFAULT_TABLE_TYPE);
  const [selectedKeyspaces, setSelectedKeyspaces] = useState<string[]>([]);

  const queryClient = useQueryClient();
  const formik = useRef({} as FormikProps<CreateXClusterConfigFormValues>);

  const xClusterConfigMutation = useMutation(
    (values: CreateXClusterConfigFormValues) => {
      if (bootstrapRequiredTableUUIDs.length > 0) {
        const bootstrapParams = {
          tables: bootstrapRequiredTableUUIDs,
          backupRequestParams: {
            storageConfigUUID: values.storageConfig.value,
            parallelism: values.parallelThreads,
            sse: values.storageConfig.name === 'S3',
            universeUUID: null
          }
        };
        return createXClusterReplication(
          values.targetUniverse.value.universeUUID,
          sourceUniverseUUID,
          values.configName,
          values.tableUUIDs.map(adaptTableUUID),
          bootstrapParams
        );
      }
      return createXClusterReplication(
        values.targetUniverse.value.universeUUID,
        sourceUniverseUUID,
        values.configName,
        values.tableUUIDs.map(adaptTableUUID)
      );
    },
    {
      onSuccess: (response, values) => {
        closeModal();
        // This newly xCluster config will be added to sourceXClusterConfigs for the source universe and
        // to targetXClusterConfigs for the target universe.
        // Invalidate queries for the participating universes.
        queryClient.invalidateQueries(['universe', sourceUniverseUUID], { exact: true });
        queryClient.invalidateQueries(['universe', values.targetUniverse.value.universeUUID], {
          exact: true
        });
        fetchTaskUntilItCompletes(response.data.taskUUID, (err: boolean) => {
          if (err) {
            toast.error(
              <span className={styles.alertMsg}>
                <i className="fa fa-exclamation-circle" />
                <span>Replication creation failed.</span>
                <a
                  href={`/tasks/${response.data.taskUUID}`}
                  rel="noopener noreferrer"
                  target="_blank"
                >
                  View Details
                </a>
              </span>
            );
          }
          queryClient.invalidateQueries(['universe', sourceUniverseUUID], { exact: true });
        });
      },
      onError: (error: Error | AxiosError) => {
        if (axios.isAxiosError(error)) {
          toast.error(error.response?.data?.error?.message ?? error.message);
        } else {
          toast.error(error.message);
        }
      }
    }
  );

  const tablesQuery = useQuery<YBTable[]>(['universe', sourceUniverseUUID, 'tables'], () =>
    fetchTablesInUniverse(sourceUniverseUUID).then((response) => response.data)
  );

  const universeQuery = useQuery<Universe>(['universe', sourceUniverseUUID], () =>
    api.fetchUniverse(sourceUniverseUUID)
  );

  /**
   * Wrapper around setFieldValue from formik.
   * Reset `isTableSelectionValidated` to false if changing
   * a validated table selection.
   */
  const setSelectedTableUUIDs = (
    tableUUIDs: string[],
    formikActions: FormikActions<CreateXClusterConfigFormValues>
  ) => {
    if (isTableSelectionValidated) {
      setIsTableSelectionValidated(false);
    }
    formikActions.setFieldValue('tableUUIDs', tableUUIDs);
  };

  const resetModalState = () => {
    setCurrentStep(FIRST_FORM_STEP);
    setBootstrapRequiredTableUUIDs([]);
    setIsTableSelectionValidated(false);
    setFormWarnings({});
    setTableType(DEFAULT_TABLE_TYPE);
    setSelectedKeyspaces([]);
  };
  const closeModal = () => {
    resetModalState();
    onHide();
  };

  /**
   * Wrapper around setFieldValue from formik.
   * Reset `isTableSelectionValidated` to false if changing
   * a validated table selection.
   */
  const setTableUUIDs = (
    tableUUIDs: string[],
    formikActions: FormikActions<CreateXClusterConfigFormValues>
  ) => {
    const { setFieldValue } = formikActions;
    if (isTableSelectionValidated) {
      setIsTableSelectionValidated(false);
    }
    setFieldValue('tableUUIDs', tableUUIDs);
  };

  const resetTableSelection = (formikActions: FormikActions<CreateXClusterConfigFormValues>) => {
    setTableUUIDs([], formikActions);
    setSelectedKeyspaces([]);
    setFormWarnings((formWarnings) => {
      const { tableUUIDs, ...newformWarnings } = formWarnings;
      return newformWarnings;
    });
  };

  const isBootstrapStepRequired = bootstrapRequiredTableUUIDs.length > 0;
  const handleFormSubmit = (
    values: CreateXClusterConfigFormValues,
    actions: FormikActions<CreateXClusterConfigFormValues>
  ) => {
    switch (currentStep) {
      case FormStep.SELECT_TARGET_UNIVERSE:
        if (values.targetUniverse.value.universeUUID !== committedTargetUniverseUUID) {
          // Reset table selection when changing target universe.
          // This is because the current table selection may be invalid for
          // the new target universe.
          resetTableSelection(actions);
          setCommittedTargetUniverseUUID(values.targetUniverse.value.universeUUID);
        }
        setCurrentStep(FormStep.SELECT_TABLES);
        actions.setSubmitting(false);
        return;
      case FormStep.SELECT_TABLES:
        if (!isTableSelectionValidated) {
          // Validation in validateForm just passed.
          setIsTableSelectionValidated(true);
          actions.setSubmitting(false);
          return;
        }

        // Table selection has already been validated.
        if (isBootstrapStepRequired) {
          setCurrentStep(FormStep.CONFIGURE_BOOTSTRAP);
          actions.setSubmitting(false);
          return;
        }
        xClusterConfigMutation.mutate(values, { onSettled: () => actions.setSubmitting(false) });
        return;
      case FormStep.CONFIGURE_BOOTSTRAP:
        xClusterConfigMutation.mutate(values, { onSettled: () => actions.setSubmitting(false) });
        return;
      default:
        assertUnreachableCase(currentStep);
    }
  };

  const handleBackNavigation = (currentStep: Exclude<FormStep, typeof FIRST_FORM_STEP>) => {
    switch (currentStep) {
      case FormStep.SELECT_TABLES:
        setCurrentStep(FormStep.SELECT_TARGET_UNIVERSE);
        return;
      case FormStep.CONFIGURE_BOOTSTRAP:
        setCurrentStep(FormStep.SELECT_TABLES);
        return;
      default:
        assertUnreachableCase(currentStep);
    }
  };

  const submitLabel = getFormSubmitLabel(
    currentStep,
    isBootstrapStepRequired,
    isTableSelectionValidated
  );
  if (
    tablesQuery.isLoading ||
    tablesQuery.isIdle ||
    universeQuery.isLoading ||
    universeQuery.isIdle
  ) {
    return (
      <YBModal
        size="large"
        title={MODAL_TITLE}
        visible={visible}
        onHide={() => {
          closeModal();
        }}
        submitLabel={submitLabel}
      >
        <YBLoading />
      </YBModal>
    );
  }

  if (tablesQuery.isError || universeQuery.isError) {
    return (
      <YBModal
        size="large"
        title={MODAL_TITLE}
        visible={visible}
        onHide={() => {
          closeModal();
        }}
      >
        <YBErrorIndicator />
      </YBModal>
    );
  }

  return (
    <YBModalForm
      size="large"
      title={MODAL_TITLE}
      visible={visible}
      validate={(values: CreateXClusterConfigFormValues) =>
        validateForm(
          values,
          currentStep,
          universeQuery.data,
          isTableSelectionValidated,
          setBootstrapRequiredTableUUIDs,
          setFormWarnings
        )
      }
      // Perform validation for select table when user submits.
      validateOnChange={currentStep !== FormStep.SELECT_TABLES}
      validateOnBlur={currentStep !== FormStep.SELECT_TABLES}
      onFormSubmit={handleFormSubmit}
      initialValues={INITIAL_VALUES}
      submitLabel={submitLabel}
      onHide={() => {
        closeModal();
      }}
      footerAccessory={
        currentStep === FIRST_FORM_STEP ? (
          <YBButton btnClass="btn" btnText={'Cancel'} onClick={closeModal} />
        ) : (
          <YBButton
            btnClass="btn"
            btnText={'Back'}
            onClick={() => handleBackNavigation(currentStep)}
          />
        )
      }
      render={(formikProps: FormikProps<CreateXClusterConfigFormValues>) => {
        // workaround for outdated version of Formik to access form methods outside of <Formik>
        formik.current = formikProps;

        if (
          tablesQuery.isLoading ||
          tablesQuery.isIdle ||
          universeQuery.isLoading ||
          universeQuery.isIdle
        ) {
          return <YBLoading />;
        }

        if (tablesQuery.isError || universeQuery.isError) {
          return <YBErrorIndicator />;
        }

        switch (currentStep) {
          case FormStep.SELECT_TARGET_UNIVERSE:
            return (
              <SelectTargetUniverseStep
                {...{
                  formik,
                  currentUniverseUUID: sourceUniverseUUID
                }}
              />
            );
          case FormStep.SELECT_TABLES: {
            // Casting because FormikValues and FormikError have different types.
            const errors = formik.current.errors as FormikErrors<CreateXClusterConfigFormErrors>;
            const { values } = formik.current;
            return (
              <>
                <div className={styles.formInstruction}>
                  {`2. Select the tables you want to add to the xCluster configuration.`}
                </div>
                <TableSelect
                  {...{
                    configAction: XClusterConfigAction.CREATE,
                    sourceUniverseUUID: sourceUniverseUUID,
                    targetUniverseUUID: values.targetUniverse.value.universeUUID,
                    currentStep,
                    setCurrentStep,
                    selectedTableUUIDs: values.tableUUIDs,
                    setSelectedTableUUIDs: (tableUUIDs: string[]) =>
                      setSelectedTableUUIDs(tableUUIDs, formik.current),
                    tableType: tableType,
                    isFixedTableType: false,
                    setTableType,
                    selectedKeyspaces,
                    setSelectedKeyspaces,
                    selectionError: errors.tableUUIDs,
                    selectionWarning: formWarnings?.tableUUIDs
                  }}
                />
              </>
            );
          }
          case FormStep.CONFIGURE_BOOTSTRAP:
            return (
              <ConfigureBootstrapStep
                {...{
                  formik,
                  bootstrapRequiredTableUUIDs,
                  sourceTables: tablesQuery.data
                }}
              />
            );
          default:
            return assertUnreachableCase(currentStep);
        }
      }}
    />
  );
};

const validateForm = async (
  values: CreateXClusterConfigFormValues,
  currentStep: FormStep,
  sourceUniveres: Universe,
  isTableSelectionValidated: boolean,
  setBootstrapRequiredTableUUIDs: (tableUUIDs: string[]) => void,
  setFormWarnings: (formWarnings: CreateXClusterConfigFormWarnings) => void
) => {
  // Since our formik verision is < 2.0 , we need to throw errors instead of
  // returning them in custom async validation:
  // https://github.com/jaredpalmer/formik/issues/1392#issuecomment-606301031

  switch (currentStep) {
    case FormStep.SELECT_TARGET_UNIVERSE: {
      const errors: Partial<CreateXClusterConfigFormErrors> = {};

      if (!values.configName) {
        errors.configName = 'Replication name is required.';
      } else if (XCLUSTER_CONFIG_NAME_ILLEGAL_PATTERN.test(values.configName)) {
        errors.configName =
          "The name of the replication configuration cannot contain any characters in [SPACE '_' '*' '<' '>' '?' '|' '\"' NULL])";
      }

      if (!values.targetUniverse) {
        errors.targetUniverse = 'Target universe is required.';
      } else if (
        getPrimaryCluster(values.targetUniverse.value.universeDetails.clusters)?.userIntent
          ?.enableNodeToNodeEncrypt !==
        getPrimaryCluster(sourceUniveres?.universeDetails.clusters)?.userIntent
          ?.enableNodeToNodeEncrypt
      ) {
        errors.targetUniverse =
          'The target universe must have the same Encryption in-Transit (TLS) configuration as the source universe. Edit the TLS configuration to proceed.';
      } else if (
        !_.isEqual(
          values.targetUniverse?.value?.universeDetails?.encryptionAtRestConfig,
          sourceUniveres?.universeDetails?.encryptionAtRestConfig
        )
      ) {
        errors.targetUniverse =
          'The target universe must have the same key management system (KMS) configuration as the source universe. Edit the KMS configuration to proceed.';
      }

      throw errors;
    }
    case FormStep.SELECT_TABLES: {
      const errors: Partial<CreateXClusterConfigFormErrors> = {};
      const warnings: CreateXClusterConfigFormWarnings = {};
      if (!isTableSelectionValidated) {
        if (!values.tableUUIDs || values.tableUUIDs.length === 0) {
          errors.tableUUIDs = {
            title: 'No tables selected.',
            body: 'Select at least 1 table to proceed'
          };
        }
        // Check if bootstrap is required, for each selected table
        let bootstrapTests: { [tableUUID: string]: boolean }[] | null = null;
        try {
          bootstrapTests = await isBootstrapRequired(
            sourceUniveres.universeUUID,
            values.tableUUIDs.map(adaptTableUUID)
          );
        } catch (error) {
          toast.error(
            <span className={styles.alertMsg}>
              <div>
                <i className="fa fa-exclamation-circle" />
                <span>Table bootstrap verification failed.</span>
              </div>
              <div>
                An error occured while verifying whether the selected tables require bootstrapping:
              </div>
              <div>{error.message}</div>
            </span>
          );
          errors.tableUUIDs = {
            title: 'Table bootstrap verification error',
            body:
              'An error occured while verifying whether the selected tables require bootstrapping.'
          };
        }
        if (bootstrapTests !== null) {
          const bootstrapTableUUIDs = bootstrapTests.reduce(
            (bootstrapTableUUIDs: string[], bootstrapTest) => {
              // Each bootstrapTest response is of the form {<tableUUID>: boolean}.
              // Until the backend supports multiple tableUUIDs per request, the response object
              // will only contain one tableUUID.
              // Note: Once backend does support multiple tableUUIDs per request, we will replace this
              //       logic with one that simply filters on the keys (tableUUIDs) of the returned object.
              const tableUUID = Object.keys(bootstrapTest)[0];

              if (bootstrapTest[tableUUID]) {
                bootstrapTableUUIDs.push(tableUUID);
              }
              return bootstrapTableUUIDs;
            },
            []
          );
          setBootstrapRequiredTableUUIDs(bootstrapTableUUIDs);

          // If some tables require bootstrapping, we need to validate the source universe has enough
          // disk space.
          if (bootstrapTableUUIDs.length > 0) {
            // Disk space validation
            const currentUniverseNodePrefix = sourceUniveres.universeDetails.nodePrefix;
            const diskUsageMetric = await fetchUniverseDiskUsageMetric(currentUniverseNodePrefix);
            const freeSpaceTrace = diskUsageMetric.disk_usage.data.find(
              (trace) => trace.name === 'free'
            );
            const freeDiskSpace = parseFloatIfDefined(
              freeSpaceTrace?.y[freeSpaceTrace.y.length - 1]
            );

            if (freeDiskSpace !== undefined && freeDiskSpace < BOOTSTRAP_MIN_FREE_DISK_SPACE_GB) {
              warnings.tableUUIDs = {
                title: 'Insufficient disk space.',
                body: `Some selected tables require bootstrapping. We recommend having at least ${BOOTSTRAP_MIN_FREE_DISK_SPACE_GB} GB of free disk space in the source universe.`
              };
            }
          }
        }
        setFormWarnings(warnings);
      }
      throw errors;
    }
    case FormStep.CONFIGURE_BOOTSTRAP: {
      const errors: Partial<CreateXClusterConfigFormErrors> = {};
      if (!values.storageConfig) {
        errors.storageConfig = 'Backup storage configuration is required.';
      }
      const shouldValidateParallelThread =
        values.parallelThreads && isYbcEnabledUniverse(sourceUniveres?.universeDetails);
      if (shouldValidateParallelThread && values.parallelThreads > PARALLEL_THREADS_RANGE.MAX) {
        errors.parallelThreads = `Parallel threads must be less than or equal to ${PARALLEL_THREADS_RANGE.MAX}`;
      } else if (
        shouldValidateParallelThread &&
        values.parallelThreads < PARALLEL_THREADS_RANGE.MIN
      ) {
        errors.parallelThreads = `Parallel threads must be greater than or equal to ${PARALLEL_THREADS_RANGE.MIN}`;
      }

      throw errors;
    }
    default:
      return {};
  }
};

const getFormSubmitLabel = (
  formStep: FormStep,
  bootstrapRequired: boolean,
  validTableSelection: boolean
) => {
  switch (formStep) {
    case FormStep.SELECT_TARGET_UNIVERSE:
      return 'Next: Select Tables';
    case FormStep.SELECT_TABLES:
      if (!validTableSelection) {
        return 'Validate Table Selection';
      }
      if (bootstrapRequired) {
        return 'Next: Configure Bootstrap';
      }
      return 'Enable Replication';
    case FormStep.CONFIGURE_BOOTSTRAP:
      return 'Bootstrap and Enable Replication';
    default:
      return assertUnreachableCase(formStep);
  }
};

/*
 * Created on Thu Sep 01 2022
 *
 * Copyright 2021 YugaByte, Inc. and Contributors
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License")
 * You may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

import { Field, FormikProps } from 'formik';
import React, { FC, useState } from 'react';
import { Col, Row } from 'react-bootstrap';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import {
  fetchThrottleParameters,
  resetThrottleParameterToDefaults,
  setThrottleParameters
} from '..';
import { YBModalForm } from '../../common/forms';
import { YBButton, YBControlledNumericInput } from '../../common/forms/fields';
import { YBLoading } from '../../common/indicators';
import { ThrottleParameters } from '../common/IBackup';
import * as Yup from 'yup';

import { toast } from 'react-toastify';
import { createErrorMessage } from '../../../utils/ObjectUtils';
import { useSelector } from 'react-redux';
import { YBConfirmModal } from '../../modals';
import { YBTag, YBTag_Types } from '../../common/YBTag';
import './BackupThrottleParameters.scss';

interface BackupThrottleParametersProps {
  visible: boolean;
  onHide: () => void;
  currentUniverseUUID: string;
}

export const BackupThrottleParameters: FC<BackupThrottleParametersProps> = ({
  visible,
  onHide,
  currentUniverseUUID
}) => {
  const currentUniverseResources = useSelector(
    (state: any) => state.universe?.currentUniverse?.data?.resources
  );

  const [showRestoreDefaultModal, setShowRestoreDefaultModal] = useState(false);

  const queryClient = useQueryClient();

  const { data: throttleParameters, isLoading } = useQuery(
    ['throttle_parameters', currentUniverseUUID],
    () => fetchThrottleParameters(currentUniverseUUID),
    {
      enabled: visible,
      onError: () => {
        toast.error('Unable to fetch throttle parameter configurations!.');
        onHide();
      }
    }
  );

  const configurehrottleParameters = useMutation(
    (values: ThrottleParameters) => setThrottleParameters(currentUniverseUUID, values),
    {
      onSuccess: () => {
        toast.success(`Parameters updated successfully!.`);
        queryClient.invalidateQueries(['throttle_parameters', currentUniverseUUID]);
        onHide();
      },
      onError: (err: any) => {
        toast.error(createErrorMessage(err));
      }
    }
  );

  const resetParameters = useMutation(() => resetThrottleParameterToDefaults(currentUniverseUUID), {
    onSuccess: () => {
      toast.success(`Parameters restored to defaults`);
      queryClient.invalidateQueries(['throttle_parameters', currentUniverseUUID]);
      onHide();
    },
    onError: (err: any) => {
      toast.error(createErrorMessage(err));
    }
  });

  if (!visible) {
    return null;
  }

  if (isLoading) {
    return <YBLoading />;
  }

  const initialValues: ThrottleParameters = {
    ...throttleParameters!.data
  };

  const noOfCoresPerNode = currentUniverseResources.numCores / currentUniverseResources.numNodes;
  const defaultBufferPerNode = noOfCoresPerNode / 2 + 1;

  const min_buffers_allowed = 1;

  const validationSchema = Yup.object().shape({
    max_concurrent_uploads: Yup.number()
      .required('Required')
      .typeError('Required')
      .min(min_buffers_allowed, 'Min limit is 1')
      .max(noOfCoresPerNode, `Max limit is ${noOfCoresPerNode}`),
    per_upload_num_objects: Yup.number()
      .required('Required')
      .typeError('Required')
      .min(min_buffers_allowed, 'Min Limit is 1')
      .max(noOfCoresPerNode, `Max limit is ${noOfCoresPerNode}`),
    max_concurrent_downloads: Yup.number()
      .required('Required')
      .typeError('Required')
      .min(min_buffers_allowed, 'Min limit is 1')
      .max(noOfCoresPerNode, `Max limit is ${noOfCoresPerNode}`),
    per_download_num_objects: Yup.number()
      .required('Required')
      .typeError('Required')
      .min(min_buffers_allowed, 'Min Limit is 1')
      .max(noOfCoresPerNode, `Max limit is ${noOfCoresPerNode}`)
  });

  return (
    <>
      <YBModalForm
        visible={visible}
        onHide={() => {
          onHide();
        }}
        title="Configure Resource Throttling"
        initialValues={initialValues}
        validationSchema={validationSchema}
        footerAccessory={
          <YBButton
            btnText="Reset to defaults"
            btnClass="btn"
            onClick={(e: any) => {
              e.preventDefault();
              setShowRestoreDefaultModal(true);
            }}
          />
        }
        showCancelButton
        dialogClassName="throttle-parameters-modal"
        submitLabel="Save"
        onFormSubmit={(values: ThrottleParameters, formik: FormikProps<ThrottleParameters>) => {
          const { setSubmitting } = formik;
          setSubmitting(false);
          for (const prop in values) {
            values[prop] = parseInt(values[prop]);
          }
          configurehrottleParameters.mutateAsync(values);
        }}
        render={(formikProps: FormikProps<ThrottleParameters>) => {
          const { values, setFieldValue, errors } = formikProps;
          return (
            <>
              <Row>
                <Col lg={12} className="no-padding infos">
                  <div>
                    Manage the speed of Backup and Restore operations by configuring resource
                    throttling.
                  </div>
                  <div>
                    For <b>faster</b> backups and restores, enter higher values.
                    <YBTag type={YBTag_Types.YB_GRAY}>Max {noOfCoresPerNode}</YBTag>
                  </div>
                  <div>
                    For <b>lower impact</b> on database performance, enter lower values.
                    <YBTag type={YBTag_Types.YB_GRAY}>Min {min_buffers_allowed}</YBTag>
                  </div>
                </Col>
                <Col lg={12} className="fields">
                  <div className="section">Backups</div>
                  <Row>
                    <Col lg={12} className="no-padding">
                      Number of parallel uploads per node{' '}
                      <span className="text-secondary">- Default 2</span>
                    </Col>
                    <Col lg={2} className="no-padding">
                      <Field
                        name="max_concurrent_uploads"
                        component={YBControlledNumericInput}
                        val={values.max_concurrent_uploads}
                        onInputChanged={(val: number) =>
                          setFieldValue('max_concurrent_uploads', val)
                        }
                      />
                      {errors.max_concurrent_uploads && (
                        <span className="err-msg">{errors.max_concurrent_uploads}</span>
                      )}
                    </Col>
                  </Row>
                  <Row>
                    <Col lg={12} className="no-padding">
                      Number of buffers per upload per node{' '}
                      <span className="text-secondary">- Default {defaultBufferPerNode}</span>
                    </Col>
                    <Col lg={2} className="no-padding">
                      <Field
                        name="per_upload_num_objects"
                        component={YBControlledNumericInput}
                        val={values.per_upload_num_objects}
                        onInputChanged={(val: number) =>
                          setFieldValue('per_upload_num_objects', val)
                        }
                      />
                      {errors.per_upload_num_objects && (
                        <span className="err-msg">{errors.per_upload_num_objects}</span>
                      )}
                    </Col>
                  </Row>
                </Col>
              </Row>
              <Row>
                <Col lg={12} className="fields">
                  <div className="section">Restores</div>
                  <Row>
                    <Col lg={12} className="no-padding">
                      Number of parallel downloads per node{' '}
                      <span className="text-secondary">- Default 2</span>
                    </Col>
                    <Col lg={2} className="no-padding">
                      <Field
                        name="max_concurrent_downloads"
                        component={YBControlledNumericInput}
                        val={values.max_concurrent_downloads}
                        onInputChanged={(val: number) =>
                          setFieldValue('max_concurrent_downloads', val)
                        }
                      />
                      {errors.max_concurrent_downloads && (
                        <span className="err-msg">{errors.max_concurrent_downloads}</span>
                      )}
                    </Col>
                  </Row>
                  <Row>
                    <Col lg={12} className="no-padding">
                      Number of buffers per download per node{' '}
                      <span className="text-secondary">- Default {defaultBufferPerNode}</span>
                    </Col>
                    <Col lg={2} className="no-padding">
                      <Field
                        name="per_download_num_objects"
                        component={YBControlledNumericInput}
                        val={values.per_download_num_objects}
                        onInputChanged={(val: number) =>
                          setFieldValue('per_download_num_objects', val)
                        }
                      />
                      {errors.per_download_num_objects && (
                        <span className="err-msg">{errors.per_download_num_objects}</span>
                      )}
                    </Col>
                  </Row>
                </Col>
              </Row>
            </>
          );
        }}
      />
      <YBConfirmModal
        name="throttle-parameters-config"
        title="Confirm Reset to defaults"
        visibleModal={showRestoreDefaultModal}
        currentModal={true}
        onConfirm={() => resetParameters.mutateAsync()}
        hideConfirmModal={() => setShowRestoreDefaultModal(false)}
      >
        <div>Are you sure you want to restore the configurations to these default values?</div>
        <br />
        <h5>Backup</h5>
        <div>
          Number of parallel uploads (per node) = <b>2</b>
        </div>
        <div>
          Number of buffers per upload (per node) = <b>{defaultBufferPerNode}</b>
        </div>
        <br />
        <h5>Restore</h5>
        <div>
          Number of parallel downloads (per node) = <b>2</b>
        </div>
        <div>
          Number of buffers per download (per node) = <b>{defaultBufferPerNode}</b>
        </div>
      </YBConfirmModal>
    </>
  );
};

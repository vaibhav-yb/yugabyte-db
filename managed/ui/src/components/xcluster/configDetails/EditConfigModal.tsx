import * as Yup from 'yup';
import { Field } from 'formik';
import { useMutation, useQueryClient } from 'react-query';
import { AxiosError } from 'axios';

import { editXclusterName } from '../../../actions/xClusterReplication';
import { YBModalForm } from '../../common/forms';
import { XClusterConfig } from '../dtos';
import { YBFormInput } from '../../common/forms/fields';
import { XCLUSTER_CONFIG_NAME_ILLEGAL_PATTERN } from '../constants';
import { handleServerError } from '../../../utils/errorHandlingUtils';
import { xClusterQueryKey } from '../../../redesign/helpers/api';

interface Props {
  visible: boolean;
  onHide: () => void;
  xClusterConfig: XClusterConfig;
}
const validationSchema = Yup.object().shape({
  name: Yup.string()
    .required('Replication name is required')
    .test(
      'Should not contain illegal characters',
      "The name of the replication configuration cannot contain any characters in [SPACE '_' '*' '<' '>' '?' '|' '\"' NULL])",
      (value) =>
        value !== null && value !== undefined && !XCLUSTER_CONFIG_NAME_ILLEGAL_PATTERN.test(value)
    )
});
export function EditConfigModal({ onHide, visible, xClusterConfig }: Props) {
  const queryClient = useQueryClient();
  const initialValues: any = { ...xClusterConfig };

  const modifyXclusterOperation = useMutation(
    (values: XClusterConfig) => {
      return editXclusterName(values);
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries(xClusterQueryKey.detail(xClusterConfig.uuid));
        onHide();
      },
      onError: (error: Error | AxiosError) =>
        handleServerError(error, { customErrorLabel: 'Create xCluster config request failed' })
    }
  );

  return (
    <YBModalForm
      size="large"
      title="Edit Replication Name"
      visible={visible}
      onHide={onHide}
      validationSchema={validationSchema}
      onFormSubmit={(values: any, { setSubmitting }: { setSubmitting: any }) => {
        modifyXclusterOperation
          .mutateAsync(values)
          .then(() => {
            setSubmitting(false);
            onHide();
          })
          .catch(() => {
            setSubmitting(false);
          });
      }}
      initialValues={initialValues}
      submitLabel="Apply Changes"
      showCancelButton
      render={() => {
        return (
          <Field
            name="name"
            placeholder="Replication name"
            label="Replication Name"
            component={YBFormInput}
          />
        );
      }}
    />
  );
}

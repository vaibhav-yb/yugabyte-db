import { FC, useEffect, useRef } from 'react';
import { useQuery } from 'react-query';
import { useUpdateEffect } from 'react-use';
import { useTranslation } from 'react-i18next';
import { Controller, useFormContext, useWatch } from 'react-hook-form';
import { Box, Grid, MenuItem, makeStyles } from '@material-ui/core';
import { YBInput, YBLabel, YBSelect } from '../../../../../../components';
import { api, QUERY_KEY } from '../../../utils/api';
import {
  getStorageTypeOptions,
  getDeviceInfoFromInstance,
  getMinDiskIops,
  getMaxDiskIops,
  getIopsByStorageType,
  getThroughputByStorageType,
  getThroughputByIops,
  useVolumeControls
} from './VolumeInfoFieldHelper';
import { isEphemeralAwsStorageInstance } from '../InstanceTypeField/InstanceTypeFieldHelper';
import {
  CloudType,
  MasterPlacementMode,
  StorageType,
  UniverseFormData,
  VolumeType
} from '../../../utils/dto';
import {
  PROVIDER_FIELD,
  DEVICE_INFO_FIELD,
  MASTER_DEVICE_INFO_FIELD,
  INSTANCE_TYPE_FIELD,
  MASTER_INSTANCE_TYPE_FIELD,
  MASTER_PLACEMENT_FIELD
} from '../../../utils/constants';

interface VolumeInfoFieldProps {
  isEditMode: boolean;
  isPrimary: boolean;
  disableIops: boolean;
  disableThroughput: boolean;
  disableStorageType: boolean;
  disableVolumeSize: boolean;
  disableNumVolumes: boolean;
  isDedicatedMasterField?: boolean;
  maxVolumeCount: number;
}

const useStyles = makeStyles((theme) => ({
  volumeInfoTextField: {
    width: theme.spacing(15.5)
  },
  storageTypeLabelField: {
    minWidth: theme.spacing(21.25)
  },
  storageTypeSelectField: {
    maxWidth: theme.spacing(35.25),
    minWidth: theme.spacing(30)
  },
  unitLabelField: {
    marginLeft: theme.spacing(2),
    alignSelf: 'center'
  }
}));

export const VolumeInfoField: FC<VolumeInfoFieldProps> = ({
  isEditMode,
  isPrimary,
  disableIops,
  disableThroughput,
  disableStorageType,
  disableVolumeSize,
  disableNumVolumes,
  isDedicatedMasterField,
  maxVolumeCount
}) => {
  const { control, setValue } = useFormContext<UniverseFormData>();
  const classes = useStyles();
  const { t } = useTranslation();
  const instanceTypeChanged = useRef(false);
  const dataTag = isDedicatedMasterField ? 'Master' : 'TServer';
  const { numVolumesDisable, volumeSizeDisable, minVolumeSize } = useVolumeControls(isEditMode);
  //watchers
  const fieldValue = isDedicatedMasterField
    ? useWatch({ name: MASTER_DEVICE_INFO_FIELD })
    : useWatch({ name: DEVICE_INFO_FIELD });
  const instanceType = isDedicatedMasterField
    ? useWatch({ name: MASTER_INSTANCE_TYPE_FIELD })
    : useWatch({ name: INSTANCE_TYPE_FIELD });
  const masterPlacement = useWatch({ name: MASTER_PLACEMENT_FIELD });
  const provider = useWatch({ name: PROVIDER_FIELD });

  //fetch run time configs
  const {
    data: providerRuntimeConfigs,
    refetch: providerConfigsRefetch
  } = useQuery(QUERY_KEY.fetchProviderRunTimeConfigs, () =>
    api.fetchRunTimeConfigs(true, provider?.uuid)
  );

  // Update field is based on master or tserver field in dedicated mode
  const UPDATE_FIELD = isDedicatedMasterField ? MASTER_DEVICE_INFO_FIELD : DEVICE_INFO_FIELD;

  //get instance details
  const { data: instanceTypes } = useQuery(
    [QUERY_KEY.getInstanceTypes, provider?.uuid],
    () => api.getInstanceTypes(provider?.uuid),
    { enabled: !!provider?.uuid }
  );
  const instance = instanceTypes?.find((item) => item.instanceTypeCode === instanceType);

  //update volume info after istance changes
  useEffect(() => {
    if (!instance) return;
    const getProviderRuntimeConfigs = async () => {
      const providerRuntimeConfigsRefetch = await providerConfigsRefetch();
      let deviceInfo = getDeviceInfoFromInstance(
        instance,
        providerRuntimeConfigsRefetch.isError
          ? providerRuntimeConfigs
          : providerRuntimeConfigsRefetch.data
      );

      //retain old volume size if its edit mode or not ephemeral storage
      if (
        fieldValue &&
        deviceInfo &&
        !isEphemeralAwsStorageInstance(instance) &&
        isEditMode &&
        provider.code !== CloudType.onprem
      ) {
        deviceInfo.volumeSize = fieldValue.volumeSize;
        deviceInfo.numVolumes = fieldValue.numVolumes;
      }

      setValue(UPDATE_FIELD, deviceInfo ?? null);
    };
    getProviderRuntimeConfigs();
  }, [instanceType, provider?.uuid]);

  //mark instance changed once only in edit mode
  useUpdateEffect(() => {
    if (isEditMode) instanceTypeChanged.current = true;
  }, [instanceType]);

  const convertToString = (str: string) => str?.toString() ?? '';

  //reset methods
  const resetThroughput = () => {
    const { storageType, throughput, diskIops, volumeSize } = fieldValue;
    if ([StorageType.IO1, StorageType.GP3, StorageType.UltraSSD_LRS].includes(storageType)) {
      //resetting throughput
      const throughputVal = getThroughputByIops(Number(throughput), diskIops, storageType);
      setValue(UPDATE_FIELD, {
        ...fieldValue,
        throughput: throughputVal,
        volumeSize: volumeSize < minVolumeSize ? minVolumeSize : volumeSize
      });
    } else
      setValue(UPDATE_FIELD, {
        ...fieldValue,
        volumeSize: volumeSize < minVolumeSize ? minVolumeSize : volumeSize
      });
  };

  //field actions
  const onStorageTypeChanged = (storageType: StorageType) => {
    const throughput = getThroughputByStorageType(storageType);
    const diskIops = getIopsByStorageType(storageType);
    setValue(UPDATE_FIELD, { ...fieldValue, throughput, diskIops, storageType });
  };

  const onVolumeSizeChanged = (value: any) => {
    const { storageType, diskIops } = fieldValue;
    setValue(UPDATE_FIELD, {
      ...fieldValue,
      volumeSize: Number(value)
    });
    if (storageType === StorageType.UltraSSD_LRS) {
      onDiskIopsChanged(diskIops);
    }
  };

  const onDiskIopsChanged = (value: any) => {
    const { storageType, volumeSize } = fieldValue;
    const maxDiskIops = getMaxDiskIops(storageType, volumeSize);
    const minDiskIops = getMinDiskIops(storageType, volumeSize);
    const diskIops = Math.max(minDiskIops, Math.min(maxDiskIops, Number(value)));
    setValue(UPDATE_FIELD, { ...fieldValue, diskIops });
  };

  const onThroughputChange = (value: any) => {
    const { storageType, diskIops } = fieldValue;
    const throughput = getThroughputByIops(Number(value), diskIops, storageType);
    setValue(UPDATE_FIELD, { ...fieldValue, throughput });
  };

  const onNumVolumesChanged = (numVolumes: any) => {
    const volumeCount = Number(numVolumes) > maxVolumeCount ? maxVolumeCount : Number(numVolumes);
    setValue(UPDATE_FIELD, { ...fieldValue, numVolumes: volumeCount });
  };

  //render
  if (!instance) return null;

  const { volumeDetailsList } = instance?.instanceTypeDetails;
  const { volumeType } = volumeDetailsList[0];

  if (![VolumeType.EBS, VolumeType.SSD, VolumeType.NVME].includes(volumeType)) return null;

  const renderVolumeInfo = () => {
    const fixedVolumeSize =
      [VolumeType.SSD, VolumeType.NVME].includes(volumeType) &&
      fieldValue?.storageType === StorageType.Scratch &&
      ![CloudType.kubernetes, CloudType.azu].includes(provider?.code);

    const fixedNumVolumes =
      [VolumeType.SSD, VolumeType.NVME].includes(volumeType) &&
      ![CloudType.kubernetes, CloudType.gcp, CloudType.azu].includes(provider?.code);

    const smartResizePossible =
      [CloudType.aws, CloudType.gcp, CloudType.azu].includes(provider?.code) &&
      !isEphemeralAwsStorageInstance(instance) &&
      fieldValue?.storageType !== StorageType.Scratch &&
      isPrimary;

    return (
      <Grid container spacing={2}>
        <Grid item lg={6}>
          <Box mt={2}>
            <Box display="flex">
              <Box display="flex">
                <YBLabel dataTestId="VolumeInfoField-Label">
                  {t('universeForm.instanceConfig.volumeInfo')}
                </YBLabel>
              </Box>

              <Box display="flex">
                <Box flex={1} className={classes.volumeInfoTextField}>
                  <YBInput
                    type="number"
                    fullWidth
                    disabled={fixedNumVolumes || disableNumVolumes || numVolumesDisable}
                    inputProps={{ min: 1, 'data-testid': `VolumeInfoField-${dataTag}-VolumeInput` }}
                    value={convertToString(fieldValue.numVolumes)}
                    onChange={(event) => onNumVolumesChanged(event.target.value)}
                    inputMode="numeric"
                  />
                </Box>

                <Box display="flex" alignItems="center" px={1} flexShrink={1}>
                  x
                </Box>

                <Box flex={1} className={classes.volumeInfoTextField}>
                  <YBInput
                    type="number"
                    fullWidth
                    disabled={
                      fixedVolumeSize ||
                      disableVolumeSize ||
                      (provider?.code !== CloudType.kubernetes &&
                        !smartResizePossible &&
                        isEditMode &&
                        !instanceTypeChanged.current) ||
                      volumeSizeDisable
                    }
                    inputProps={{
                      min: 1,
                      'data-testid': `VolumeInfoField-${dataTag}-VolumeSizeInput`
                    }}
                    value={convertToString(fieldValue.volumeSize)}
                    onChange={(event) => onVolumeSizeChanged(event.target.value)}
                    onBlur={resetThroughput}
                    inputMode="numeric"
                  />
                </Box>
                <span className={classes.unitLabelField}>
                  {provider?.code === CloudType.kubernetes
                    ? t('universeForm.instanceConfig.k8VolumeSizeUnit')
                    : t('universeForm.instanceConfig.volumeSizeUnit')}
                </span>
              </Box>
            </Box>
          </Box>
        </Grid>
      </Grid>
    );
  };

  const renderStorageType = () => {
    if (
      [CloudType.gcp, CloudType.azu].includes(provider?.code) ||
      (volumeType === VolumeType.EBS && provider?.code === CloudType.aws)
    )
      return (
        <Grid container spacing={2}>
          <Grid item lg={6}>
            <Box mt={1}>
              <Box display="flex">
                <YBLabel
                  dataTestId="VolumeInfoField-StorageTypeLabel"
                  className={classes.storageTypeLabelField}
                >
                  {provider?.code === CloudType.aws
                    ? t('universeForm.instanceConfig.ebs')
                    : t('universeForm.instanceConfig.ssd')}
                </YBLabel>
                <Box flex={1}>
                  <YBSelect
                    className={classes.storageTypeSelectField}
                    disabled={disableStorageType}
                    value={fieldValue.storageType}
                    inputProps={{
                      min: 1,
                      'data-testid': `VolumeInfoField-${dataTag}-StorageTypeSelect`
                    }}
                    onChange={(event) =>
                      onStorageTypeChanged((event?.target.value as unknown) as StorageType)
                    }
                  >
                    {getStorageTypeOptions(provider?.code).map((item) => (
                      <MenuItem key={item.value} value={item.value}>
                        {item.label}
                      </MenuItem>
                    ))}
                  </YBSelect>
                </Box>
              </Box>
            </Box>
          </Grid>
        </Grid>
      );

    return null;
  };

  const renderDiskIops = () => {
    if (
      ![StorageType.IO1, StorageType.GP3, StorageType.UltraSSD_LRS].includes(fieldValue.storageType)
    )
      return null;

    return (
      <Grid container spacing={2}>
        <Grid item lg={6}>
          <Box display="flex" mt={2}>
            <YBLabel dataTestId="VolumeInfoField-DiskIopsLabel">
              {t('universeForm.instanceConfig.provisionedIops')}
            </YBLabel>
            <Box flex={1}>
              <YBInput
                type="number"
                fullWidth
                disabled={disableIops}
                inputProps={{ min: 1, 'data-testid': `VolumeInfoField-${dataTag}-DiskIopsInput` }}
                value={convertToString(fieldValue.diskIops)}
                onChange={(event) => onDiskIopsChanged(event.target.value)}
                onBlur={resetThroughput}
                inputMode="numeric"
              />
            </Box>
          </Box>
        </Grid>
      </Grid>
    );
  };

  const renderThroughput = () => {
    if (![StorageType.GP3, StorageType.UltraSSD_LRS].includes(fieldValue.storageType)) return null;
    return (
      <Grid container spacing={2}>
        <Grid item lg={6}>
          <Box display="flex" mt={1}>
            <YBLabel dataTestId="VolumeInfoField-ThroughputLabel">
              {t('universeForm.instanceConfig.provisionedThroughput')}
            </YBLabel>
            <Box flex={1}>
              <YBInput
                type="number"
                fullWidth
                disabled={disableThroughput}
                inputProps={{ min: 1, 'data-testid': `VolumeInfoField-${dataTag}-ThroughputInput` }}
                value={convertToString(fieldValue.throughput)}
                onChange={(event) => onThroughputChange(event.target.value)}
                inputMode="numeric"
              />
            </Box>
            <span className={classes.unitLabelField}>
              {t('universeForm.instanceConfig.throughputUnit')}
            </span>
          </Box>
        </Grid>
      </Grid>
    );
  };

  return (
    <Controller
      control={control}
      name={UPDATE_FIELD}
      render={() => (
        <>
          {fieldValue && (
            <Box display="flex" width="100%" flexDirection="column">
              <Box>
                {renderVolumeInfo()}
                {!(
                  provider?.code === CloudType.gcp &&
                  masterPlacement === MasterPlacementMode.DEDICATED
                ) && <>{renderStorageType()}</>}
              </Box>

              {fieldValue.storageType && (
                <Box>
                  {renderDiskIops()}
                  {renderThroughput()}
                </Box>
              )}
            </Box>
          )}
        </>
      )}
    />
  );
};

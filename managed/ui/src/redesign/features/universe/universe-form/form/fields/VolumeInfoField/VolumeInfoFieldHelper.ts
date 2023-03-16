import { useRef, useState } from 'react';
import _ from 'lodash';
import { useUpdateEffect } from 'react-use';
import { useWatch, useFormContext } from 'react-hook-form';
import {
  PLACEMENTS_FIELD,
  TOTAL_NODES_FIELD,
  INSTANCE_TYPE_FIELD,
  DEVICE_INFO_FIELD
} from '../../../utils/constants';
import {
  CloudType,
  DeviceInfo,
  InstanceType,
  RunTimeConfigEntry,
  StorageType,
  UniverseFormData
} from '../../../utils/dto';
import { isEphemeralAwsStorageInstance } from '../InstanceTypeField/InstanceTypeFieldHelper';

export const IO1_DEFAULT_DISK_IOPS = 1000;
export const IO1_MAX_DISK_IOPS = 64000;

export const GP3_DEFAULT_DISK_IOPS = 3000;
export const GP3_MAX_IOPS = 16000;
export const GP3_DEFAULT_DISK_THROUGHPUT = 125;
export const GP3_MAX_THROUGHPUT = 1000;
export const GP3_IOPS_TO_MAX_DISK_THROUGHPUT = 4;

export const UltraSSD_DEFAULT_DISK_IOPS = 3000;
export const UltraSSD_DEFAULT_DISK_THROUGHPUT = 125;
export const UltraSSD_MIN_DISK_IOPS = 100;
export const UltraSSD_DISK_IOPS_MAX_PER_GB = 300;
export const UltraSSD_IOPS_TO_MAX_DISK_THROUGHPUT = 4;
export const UltraSSD_DISK_THROUGHPUT_CAP = 2500;

export interface StorageTypeOption {
  value: StorageType;
  label: string;
}

export const DEFAULT_STORAGE_TYPES = {
  [CloudType.aws]: StorageType.GP3,
  [CloudType.gcp]: StorageType.Persistent,
  [CloudType.azu]: StorageType.Premium_LRS
};

export const AWS_STORAGE_TYPE_OPTIONS: StorageTypeOption[] = [
  { value: StorageType.IO1, label: 'IO1' },
  { value: StorageType.GP2, label: 'GP2' },
  { value: StorageType.GP3, label: 'GP3' }
];

export const GCP_STORAGE_TYPE_OPTIONS: StorageTypeOption[] = [
  { value: StorageType.Persistent, label: 'Persistent' },
  { value: StorageType.Scratch, label: 'Local Scratch' }
];

export const AZURE_STORAGE_TYPE_OPTIONS: StorageTypeOption[] = [
  { value: StorageType.StandardSSD_LRS, label: 'Standard' },
  { value: StorageType.Premium_LRS, label: 'Premium' },
  { value: StorageType.UltraSSD_LRS, label: 'Ultra' }
];

export const getMinDiskIops = (storageType: StorageType, volumeSize: number) => {
  return storageType === StorageType.UltraSSD_LRS
    ? Math.max(UltraSSD_MIN_DISK_IOPS, volumeSize)
    : 0;
};

export const getMaxDiskIops = (storageType: StorageType, volumeSize: number) => {
  switch (storageType) {
    case StorageType.IO1:
      return IO1_MAX_DISK_IOPS;
    case StorageType.UltraSSD_LRS:
      return volumeSize * UltraSSD_DISK_IOPS_MAX_PER_GB;
    default:
      return GP3_MAX_IOPS;
  }
};

export const getStorageTypeOptions = (providerCode?: CloudType): StorageTypeOption[] => {
  switch (providerCode) {
    case CloudType.aws:
      return AWS_STORAGE_TYPE_OPTIONS;
    case CloudType.gcp:
      return GCP_STORAGE_TYPE_OPTIONS;
    case CloudType.azu:
      return AZURE_STORAGE_TYPE_OPTIONS;
    default:
      return [];
  }
};

export const getIopsByStorageType = (storageType: StorageType) => {
  if (storageType === StorageType.IO1) {
    return IO1_DEFAULT_DISK_IOPS;
  } else if (storageType === StorageType.GP3) {
    return GP3_DEFAULT_DISK_IOPS;
  } else if (storageType === StorageType.UltraSSD_LRS) {
    return UltraSSD_DEFAULT_DISK_IOPS;
  }
  return null;
};

export const getThroughputByStorageType = (storageType: StorageType) => {
  if (storageType === StorageType.GP3) {
    return GP3_DEFAULT_DISK_THROUGHPUT;
  } else if (storageType === StorageType.UltraSSD_LRS) {
    return UltraSSD_DEFAULT_DISK_THROUGHPUT;
  }
  return null;
};

export const getThroughputByIops = (
  currentThroughput: number,
  diskIops: number,
  storageType: StorageType
) => {
  if (storageType === StorageType.GP3) {
    if (
      (diskIops > GP3_DEFAULT_DISK_IOPS || currentThroughput > GP3_DEFAULT_DISK_THROUGHPUT) &&
      diskIops / currentThroughput < GP3_IOPS_TO_MAX_DISK_THROUGHPUT
    ) {
      return Math.min(
        GP3_MAX_THROUGHPUT,
        Math.max(diskIops / GP3_IOPS_TO_MAX_DISK_THROUGHPUT, GP3_DEFAULT_DISK_THROUGHPUT)
      );
    }
  } else if (storageType === StorageType.UltraSSD_LRS) {
    const maxThroughput = Math.min(
      diskIops / UltraSSD_IOPS_TO_MAX_DISK_THROUGHPUT,
      UltraSSD_DISK_THROUGHPUT_CAP
    );
    return Math.max(0, Math.min(maxThroughput, currentThroughput));
  }

  return currentThroughput;
};

const getVolumeSize = (instance: InstanceType, providerRuntimeConfigs: any) => {
  let volumeSize = null;

  if (instance.providerCode === CloudType.aws) {
    volumeSize = providerRuntimeConfigs?.configEntries?.find(
      (c: RunTimeConfigEntry) => c.key === 'yb.aws.default_volume_size_gb'
    )?.value;
  } else if (instance.providerCode === CloudType.gcp) {
    volumeSize = providerRuntimeConfigs?.configEntries?.find(
      (c: RunTimeConfigEntry) => c.key === 'yb.gcp.default_volume_size_gb'
    )?.value;
  } else if (instance.providerCode === CloudType.kubernetes) {
    volumeSize = providerRuntimeConfigs?.configEntries?.find(
      (c: RunTimeConfigEntry) => c.key === 'yb.kubernetes.default_volume_size_gb'
    )?.value;
  } else if (instance.providerCode === CloudType.azu) {
    volumeSize = providerRuntimeConfigs?.configEntries?.find(
      (c: RunTimeConfigEntry) => c.key === 'yb.azure.default_volume_size_gb'
    )?.value;
  }
  return volumeSize;
};

const getStorageType = (instance: InstanceType, providerRuntimeConfigs: any) => {
  let storageType = null;
  if (isEphemeralAwsStorageInstance(instance))
    //aws ephemeral storage
    return storageType;

  if (instance.providerCode === CloudType.aws) {
    storageType = providerRuntimeConfigs?.configEntries?.find(
      (c: RunTimeConfigEntry) => c.key === 'yb.aws.storage.default_storage_type'
    )?.value;
  } else if (instance.providerCode === CloudType.gcp) {
    storageType = providerRuntimeConfigs?.configEntries?.find(
      (c: RunTimeConfigEntry) => c.key === 'yb.gcp.storage.default_storage_type'
    )?.value;
  } else if (instance.providerCode === CloudType.azu) {
    storageType = providerRuntimeConfigs?.configEntries?.find(
      (c: RunTimeConfigEntry) => c.key === 'yb.azure.storage.default_storage_type'
    )?.value;
  }
  return storageType;
};

export const getDeviceInfoFromInstance = (
  instance: InstanceType,
  providerRuntimeConfigs: any
): DeviceInfo | null => {
  if (!instance.instanceTypeDetails.volumeDetailsList.length) return null;

  const { volumeDetailsList } = instance.instanceTypeDetails;
  const volumeSize = volumeDetailsList[0].volumeSizeGB;
  const defaultInstanceVolumeSize = isEphemeralAwsStorageInstance(instance)
    ? volumeSize
    : getVolumeSize(instance, providerRuntimeConfigs);
  const storageType = getStorageType(instance, providerRuntimeConfigs);

  return {
    numVolumes: volumeDetailsList.length,
    volumeSize: defaultInstanceVolumeSize ?? volumeSize,
    storageClass: 'standard',
    storageType,
    mountPoints:
      instance.providerCode === CloudType.onprem
        ? volumeDetailsList.flatMap((item) => item.mountPath).join(',')
        : null,
    diskIops: getIopsByStorageType(storageType),
    throughput: getThroughputByStorageType(storageType)
  };
};

export const useVolumeControls = (isEditMode: boolean) => {
  const [numVolumesDisable, setNumVolumesDisable] = useState(isEditMode ? true : false);
  const [volumeSizeDisable, setVolumeSizeDisable] = useState(false);
  const [userTagsDisable, setUserTagsDisable] = useState(false);
  const [minVolumeSize, setMinVolumeSize] = useState(1);
  const { setValue } = useFormContext<UniverseFormData>();

  //watchers
  const totalNodes = useWatch({ name: TOTAL_NODES_FIELD });
  const placements = useWatch({ name: PLACEMENTS_FIELD });
  const instanceType = useWatch({ name: INSTANCE_TYPE_FIELD });
  const deviceInfo = useWatch({ name: DEVICE_INFO_FIELD });

  const initialCombination = useRef({
    totalNodes: Number(totalNodes),
    placements,
    instanceType,
    deviceInfo
  });

  useUpdateEffect(() => {
    if (isEditMode) {
      if (
        !_.isEqual(initialCombination.current.instanceType, instanceType) ||
        _.intersectionBy(initialCombination.current.placements, placements, 'name').length <= 0
      ) {
        //Enable numVolumes and volumeSize when instancetype is updated
        setMinVolumeSize(1);
        setNumVolumesDisable(false);
        setVolumeSizeDisable(false);
        setUserTagsDisable(false);
      } else if (!_.isEqual(initialCombination.current.totalNodes, Number(totalNodes))) {
        //On total nodes changed
        //Disable numVolumes and volumeSize when Total Nodes is updated
        setValue(DEVICE_INFO_FIELD, initialCombination.current.deviceInfo);
        setNumVolumesDisable(true);
        setVolumeSizeDisable(true);
        setUserTagsDisable(false);
      } else if (!_.isEqual(initialCombination.current.placements, placements)) {
        setValue(DEVICE_INFO_FIELD, initialCombination.current.deviceInfo);
        setNumVolumesDisable(true);
        setVolumeSizeDisable(true);
        setUserTagsDisable(true);
      } else {
        //Smart Resize/Resize disk
        setMinVolumeSize(initialCombination.current.deviceInfo.volumeSize);
        setNumVolumesDisable(true);
        setVolumeSizeDisable(false);
        setUserTagsDisable(true);
      }
    }
  }, [totalNodes, placements, instanceType, deviceInfo?.volumeSize]);

  return { numVolumesDisable, volumeSizeDisable, userTagsDisable, minVolumeSize };
};

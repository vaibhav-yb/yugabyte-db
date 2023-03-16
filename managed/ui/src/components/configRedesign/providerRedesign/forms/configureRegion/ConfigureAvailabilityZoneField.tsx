/*
 * Copyright 2022 YugaByte, Inc. and Contributors
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License")
 * You may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */
import React from 'react';
import clsx from 'clsx';
import { makeStyles } from '@material-ui/core';
import { useFieldArray, useFormContext } from 'react-hook-form';

import { YBButton } from '../../../../common/forms/fields';
import { YBInputField } from '../../../../../redesign/components';
import { ConfigureRegionFormValues } from './ConfigureRegionModal';
import { YBReactSelectField } from '../../components/YBReactSelect/YBReactSelectField';

import { CloudVendorAvailabilityZoneMutation } from '../../types';

interface ConfigureAvailabilityZoneFieldProps {
  isSubmitting: boolean;
  zoneCodeOptions: string[] | undefined;
  className?: string;
}

export type ExposedAZProperties = Pick<CloudVendorAvailabilityZoneMutation, 'code' | 'subnet'>;

const useStyles = makeStyles((theme) => ({
  zonesContainer: {
    display: 'flex',
    flexDirection: 'column',
    gap: theme.spacing(1),

    marginTop: theme.spacing(1)
  },
  zoneConfigContainer: {
    display: 'flex',
    gap: theme.spacing(1)
  },
  removeZoneButton: {
    backgroundColor: 'red',
    padding: theme.spacing(1, 2, 1, 2),
    '& i': {
      color: 'white'
    },
    maxHeight: '42px'
  }
}));

export const ConfigureAvailabilityZoneField = ({
  isSubmitting,
  zoneCodeOptions,
  className
}: ConfigureAvailabilityZoneFieldProps) => {
  const classes = useStyles();
  const { control, watch } = useFormContext<ConfigureRegionFormValues>();
  const { fields, append, remove } = useFieldArray({ control, name: 'zones' });
  const addZoneField = () => {
    append({ code: { value: '', label: '', isDisabled: true }, subnet: '' });
  };

  const zones = watch('zones');
  const selectZoneCodeOptions = zoneCodeOptions?.map((zoneCode) => ({
    value: zoneCode,
    label: zoneCode,
    isDisabled: zones?.find((zone) => zone.code.value === zoneCode) !== undefined
  }));
  return (
    <div className={clsx(className)}>
      <YBButton
        btnIcon="fa fa-plus"
        btnText="Add Zone"
        btnClass="btn btn-default"
        btnType="button"
        onClick={addZoneField}
        disabled={isSubmitting || zoneCodeOptions === undefined}
        data-testid="ConfigureAvailabilityZonField-AddZoneButton"
      />
      <div className={classes.zonesContainer}>
        {fields.map((zone, index) => (
          <div key={zone.id} className={classes.zoneConfigContainer}>
            <YBReactSelectField
              control={control}
              name={`zones.${index}.code`}
              options={selectZoneCodeOptions}
            />
            <YBInputField
              control={control}
              name={`zones.${index}.subnet`}
              placeholder="Enter..."
              fullWidth
            />
            <YBButton
              className={classes.removeZoneButton}
              btnIcon="fa fa-trash-o"
              btnType="button"
              onClick={() => remove(index)}
            />
          </div>
        ))}
      </div>
    </div>
  );
};

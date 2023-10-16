/*
 * Created on Fri Feb 03 2023
 *
 * Copyright 2021 YugaByte, Inc. and Contributors
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License")
 * You may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

import { FC } from 'react';
import moment from 'moment-timezone';
import { useSelector } from 'react-redux';

export const YBTimeFormats = {
  YB_DEFAULT_TIMESTAMP: 'MMM-DD-YYYY HH:mm:ss [UTC]ZZ',
  YB_DATE_ONLY_TIMESTAMP: 'MMM-DD-YYYY',
  YB_HOURS_FIRST_TIMESTAMP: 'HH:mm:ss MMM-DD-YYYY [UTC]ZZ',
  YB_ISO8601_TIMESTAMP: 'YYYY-MM-DD[T]H:mm:ssZZ'
} as const;
export type YBTimeFormats = typeof YBTimeFormats[keyof typeof YBTimeFormats];
/**
 * Converts date to RFC3339 format("yyyy-MM-dd'T'HH:mm:ss'Z'")
 * @param d Date
 * @returns RFC3339 format string
 */
export const convertToISODateString = (d: Date) => {
  const pad = (n: number) => {
    return n < 10 ? '0' + n : n;
  };
  try {
    return (
      d.getUTCFullYear() +
      '-' +
      pad(d.getUTCMonth() + 1) +
      '-' +
      pad(d.getUTCDate()) +
      'T' +
      pad(d.getUTCHours()) +
      ':' +
      pad(d.getUTCMinutes()) +
      ':' +
      pad(d.getUTCSeconds()) +
      'Z'
    );
  } catch (e) {
    console.error(e);
    return '-';
  }
};

/**
 * Format the provided datetime string using one of our standard YBA datetime formats.
 */
export const formatDatetime = (
  date: moment.MomentInput,
  timeFormat: YBTimeFormats = YBTimeFormats.YB_DEFAULT_TIMESTAMP,
  timezone?: string
): string => {
  return timezone ? moment(date).tz(timezone).format(timeFormat) : moment(date).format(timeFormat);
};

type FormatDateProps = {
  date: Date | string | number;
  timeFormat: YBTimeFormats;
};

export const YBFormatDate: FC<FormatDateProps> = ({ date, timeFormat }) => {
  const currentUserTimezone = useSelector((state: any) => state.customer?.currentUser?.data?.timezone);
  return <>{formatDatetime(date, timeFormat, currentUserTimezone)}</>;
};

export const ybFormatDate = (
  date: Date | string | number,
  timeFormat = YBTimeFormats.YB_DEFAULT_TIMESTAMP
) => {
  return <YBFormatDate date={date} timeFormat={timeFormat} />;
};

export const dateStrToMoment = (str: string) => {
  return moment(str);
};

/*
 * Created on Tue Aug 16 2022
 *
 * Copyright 2021 YugaByte, Inc. and Contributors
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License")
 * You may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

import axios from 'axios';
import { ROOT_URL } from '../../../config';
import { TIME_RANGE_STATE } from './IBackup';

export function getRestoreList(
  page = 0,
  limit = 10,
  searchText: string,
  timeRange: TIME_RANGE_STATE,
  states: any[],
  sortBy: string,
  direction: string,
  moreFilters: any[] | undefined,
  universeUUID?: string,
  storageConfigUUID?: string | null
) {
  const payload = {
    sortBy,
    direction,
    filter: {},
    limit,
    offset: page,
    needTotalCount: true
  };
  if (searchText) {
    payload['filter'] = {
      universeNameList: [searchText]
    };
  }
  if (universeUUID) {
    payload['filter']['universeUUIDList'] = [universeUUID];
  }

  if (storageConfigUUID) {
    payload['filter']['storageConfigUUIDList'] = [storageConfigUUID];
  }

  if (states.length !== 0 && states[0].label !== 'All') {
    payload.filter['states'] = [states[0].value];
  }
  if (timeRange.startTime && timeRange.endTime) {
    payload.filter['dateRangeStart'] = timeRange.startTime.toISOString();
    payload.filter['dateRangeEnd'] = timeRange.endTime.toISOString();
  }

  if (Array.isArray(moreFilters) && moreFilters?.length > 0) {
    payload.filter[moreFilters[0].value] = true;
  }

  const cUUID = localStorage.getItem('customerId');
  const requestUrl = `${ROOT_URL}/customers/${cUUID}/restore/page`;
  return axios.post(requestUrl, payload);
}

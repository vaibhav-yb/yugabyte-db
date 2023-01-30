/*
 * Created on Tue Aug 16 2022
 *
 * Copyright 2021 YugaByte, Inc. and Contributors
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License")
 * You may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */
import React, { FC, useReducer, useState } from 'react';
import moment from 'moment';
import { RemoteObjSpec, SortOrder, TableHeaderColumn } from 'react-bootstrap-table';
import { useQuery } from 'react-query';
import Select, { OptionTypeBase } from 'react-select';
import { StatusBadge } from '../../common/badge/StatusBadge';
import { YBMultiSelectRedesiged } from '../../common/forms/fields';
import { YBSearchInput } from '../../common/forms/fields/YBSearchInput';
import { YBLoading } from '../../common/indicators';
import { YBTable } from '../../common/YBTable';
import { formatBytes } from '../../xcluster/ReplicationUtils';
import {
  CALDENDAR_ICON,
  DATE_FORMAT,
  ENTITY_NOT_AVAILABLE,
  FormatUnixTimeStampTimeToTimezone
} from '../common/BackupUtils';
import { TIME_RANGE_STATE } from '../common/IBackup';
import { IRestore, RESTORE_STATUS_OPTIONS } from '../common/IRestore';
import { getRestoreList } from '../common/RestoreAPI';
import { DEFAULT_TIME_STATE, TIME_RANGE_OPTIONS } from '../components/BackupList';
import { RestoreEmpty } from './RestoreEmpty';
import './Restore.scss';

const reactWidgets = require('react-widgets');
const momentLocalizer = require('react-widgets-moment');
require('react-widgets/dist/css/react-widgets.css');

const { DateTimePicker } = reactWidgets;
momentLocalizer(moment);

const DEFAULT_SORT_COLUMN = 'createTime';
const DEFAULT_SORT_DIRECTION = 'DESC';

interface RestoreProps {
  type: 'UNIVERSE_LEVEL' | 'ACCOUNT_LEVEL';
  universeUUID?: string;
}

export const Restore: FC<RestoreProps> = ({ universeUUID, type }) => {
  const [sizePerPage, setSizePerPage] = useState(10);
  const [page, setPage] = useState(1);
  const [searchText, setSearchText] = useState('');
  const [status, setStatus] = useState<any[]>([RESTORE_STATUS_OPTIONS[0]]);
  const [sortDirection, setSortDirection] = useState(DEFAULT_SORT_DIRECTION);

  const [customStartTime, setCustomStartTime] = useState<Date | undefined>();
  const [customEndTime, setCustomEndTime] = useState<Date | undefined>();

  const timeReducer = (_state: TIME_RANGE_STATE, action: OptionTypeBase) => {
    if (action.label === 'Custom') {
      return { startTime: customStartTime, endTime: customEndTime, label: action.label };
    }
    if (action.label === 'All time') {
      return { startTime: null, endTime: null, label: action.label };
    }

    return {
      label: action.label,
      startTime: moment().subtract(action.value[0], action.value[1]),
      endTime: new Date()
    };
  };

  const [timeRange, dispatchTimeRange] = useReducer(timeReducer, DEFAULT_TIME_STATE);

  const { data: restoreList, isLoading } = useQuery(
    [
      'restore',
      (page - 1) * sizePerPage,
      sizePerPage,
      searchText,
      timeRange,
      status,
      DEFAULT_SORT_COLUMN,
      sortDirection,
      [],
      universeUUID
    ],
    () =>
      getRestoreList(
        (page - 1) * sizePerPage,
        sizePerPage,
        searchText,
        timeRange,
        status,
        DEFAULT_SORT_COLUMN,
        sortDirection,
        [],
        universeUUID
      )
  );

  const isFilterApplied = () => {
    return (
      searchText.length !== 0 ||
      status[0].value !== null ||
      timeRange.startTime !== null ||
      timeRange.endTime !== null
    );
  };

  if (!isFilterApplied() && restoreList?.data.entities?.length === 0) {
    return <RestoreEmpty />;
  }

  return (
    <div className="restore-v2">
      <div className="search-filter-options">
        <div className="search-placeholder">
          <YBSearchInput
            placeHolder="Search universe name"
            onEnterPressed={(val: string) => setSearchText(val)}
          />
        </div>
        <YBMultiSelectRedesiged
          className="backup-status-filter"
          name="statuses"
          customLabel="Status:"
          placeholder="Status"
          isMulti={false}
          options={RESTORE_STATUS_OPTIONS}
          value={status}
          onChange={(value: any) => {
            setStatus(value ? [value] : []);
          }}
        />
        <div className="date-filters no-padding">
          {timeRange.label === 'Custom' && (
            <div className="custom-date-picker">
              <DateTimePicker
                placeholder="Pick a start time"
                step={10}
                formats={DATE_FORMAT}
                onChange={(time: Date) => {
                  setCustomStartTime(time);
                  dispatchTimeRange({
                    label: 'Custom'
                  });
                }}
              />
              <span>-</span>
              <DateTimePicker
                placeholder="Pick a end time"
                step={10}
                formats={DATE_FORMAT}
                onChange={(time: Date) => {
                  setCustomEndTime(time);
                  dispatchTimeRange({
                    label: 'Custom'
                  });
                }}
              />
            </div>
          )}
          <Select
            className="time-range"
            options={TIME_RANGE_OPTIONS}
            onChange={(value) => {
              dispatchTimeRange({
                ...value
              });
            }}
            styles={{
              input: (styles) => {
                return { ...styles, ...CALDENDAR_ICON() };
              },
              placeholder: (styles) => ({ ...styles, ...CALDENDAR_ICON() }),
              singleValue: (styles) => ({ ...styles, ...CALDENDAR_ICON() }),
              menu: (styles) => ({
                ...styles,
                zIndex: 10,
                height: '325px'
              }),
              menuList: (base) => ({
                ...base,
                minHeight: '325px'
              })
            }}
            defaultValue={TIME_RANGE_OPTIONS.find((t) => t.label === 'All time')}
            maxMenuHeight={300}
          ></Select>
        </div>
      </div>
      {isLoading && <YBLoading />}
      <YBTable
        data={restoreList?.data.entities ?? []}
        options={{
          sizePerPage,
          onSizePerPageList: setSizePerPage,
          page,
          prePage: 'Prev',
          nextPage: 'Next',
          onPageChange: (page) => setPage(page),
          defaultSortOrder: DEFAULT_SORT_DIRECTION.toLowerCase() as SortOrder,
          defaultSortName: DEFAULT_SORT_COLUMN,
          onSortChange: (_: any, SortOrder: SortOrder) => setSortDirection(SortOrder.toUpperCase())
        }}
        pagination={true}
        remote={(remoteObj: RemoteObjSpec) => {
          return {
            ...remoteObj,
            pagination: true
          };
        }}
        fetchInfo={{ dataTotalSize: restoreList?.data.length ?? 10 }}
        hover
      >
        <TableHeaderColumn dataField="restoreUUID" isKey={true} hidden={true} />

        <TableHeaderColumn
          dataField="createTime"
          dataFormat={(time) => <FormatUnixTimeStampTimeToTimezone timestamp={time} />}
          width="20%"
          dataSort
        >
          Taken At
        </TableHeaderColumn>
        <TableHeaderColumn
          dataField="restoreSizeInBytes"
          dataFormat={(_, row) => {
            return row.restoreSizeInBytes ? formatBytes(row.restoreSizeInBytes) : '-';
          }}
          width="20%"
        >
          Size
        </TableHeaderColumn>
        <TableHeaderColumn
          dataField="sourceUniverseName"
          dataFormat={(sourceName) => (sourceName ? sourceName : ENTITY_NOT_AVAILABLE)}
          width="20%"
        >
          Backup Source Universe
        </TableHeaderColumn>
        <TableHeaderColumn
          dataField="targetUniverseName"
          dataFormat={(_name, row: IRestore) =>
            row.targetUniverseName ? row.targetUniverseName : ENTITY_NOT_AVAILABLE
          }
          width="20%"
          hidden={type === 'UNIVERSE_LEVEL'}
        >
          Target Universe
        </TableHeaderColumn>
        <TableHeaderColumn
          dataField="state"
          dataFormat={(state) => {
            return <StatusBadge statusType={state} customLabel={state} />;
          }}
          width="15%"
        >
          Status
        </TableHeaderColumn>
      </YBTable>
    </div>
  );
};

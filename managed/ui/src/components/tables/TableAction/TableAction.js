// Copyright (c) YugaByte, Inc.

import { Component, Fragment } from 'react';
import PropTypes from 'prop-types';
import {
  BulkImportContainer,
  CreateBackupContainer,
  RestoreBackupContainer,
  DeleteBackupContainer,
  StopBackupContainer
} from '../../../components/tables';
import { ImportReleaseContainer, UpdateReleaseContainer } from '../../../components/releases';
import { ReleaseStateEnum } from '../../releases/UpdateRelease/UpdateRelease';
import { MenuItem } from 'react-bootstrap';
import { YBLabelWithIcon } from '../../common/descriptors';
import { YBButton } from '../../common/forms/fields';
import _ from 'lodash';
import { BackupCreateModal } from '../../backupv2/components/BackupCreateModal';
import { TableTypeLabel } from '../../../redesign/helpers/dtos';
import { BACKUP_API_TYPES, Backup_Options_Type } from '../../backupv2';
import { RbacValidator, hasNecessaryPerm } from '../../../redesign/features/rbac/common/RbacApiPermValidator';
import { ApiPermissionMap } from '../../../redesign/features/rbac/ApiAndUserPermMapping';

export default class TableAction extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showModal: false
    };
    this.openModal = this.openModal.bind(this);
    this.closeModal = this.closeModal.bind(this);
  }

  static propTypes = {
    currentRow: PropTypes.object,
    isMenuItem: PropTypes.bool,
    btnClass: PropTypes.string,
    onModalSubmit: PropTypes.func,
    onSubmit: PropTypes.func,
    onError: PropTypes.func,
    actionType: PropTypes.oneOf([
      'drop',
      'import',
      'create-backup',
      'create-scheduled-backup',
      'restore-backup',
      'import-release',
      'active-release',
      'disable-release',
      'delete-release',
      'delete-backup'
    ])
  };

  static defaultProps = {
    isMenuItem: true,
    btnClass: 'btn-default'
  };

  openModal() {
    this.setState((prevState, props) => {
      return {
        selectedRow: props.currentRow,
        showModal: true
      };
    });
  }

  closeModal() {
    this.setState(() => {
      return {
        showModal: false
      };
    });
  }

  render() {
    const { actionType, isMenuItem, disabled, onSubmit, onError } = this.props;
    let modalContainer = null;
    let btnLabel = null;
    let btnIcon = null;
    if (actionType === 'import') {
      btnLabel = 'Bulk Import';
      btnIcon = 'fa fa-download';
      modalContainer = (
        <BulkImportContainer
          visible={this.state.showModal}
          onHide={this.closeModal}
          tableInfo={this.state.selectedRow}
        />
      );
    } else if (actionType === 'create-scheduled-backup') {
      btnLabel = 'Create Scheduled Backup';
      btnIcon = 'fa fa-calendar-o';
      modalContainer = (
        <CreateBackupContainer
          visible={this.state.showModal}
          onHide={this.closeModal}
          tableInfo={this.state.selectedRow}
          onSubmit={onSubmit}
          onError={onError}
          isScheduled
        />
      );
    } else if (actionType === 'create-backup') {
      btnLabel = 'Create Backup';
      btnIcon = 'fa fa-upload';
      modalContainer = (
        <BackupCreateModal
          visible={this.state.showModal}
          onHide={this.closeModal}
          currentUniverseUUID={this.props.universeUUID}
          editValues={{
            api_type: {
              value: this.state.selectedRow?.tableType,
              label: TableTypeLabel[this.state.selectedRow?.tableType]
            },
            db_to_backup: {
              value: this.state.selectedRow?.keySpace,
              label: this.state.selectedRow?.keySpace
            },
            selected_ycql_tables:
              this.state.selectedRow?.tableType !== BACKUP_API_TYPES.YSQL
                ? [{ ...this.state.selectedRow, tableUUID: this.state.selectedRow?.tableID }]
                : [],
            backup_tables: Backup_Options_Type.CUSTOM
          }}
        />
      );
    } else if (actionType === 'stop-backup') {
      btnLabel = 'Abort Backup';
      btnIcon = 'fa fa-ban';
      modalContainer = (
        <StopBackupContainer
          visible={this.state.showModal}
          onHide={this.closeModal}
          tableInfo={this.state.selectedRow}
          onSubmit={onSubmit}
          onError={onError}
        />
      );
    } else if (actionType === 'restore-backup') {
      btnLabel = 'Restore Backup';
      btnIcon = 'fa fa-download';
      modalContainer = (
        <RestoreBackupContainer
          visible={this.state.showModal}
          onHide={this.closeModal}
          backupInfo={this.state.selectedRow}
          onSubmit={onSubmit}
          onError={onError}
        />
      );
    } else if (actionType === 'import-release') {
      btnLabel = 'Import';
      btnIcon = 'fa fa-upload';
      modalContainer = (
        <ImportReleaseContainer
          visible={this.state.showModal}
          onHide={this.closeModal}
          onModalSubmit={onSubmit}
        />
      );
    } else if (actionType === 'delete-backup') {
      btnLabel = 'Delete Backup';
      btnIcon = 'fa fa-trash';
      modalContainer = (
        <DeleteBackupContainer
          visible={this.state.showModal}
          onHide={this.closeModal}
          tableInfo={this.state.selectedRow}
          onSubmit={onSubmit}
          onError={onError}
        />
      );
    } else if (['disable-release', 'delete-release', 'active-release'].includes(actionType)) {
      let action;
      switch (actionType) {
        case 'disable-release':
          btnLabel = 'Disable';
          btnIcon = 'fa fa-ban';
          action = ReleaseStateEnum.DISABLED;
          break;
        case 'delete-release':
          btnLabel = 'Delete';
          btnIcon = 'fa fa-trash';
          action = ReleaseStateEnum.DELETED;
          break;
        case 'active-release':
          btnLabel = 'Active';
          btnIcon = 'fa fa-check';
          action = ReleaseStateEnum.ACTIVE;
          break;
        default:
          break;
      }
      modalContainer = (
        <UpdateReleaseContainer
          visible={this.state.showModal}
          onHide={this.closeModal}
          releaseInfo={this.state.selectedRow}
          actionType={action}
          onModalSubmit={this.props.onModalSubmit}
        />
      );
    }

    const getRbacWrappedComp = (hasPerm) => {
      return (
        <RbacValidator customValidateFunction={() => hasPerm} isControl overrideStyle={{ display: 'block' }}>
          <Fragment>
            <MenuItem eventKey={btnId} onClick={!hasPerm ? null : this.openModal} disabled={!hasPerm}>
              <YBLabelWithIcon icon={btnIcon}>{btnLabel}</YBLabelWithIcon>
            </MenuItem>
            {modalContainer}
          </Fragment>
        </RbacValidator>
      );
    };



    const btnId = _.uniqueId('table_action_btn_');

    if (actionType === 'create-backup') {
      return getRbacWrappedComp(hasNecessaryPerm({ ...ApiPermissionMap.CREATE_BACKUP, onResource: this.props.universeUUID }));
    }

    if (actionType === 'import') {
      return getRbacWrappedComp(hasNecessaryPerm({ ...ApiPermissionMap.BULK_IMPORT_TABLES, onResource: this.props.universeUUID }));
    }

    if (isMenuItem) {
      return (
        <Fragment>
          <MenuItem eventKey={btnId} onClick={disabled ? null : this.openModal} disabled={disabled}>
            <YBLabelWithIcon icon={btnIcon}>{btnLabel}</YBLabelWithIcon>
          </MenuItem>
          {modalContainer}
        </Fragment>
      );
    }
    return (
      <div className={this.props.className}>
        <YBButton
          btnText={btnLabel}
          btnIcon={btnIcon}
          disabled={disabled}
          btnClass={'btn ' + this.props.btnClass}
          onClick={disabled ? null : this.openModal}
        />
        {modalContainer}
      </div>
    );
  }
}

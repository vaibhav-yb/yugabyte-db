import { FC, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Box, Typography, Link } from '@material-ui/core';
import { YBTooltip } from '../../../components';
import { YBWidget } from '../../../../components/panels';
import { YBLoadingCircleIcon } from '../../../../components/common/indicators';
import { DBUpgradeModal } from '../universe-actions/rollback-upgrade/DBUpgradeModal';
import { isNonEmptyObject } from '../../../../utils/ObjectUtils';
import {
  getUniverseStatus,
  SoftwareUpgradeState,
  getUniversePendingTask,
  UniverseState,
  SoftwareUpgradeTaskType
} from '../../../../components/universes/helpers/universeHelpers';
import { Universe } from '../universe-form/utils/dto';
import { dbVersionWidgetStyles } from './DBVersionWidgetStyles';
//icons
import UpgradeArrow from '../../../assets/upgrade-arrow.svg';
import WarningExclamation from '../../../assets/warning-triangle.svg';

interface DBVersionWidgetProps {
  dbVersionValue: string;
  currentUniverse: Universe;
  tasks: Record<string, any>;
  higherVersionCount: number;
}

export const DBVersionWidget: FC<DBVersionWidgetProps> = ({
  dbVersionValue,
  currentUniverse,
  tasks,
  higherVersionCount
}) => {
  const { t } = useTranslation();
  const classes = dbVersionWidgetStyles();
  const [openUpgradeModal, setUpgradeModal] = useState(false);
  const minifiedCurrentVersion = dbVersionValue?.split('-')[0];
  const upgradeState = currentUniverse?.universeDetails?.softwareUpgradeState;
  const previousDBVersion = currentUniverse?.universeDetails?.prevYBSoftwareConfig?.softwareVersion;
  const isUniversePaused = currentUniverse?.universeDetails?.universePaused;
  const minifiedPrevVersion = previousDBVersion?.split('-')[0];
  const universeStatus = getUniverseStatus(currentUniverse);
  const universePendingTask = getUniversePendingTask(
    currentUniverse.universeUUID,
    tasks?.customerTaskList
  );

  let statusDisplay = (
    <Box display="flex" flexDirection={'row'} alignItems={'center'}>
      <Typography className={classes.versionText}>v{minifiedCurrentVersion}</Typography>
      &nbsp;
      {higherVersionCount > 0 && upgradeState === SoftwareUpgradeState.READY && !isUniversePaused && (
        <>
          <img src={UpgradeArrow} height="14px" width="14px" alt="--" /> &nbsp;
          <Link
            component={'button'}
            underline="always"
            onClick={() => setUpgradeModal(true)}
            className={classes.upgradeLink}
          >
            {t('universeActions.dbRollbackUpgrade.widget.upgradeAvailable')}
          </Link>
        </>
      )}
      {upgradeState === SoftwareUpgradeState.PRE_FINALIZE && (
        <YBTooltip title="Pending upgrade Finalization">
          <img src={WarningExclamation} height={'14px'} width="14px" alt="--" />
        </YBTooltip>
      )}
      {[SoftwareUpgradeState.FINALIZE_FAILED, SoftwareUpgradeState.UPGRADE_FAILED].includes(
        upgradeState
      ) && (
        <YBTooltip
          title={
            upgradeState === SoftwareUpgradeState.FINALIZE_FAILED
              ? `Failed to finalize upgrade to v${minifiedCurrentVersion}`
              : `Failed to upgrade database version to v${minifiedCurrentVersion}`
          }
        >
          <span>
            <i className={`fa fa-warning ${classes.errorIcon}`} />
          </span>
        </YBTooltip>
      )}
      {upgradeState === SoftwareUpgradeState.ROLLBACK_FAILED && (
        <YBTooltip title={`Failed to rollback to v${minifiedPrevVersion}`}>
          <span>
            <i className={`fa fa-warning ${classes.errorIcon}`} />
          </span>
        </YBTooltip>
      )}
    </Box>
  );

  if (
    universeStatus.state === UniverseState.PENDING &&
    isNonEmptyObject(universePendingTask) &&
    upgradeState !== SoftwareUpgradeState.READY
  ) {
    statusDisplay = (
      <Box display={'flex'} flexDirection={'row'} alignItems="baseline">
        <YBLoadingCircleIcon size="inline" variant="primary" />
        <Typography variant="body2" className={classes.blueText}>
          {universePendingTask.type === SoftwareUpgradeTaskType.ROLLBACK_UPGRADE &&
            (t('universeActions.dbRollbackUpgrade.widget.rollingBackTooltip', {
              version: minifiedPrevVersion
            }) as string)}
          {universePendingTask.type === SoftwareUpgradeTaskType.FINALIZE_UPGRADE &&
            (t('universeActions.dbRollbackUpgrade.widget.finalizingTooltip', {
              version: minifiedCurrentVersion
            }) as string)}
          {universePendingTask.type === SoftwareUpgradeTaskType.SOFTWARE_UPGRADE &&
            (t('universeActions.dbRollbackUpgrade.widget.upgradingTooltip', {
              version: minifiedCurrentVersion
            }) as string)}
        </Typography>
      </Box>
    );
  }

  return (
    <>
      <YBWidget
        noHeader
        noMargin
        size={1}
        className={classes.versionContainer}
        body={
          <Box
            display={'flex'}
            flexDirection={'row'}
            pt={3}
            pl={2}
            pr={2}
            width="100%"
            height={'100%'}
            justifyContent={'space-between'}
            alignItems={'center'}
          >
            <Typography variant="body1">
              {t('universeActions.dbRollbackUpgrade.widget.versionLabel')}
            </Typography>
            {statusDisplay}
          </Box>
        }
      />
      <DBUpgradeModal
        open={openUpgradeModal}
        onClose={() => setUpgradeModal(false)}
        universeData={currentUniverse}
      />
    </>
  );
};

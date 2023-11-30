// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.commissioner.tasks.upgrade;

import com.google.inject.Inject;
import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.ITask.Abortable;
import com.yugabyte.yw.commissioner.ITask.Retryable;
import com.yugabyte.yw.commissioner.UserTaskDetails;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.common.gflags.AutoFlagUtil;
import com.yugabyte.yw.forms.FinalizeUpgradeParams;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.NodeDetails;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Abortable
@Retryable
public class FinalizeUpgrade extends SoftwareUpgradeTaskBase {

  @Inject
  protected FinalizeUpgrade(BaseTaskDependencies baseTaskDependencies) {
    super(baseTaskDependencies);
  }

  @Override
  public UserTaskDetails.SubTaskGroupType getTaskSubGroupType() {
    return UserTaskDetails.SubTaskGroupType.FinalizingUpgrade;
  }

  @Override
  public NodeDetails.NodeState getNodeState() {
    return NodeDetails.NodeState.FinalizeUpgrade;
  }

  @Override
  public void validateParams(boolean isFirstTry) {
    super.validateParams(isFirstTry);
    taskParams().verifyParams(getUniverse(), isFirstTry);
  }

  @Override
  protected FinalizeUpgradeParams taskParams() {
    return (FinalizeUpgradeParams) taskParams;
  }

  @Override
  public void run() {
    runUpgrade(
        () -> {
          Universe universe = getUniverse();
          String version =
              universe.getUniverseDetails().getPrimaryCluster().userIntent.ybSoftwareVersion;

          createUpdateUniverseSoftwareUpgradeStateTask(
              UniverseDefinitionTaskParams.SoftwareUpgradeState.Finalizing,
              false /* isSoftwareRollbackAllowed */);

          if (!confGetter.getConfForScope(universe, UniverseConfKeys.skipUpgradeFinalize)) {
            if (taskParams().upgradeSystemCatalog) {
              // Run YSQL upgrade on the universe.
              createRunYsqlUpgradeTask(version).setSubTaskGroupType(getTaskSubGroupType());
            }
            // Promote all auto flags upto class External.
            createPromoteAutoFlagTask(
                    universe.getUniverseUUID(),
                    true /* ignoreErrors */,
                    AutoFlagUtil.EXTERNAL_AUTO_FLAG_CLASS_NAME /* maxClass */)
                .setSubTaskGroupType(getTaskSubGroupType());

            createUpdateUniverseSoftwareUpgradeStateTask(
                UniverseDefinitionTaskParams.SoftwareUpgradeState.Ready);

          } else {
            log.info("Skipping upgrade finalization for universe : " + universe.getUniverseUUID());
          }
        });
  }
}

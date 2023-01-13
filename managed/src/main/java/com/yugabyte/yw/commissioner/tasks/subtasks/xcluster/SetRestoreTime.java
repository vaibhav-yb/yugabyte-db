package com.yugabyte.yw.commissioner.tasks.subtasks.xcluster;

import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.tasks.XClusterConfigTaskBase;
import com.yugabyte.yw.forms.XClusterConfigTaskParams;
import com.yugabyte.yw.models.XClusterConfig;
import com.yugabyte.yw.models.TaskInfo;
import com.yugabyte.yw.models.Restore;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

@Slf4j
public class SetRestoreTime extends XClusterConfigTaskBase {

  @Inject
  protected SetRestoreTime(BaseTaskDependencies baseTaskDependencies) {
    super(baseTaskDependencies);
  }

  public static class Params extends XClusterConfigTaskParams {
    // The source universe UUID must be stored in universeUUID field.
    // The parent xCluster config must be stored in xClusterConfig field.
    // Table ids to set restore time for.
    public Set<String> tableIds;
  }

  @Override
  protected Params taskParams() {
    return (Params) taskParams;
  }

  @Override
  public String getName() {
    return String.format(
        "%s (sourceUniverse=%s, xClusterUuid=%s, tableIds=%s)",
        super.getName(),
        taskParams().universeUUID,
        taskParams().getXClusterConfig().uuid,
        taskParams().tableIds);
  }

  @Override
  public void run() {
    log.info("Running {}", getName());

    // The restore must belong to a parent xCluster config.
    XClusterConfig xClusterConfig = taskParams().getXClusterConfig();
    if (xClusterConfig == null) {
      throw new RuntimeException(
          "taskParams().xClusterConfig is null. Each SetRestoreTime subtask must belong to an "
              + "xCluster config");
    }

    // Update the DB.
    Date now = new Date();
    xClusterConfig.setRestoreTimeForTables(taskParams().tableIds, now, taskUUID);
    log.info("Restore time for tables {} set to {}", taskParams().tableIds, now);

    log.info("Completed {}", getName());
  }
}

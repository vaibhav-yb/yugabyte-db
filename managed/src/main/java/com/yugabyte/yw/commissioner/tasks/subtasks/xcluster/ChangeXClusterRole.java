package com.yugabyte.yw.commissioner.tasks.subtasks.xcluster;

import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.tasks.XClusterConfigTaskBase;
import com.yugabyte.yw.forms.XClusterConfigTaskParams;
import com.yugabyte.yw.models.HighAvailabilityConfig;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.XClusterConfig;
import com.yugabyte.yw.models.XClusterConfig.ConfigType;
import java.util.Objects;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.yb.cdc.CdcConsumer.XClusterRole;
import org.yb.client.ChangeXClusterRoleResponse;
import org.yb.client.GetMasterClusterConfigResponse;
import org.yb.client.YBClient;

@Slf4j
public class ChangeXClusterRole extends XClusterConfigTaskBase {

  @Inject
  protected ChangeXClusterRole(BaseTaskDependencies baseTaskDependencies) {
    super(baseTaskDependencies);
  }

  public static class Params extends XClusterConfigTaskParams {
    // The parent xCluster config must be stored in xClusterConfig field.

    // The source universe role.
    public XClusterRole sourceRole;

    // The target universe role.
    public XClusterRole targetRole;
  }

  @Override
  protected Params taskParams() {
    return (Params) taskParams;
  }

  @Override
  public String getName() {
    return String.format(
        "%s(xClusterConfig=%s,sourceRole=%s,targetRole=%s)",
        super.getName(),
        taskParams().getXClusterConfig(),
        taskParams().sourceRole,
        taskParams().targetRole);
  }

  @Override
  public void run() {
    log.info("Running {}", getName());

    XClusterConfig xClusterConfig = getXClusterConfigFromTaskParams();
    Universe universe = null;
    XClusterRole requestedRole = null;

    if (!xClusterConfig.type.equals(ConfigType.Txn)) {
      throw new IllegalArgumentException(
          "XCluster role can change only for transactional xCluster configs");
    }

    // Only one universe role can change in one subtask call.
    if (Objects.nonNull(taskParams().sourceRole)) {
      universe = Universe.getOrBadRequest(xClusterConfig.sourceUniverseUUID);
      requestedRole = taskParams().sourceRole;
    }
    if (Objects.nonNull(taskParams().targetRole)) {
      if (Objects.nonNull(universe)) {
        throw new IllegalArgumentException("The role of only one universe can be set");
      }
      universe = Universe.getOrBadRequest(xClusterConfig.targetUniverseUUID);
      requestedRole = taskParams().targetRole;
    }
    if (Objects.isNull(universe) || Objects.isNull(requestedRole)) {
      throw new IllegalArgumentException("No role change is requested");
    }

    String targetUniverseMasterAddresses = universe.getMasterAddresses();
    String targetUniverseCertificate = universe.getCertificateNodetoNode();
    try (YBClient client =
        ybService.getClient(targetUniverseMasterAddresses, targetUniverseCertificate)) {
      // Sync roles in YBA with YBDB.
      GetMasterClusterConfigResponse clusterConfigResp = client.getMasterClusterConfig();
      XClusterRole currentXClusterRole =
          clusterConfigResp.getConfig().getConsumerRegistry().getRole();
      if (Objects.nonNull(taskParams().sourceRole)) {
        xClusterConfig.sourceActive = currentXClusterRole.equals(XClusterRole.ACTIVE);
      } else {
        xClusterConfig.targetActive = currentXClusterRole.equals(XClusterRole.ACTIVE);
      }
      xClusterConfig.update();
      log.info(
          "Universe role for universe {} was synced to {}",
          universe.universeUUID,
          currentXClusterRole);

      if (requestedRole.equals(currentXClusterRole)) {
        log.warn(
            "The universe {} is already in {} role; no change happened",
            universe.universeUUID,
            currentXClusterRole);
      } else {
        ChangeXClusterRoleResponse resp = client.changeXClusterRole(requestedRole);
        if (resp.hasError()) {
          throw new RuntimeException(
              String.format(
                  "Failed to set the role for universe %s to %s on XClusterConfig(%s): %s",
                  universe.universeUUID, requestedRole, xClusterConfig, resp.errorMessage()));
        }

        if (HighAvailabilityConfig.get().isPresent()) {
          getUniverse(true).incrementVersion();
        }

        // Save the role in the DB.
        if (Objects.nonNull(taskParams().sourceRole)) {
          xClusterConfig.sourceActive = requestedRole.equals(XClusterRole.ACTIVE);
        } else {
          xClusterConfig.targetActive = requestedRole.equals(XClusterRole.ACTIVE);
        }
        xClusterConfig.update();
      }

    } catch (Exception e) {
      log.error("{} hit error : {}", getName(), e.getMessage());
      throw new RuntimeException(e);
    }

    log.info("Completed {}", getName());
  }
}

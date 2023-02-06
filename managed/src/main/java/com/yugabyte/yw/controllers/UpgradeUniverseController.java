// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.common.config.RuntimeConfGetter;
import com.yugabyte.yw.common.config.RuntimeConfigFactory;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.controllers.handlers.UpgradeUniverseHandler;
import com.yugabyte.yw.forms.CertsRotateParams;
import com.yugabyte.yw.forms.GFlagsUpgradeParams;
import com.yugabyte.yw.forms.KubernetesOverridesUpgradeParams;
import com.yugabyte.yw.forms.PlatformResults.YBPTask;
import com.yugabyte.yw.forms.ResizeNodeParams;
import com.yugabyte.yw.forms.RestartTaskParams;
import com.yugabyte.yw.forms.SoftwareUpgradeParams;
import com.yugabyte.yw.forms.SystemdUpgradeParams;
import com.yugabyte.yw.forms.ThirdpartySoftwareUpgradeParams;
import com.yugabyte.yw.forms.TlsToggleParams;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.UserIntent;
import com.yugabyte.yw.forms.UpgradeTaskParams;
import com.yugabyte.yw.forms.VMImageUpgradeParams;
import com.yugabyte.yw.models.Audit;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.Universe;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import play.libs.Json;
import play.mvc.Result;

@Slf4j
@Api(
    value = "Universe Upgrades Management",
    authorizations = @Authorization(AbstractPlatformController.API_KEY_AUTH))
public class UpgradeUniverseController extends AuthenticatedController {

  @Inject UpgradeUniverseHandler upgradeUniverseHandler;

  @Inject RuntimeConfigFactory runtimeConfigFactory;

  @Inject RuntimeConfGetter confGetter;

  /**
   * API that restarts all nodes in the universe. Supports rolling and non-rolling restart
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Restart Universe",
      notes = "Queues a task to perform a rolling restart in a universe.",
      nickname = "restartUniverse",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "restart_task_params",
          value = "Restart Task Params",
          dataType = "com.yugabyte.yw.forms.RestartTaskParams",
          required = true,
          paramType = "body"))
  public Result restartUniverse(UUID customerUuid, UUID universeUuid) {
    return requestHandler(
        upgradeUniverseHandler::restartUniverse,
        RestartTaskParams.class,
        Audit.ActionType.Restart,
        customerUuid,
        universeUuid);
  }

  /**
   * API that upgrades YugabyteDB software version in all nodes. Supports rolling and non-rolling
   * upgrade of the universe.
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Upgrade Software",
      notes = "Queues a task to perform software upgrade and rolling restart in a universe.",
      nickname = "upgradeSoftware",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "software_upgrade_params",
          value = "Software Upgrade Params",
          dataType = "com.yugabyte.yw.forms.SoftwareUpgradeParams",
          required = true,
          paramType = "body"))
  public Result upgradeSoftware(UUID customerUuid, UUID universeUuid) {
    return requestHandler(
        upgradeUniverseHandler::upgradeSoftware,
        SoftwareUpgradeParams.class,
        Audit.ActionType.UpgradeSoftware,
        customerUuid,
        universeUuid);
  }

  /**
   * API that upgrades gflags in all nodes of primary cluster. Supports rolling, non-rolling, and
   * non-restart upgrades upgrade of the universe.
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Upgrade GFlags",
      notes = "Queues a task to perform gflags upgrade and rolling restart in a universe.",
      nickname = "upgradeGFlags",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "gflags_upgrade_params",
          value = "GFlags Upgrade Params",
          dataType = "com.yugabyte.yw.forms.GFlagsUpgradeParams",
          required = true,
          paramType = "body"))
  public Result upgradeGFlags(UUID customerUuid, UUID universeUuid) {
    return requestHandler(
        upgradeUniverseHandler::upgradeGFlags,
        GFlagsUpgradeParams.class,
        Audit.ActionType.UpgradeGFlags,
        customerUuid,
        universeUuid);
  }

  /**
   * API that upgrades kubernetes overrides for primary and read clusters.
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Upgrade KubernetesOverrides",
      notes = "Queues a task to perform Kubernetesoverrides upgrade for a kubernetes universe.",
      nickname = "upgradeKubernetesOverrides",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "Kubernetes_overrides_upgrade_params",
          value = "Kubernetes Override Upgrade Params",
          dataType = "com.yugabyte.yw.forms.KubernetesOverridesUpgradeParams",
          required = true,
          paramType = "body"))
  public Result upgradeKubernetesOverrides(UUID customerUuid, UUID universeUuid) {
    return requestHandler(
        upgradeUniverseHandler::upgradeKubernetesOverrides,
        KubernetesOverridesUpgradeParams.class,
        Audit.ActionType.UpgradeKubernetesOverrides,
        customerUuid,
        universeUuid);
  }

  /**
   * API that rotates custom certificates for onprem universes. Supports rolling and non-rolling
   * upgrade of the universe.
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Upgrade Certs",
      notes = "Queues a task to perform certificate rotation and rolling restart in a universe.",
      nickname = "upgradeCerts",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "certs_rotate_params",
          value = "Certs Rotate Params",
          dataType = "com.yugabyte.yw.forms.CertsRotateParams",
          required = true,
          paramType = "body"))
  public Result upgradeCerts(UUID customerUuid, UUID universeUuid) {
    return requestHandler(
        upgradeUniverseHandler::rotateCerts,
        CertsRotateParams.class,
        Audit.ActionType.UpgradeCerts,
        customerUuid,
        universeUuid);
  }

  /**
   * API that toggles TLS state of the universe. Can enable/disable node to node and client to node
   * encryption. Supports rolling and non-rolling upgrade of the universe.
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Upgrade TLS",
      notes = "Queues a task to perform TLS ugprade and rolling restart in a universe.",
      nickname = "upgradeTls",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "tls_toggle_params",
          value = "TLS Toggle Params",
          dataType = "com.yugabyte.yw.forms.TlsToggleParams",
          required = true,
          paramType = "body"))
  public Result upgradeTls(UUID customerUuid, UUID universeUuid) {
    return requestHandler(
        upgradeUniverseHandler::toggleTls,
        TlsToggleParams.class,
        Audit.ActionType.ToggleTls,
        customerUuid,
        universeUuid);
  }

  /**
   * API that resizes nodes in the universe. Supports only rolling upgrade.
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Resize Node",
      notes = "Queues a task to perform node resize and rolling restart in a universe.",
      nickname = "resizeNode",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "resize_node_params",
          value = "Resize Node Params",
          dataType = "com.yugabyte.yw.forms.ResizeNodeParams",
          required = true,
          paramType = "body"))
  public Result resizeNode(UUID customerUuid, UUID universeUuid) {
    return requestHandler(
        upgradeUniverseHandler::resizeNode,
        ResizeNodeParams.class,
        Audit.ActionType.ResizeNode,
        customerUuid,
        universeUuid);
  }

  /**
   * API that upgrades third-party software on nodes in the universe. Supports only rolling upgrade.
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Upgrade third-party software",
      notes = "Queues a task to perform upgrade third-party software in a universe.",
      nickname = "upgradeThirdpartySoftware",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "thirdparty_software_upgrade_params",
          value = "Thirdparty Software Upgrade Params",
          dataType = "com.yugabyte.yw.forms.ThirdpartySoftwareUpgradeParams",
          required = true,
          paramType = "body"))
  public Result upgradeThirdpartySoftware(UUID customerUuid, UUID universeUuid) {
    return requestHandler(
        upgradeUniverseHandler::thirdpartySoftwareUpgrade,
        ThirdpartySoftwareUpgradeParams.class,
        Audit.ActionType.ThirdpartySoftwareUpgrade,
        customerUuid,
        universeUuid);
  }

  /**
   * API that upgrades VM Image for AWS and GCP based universes. Supports only rolling upgrade of
   * the universe.
   *
   * @param customerUuid ID of customer
   * @param universeUuid ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Upgrade VM Image",
      notes = "Queues a task to perform VM Image upgrade and rolling restart in a universe.",
      nickname = "upgradeVMImage",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "vmimage_upgrade_params",
          value = "VM Image Upgrade Params",
          dataType = "com.yugabyte.yw.forms.VMImageUpgradeParams",
          required = true,
          paramType = "body"))
  public Result upgradeVMImage(UUID customerUuid, UUID universeUuid) {
    Customer customer = Customer.getOrBadRequest(customerUuid);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUuid, customer);

    // TODO yb.cloud.enabled is redundant here because many tests set it during runtime,
    // to enable this method in cloud. Clean it up later when the tests are fixed.
    if (!runtimeConfigFactory.forUniverse(universe).getBoolean("yb.cloud.enabled")
        && !confGetter.getConfForScope(universe, UniverseConfKeys.ybUpgradeVmImage)) {
      throw new PlatformServiceException(METHOD_NOT_ALLOWED, "VM image upgrade is disabled.");
    }

    return requestHandler(
        upgradeUniverseHandler::upgradeVMImage,
        VMImageUpgradeParams.class,
        Audit.ActionType.UpgradeVmImage,
        customerUuid,
        universeUuid);
  }

  /**
   * API that upgrades from cron to systemd for universes. Supports only rolling upgrade of the
   * universe.
   *
   * @param customerUUID ID of customer
   * @param universeUUID ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Upgrade Systemd",
      notes = "Queues a task to perform systemd upgrade and rolling restart in a universe.",
      nickname = "upgradeSystemd",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "systemd_upgrade_params",
          value = "Systemd Upgrade Params",
          dataType = "com.yugabyte.yw.forms.SystemdUpgradeParams",
          required = true,
          paramType = "body"))
  public Result upgradeSystemd(UUID customerUUID, UUID universeUUID) {
    return requestHandler(
        upgradeUniverseHandler::upgradeSystemd,
        SystemdUpgradeParams.class,
        Audit.ActionType.UpgradeSystemd,
        customerUUID,
        universeUUID);
  }

  /**
   * API that reboots all nodes in the universe. Only supports rolling upgrade.
   *
   * @param customerUUID ID of customer
   * @param universeUUID ID of universe
   * @return Result of update operation with task id
   */
  @ApiOperation(
      value = "Reboot universe",
      notes = "Queues a task to perform a rolling reboot in a universe.",
      nickname = "rebootUniverse",
      response = YBPTask.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "upgrade_task_params",
          value = "Upgrade Task Params",
          dataType = "com.yugabyte.yw.forms.UpgradeTaskParams",
          required = true,
          paramType = "body"))
  public Result rebootUniverse(UUID customerUUID, UUID universeUUID) {
    return requestHandler(
        upgradeUniverseHandler::rebootUniverse,
        UpgradeTaskParams.class,
        Audit.ActionType.RebootUniverse,
        customerUUID,
        universeUUID);
  }

  private <T extends UpgradeTaskParams> Result requestHandler(
      IUpgradeUniverseHandlerMethod<T> serviceMethod,
      Class<T> type,
      Audit.ActionType auditActionType,
      UUID customerUuid,
      UUID universeUuid) {
    Customer customer = Customer.getOrBadRequest(customerUuid);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUuid, customer);
    T requestParams =
        UniverseControllerRequestBinder.bindFormDataToUpgradeTaskParams(
            ctx(), request(), type, universe);

    log.info(
        "Upgrade for universe {} [ {} ] customer {}.",
        universe.name,
        universe.universeUUID,
        customer.uuid);

    // prevent race condition in the case userIntent updates before we createAuditEntry
    UserIntent userIntent =
        Json.fromJson(
            Json.toJson(universe.getUniverseDetails().getPrimaryCluster().userIntent),
            UserIntent.class);
    UUID taskUuid = serviceMethod.upgrade(requestParams, customer, universe);
    JsonNode additionalDetails = null;
    if (type.equals(GFlagsUpgradeParams.class)) {
      additionalDetails =
          upgradeUniverseHandler.constructGFlagAuditPayload(
              (GFlagsUpgradeParams) requestParams, userIntent);
    }
    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            Audit.TargetType.Universe,
            universeUuid.toString(),
            auditActionType,
            request().body().asJson(),
            taskUuid,
            additionalDetails);
    return new YBPTask(taskUuid, universe.universeUUID).asResult();
  }
}

/*
 * Copyright 2021 YugaByte, Inc. and Contributors
 *
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

package com.yugabyte.yw.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.yugabyte.yw.cloud.UniverseResourceDetails;
import com.yugabyte.yw.cloud.UniverseResourceDetails.Context;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.common.Util;
import com.yugabyte.yw.common.config.RuntimeConfigFactory;
import com.yugabyte.yw.common.utils.FileUtils;
import com.yugabyte.yw.controllers.handlers.UniverseInfoHandler;
import com.yugabyte.yw.forms.PlatformResults;
import com.yugabyte.yw.forms.TriggerHealthCheckResult;
import com.yugabyte.yw.models.Audit;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.HealthCheck.Details;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.NodeDetails;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.mvc.Result;
import play.mvc.Results;

@Api(
    value = "Universe information",
    authorizations = @Authorization(AbstractPlatformController.API_KEY_AUTH))
@Slf4j
public class UniverseInfoController extends AuthenticatedController {

  @Inject private RuntimeConfigFactory runtimeConfigFactory;
  @Inject private UniverseInfoHandler universeInfoHandler;
  @Inject private HttpExecutionContext ec;

  private static final String YSQL_USERNAME_HEADER = "ysql-username";
  private static final String YSQL_PASSWORD_HEADER = "ysql-password";

  /**
   * API that checks the status of the the tservers and masters in the universe.
   *
   * @return result of the universe status operation.
   */
  @ApiOperation(
      value = "Get a universe's status",
      notes = "This will return a Map of node name to its status in json format",
      responseContainer = "Map",
      response = Object.class)
  // TODO API document error case.
  public Result universeStatus(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUUID, customer);

    // Get alive status
    JsonNode result = universeInfoHandler.status(universe);
    return PlatformResults.withRawData(result);
  }

  @ApiOperation(
      value = "Get a resource usage estimate for a universe",
      hidden = true,
      notes =
          "Expects UniverseDefinitionTaskParams in request body and calculates the resource "
              + "estimate for NodeDetailsSet in that.",
      response = UniverseResourceDetails.class)
  public Result getUniverseResources(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getOrBadRequest(universeUUID);
    return PlatformResults.withData(
        universeInfoHandler.getUniverseResources(customer, universe.getUniverseDetails()));
  }

  @ApiOperation(
      value = "Get a cost estimate for a universe",
      nickname = "getUniverseCost",
      response = UniverseResourceDetails.class)
  public Result universeCost(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUUID, customer);

    Context context =
        new Context(
            runtimeConfigFactory.globalRuntimeConf(), customer, universe.getUniverseDetails());
    return PlatformResults.withData(
        UniverseResourceDetails.create(universe.getUniverseDetails(), context));
  }

  @ApiOperation(
      value = "Get a cost estimate for all universes",
      nickname = "getUniverseCostForAll",
      responseContainer = "List",
      response = UniverseResourceDetails.class)
  public Result universeListCost(UUID customerUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    return PlatformResults.withData(universeInfoHandler.universeListCost(customer));
  }

  /**
   * Endpoint to retrieve the IP of the master leader for a given universe.
   *
   * @param customerUUID UUID of Customer the target Universe belongs to.
   * @param universeUUID UUID of Universe to retrieve the master leader private IP of.
   * @return The private IP of the master leader.
   */
  @ApiOperation(
      value = "Get IP address of a universe's master leader",
      nickname = "getMasterLeaderIP",
      response = Object.class)
  public Result getMasterLeaderIP(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUUID, customer);

    HostAndPort leaderMasterHostAndPort = universeInfoHandler.getMasterLeaderIP(universe);
    ObjectNode result = Json.newObject().put("privateIP", leaderMasterHostAndPort.getHost());
    return PlatformResults.withRawData(result);
  }

  @ApiOperation(
      value = "Get live queries for a universe",
      nickname = "getLiveQueries",
      response = Object.class)
  public Result getLiveQueries(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUUID, customer);
    log.info("Live queries for customer {}, universe {}", customer.uuid, universe.universeUUID);
    JsonNode resultNode = universeInfoHandler.getLiveQuery(universe);
    return PlatformResults.withRawData(resultNode);
  }

  @ApiOperation(
      value = "Get slow queries for a universe",
      nickname = "getSlowQueries",
      response = Object.class)
  public Result getSlowQueries(UUID customerUUID, UUID universeUUID) {
    log.info("Slow queries for customer {}, universe {}", customerUUID, universeUUID);
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUUID, customer);
    JsonNode resultNode = universeInfoHandler.getSlowQueries(universe);
    return Results.ok(resultNode);
  }

  @ApiOperation(
      value = "Reset slow queries for a universe",
      nickname = "resetSlowQueries",
      response = Object.class)
  public Result resetSlowQueries(UUID customerUUID, UUID universeUUID) {
    log.info("Resetting Slow queries for customer {}, universe {}", customerUUID, universeUUID);
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUUID, customer);
    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            Audit.TargetType.Universe,
            universeUUID.toString(),
            Audit.ActionType.ResetSlowQueries);
    return PlatformResults.withRawData(universeInfoHandler.resetSlowQueries(universe));
  }

  /**
   * API that checks the health of all the tservers and masters in the universe, as well as certain
   * conditions on the machines themselves, such as disk utilization, presence of FATAL or core
   * files, etc.
   *
   * @return result of the checker script
   */
  @ApiOperation(
      value = "Run a universe health check",
      notes =
          "Checks the health of all tablet servers and masters in the universe, as well as certain conditions on the machines themselves, including disk utilization, presence of FATAL or core files, and more.",
      nickname = "healthCheckUniverse",
      response = Object.class)
  public Result healthCheck(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe.getValidUniverseOrBadRequest(universeUUID, customer);

    List<Details> detailsList = universeInfoHandler.healthCheck(universeUUID);
    return PlatformResults.withData(detailsList);
  }

  @ApiOperation(
      value = "Trigger a universe health check",
      notes = "Trigger a universe health check and return the trigger time.",
      response = TriggerHealthCheckResult.class)
  public Result triggerHealthCheck(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUUID, customer);

    if (!runtimeConfigFactory.forUniverse(universe).getBoolean("yb.health.trigger_api.enabled")) {
      throw new PlatformServiceException(
          METHOD_NOT_ALLOWED, "Manual health check trigger is disabled.");
    }

    OffsetDateTime dt = OffsetDateTime.now(ZoneOffset.UTC);
    universeInfoHandler.triggerHealthCheck(customer, universe);

    TriggerHealthCheckResult res = new TriggerHealthCheckResult();
    res.timestamp = new Date(dt.toInstant().toEpochMilli());

    return PlatformResults.withData(res);
  }

  /**
   * API that downloads the log files for a particular node in a universe. Synchronized due to
   * potential race conditions.
   *
   * @param customerUUID ID of customer
   * @param universeUUID ID of universe
   * @param nodeName name of the node
   * @return tar file of the tserver and master log files (if the node is a master server).
   */
  @ApiOperation(
      value = "Download a node's logs",
      notes = "Downloads the log files from a given node.",
      nickname = "downloadNodeLogs",
      response = String.class,
      produces = "application/x-compressed")
  public CompletionStage<Result> downloadNodeLogs(
      UUID customerUUID, UUID universeUUID, String nodeName) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getValidUniverseOrBadRequest(universeUUID, customer);
    log.debug("Retrieving logs for " + nodeName);
    NodeDetails node =
        universe
            .maybeGetNode(nodeName)
            .orElseThrow(() -> new PlatformServiceException(NOT_FOUND, nodeName));
    return CompletableFuture.supplyAsync(
        () -> {
          String storagePath =
              runtimeConfigFactory.staticApplicationConf().getString("yb.storage.path");
          String tarFileName = node.cloudInfo.private_ip + "-logs.tar.gz";
          Path targetFile = Paths.get(storagePath + "/" + tarFileName);
          File file =
              universeInfoHandler.downloadNodeLogs(customer, universe, node, targetFile).toFile();
          InputStream is = FileUtils.getInputStreamOrFail(file);
          file.delete(); // TODO: should this be done in finally?
          // return the file to client
          response().setHeader("Content-Disposition", "attachment; filename=" + file.getName());
          return ok(is).as("application/x-compressed");
        },
        ec.current());
  }
}

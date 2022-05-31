// Copyright 2020 YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.yugabyte.yw.controllers;

import static com.yugabyte.yw.models.helpers.CommonUtils.datePlus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.yugabyte.yw.commissioner.Common;
import com.yugabyte.yw.commissioner.Common.CloudType;
import com.yugabyte.yw.common.CloudQueryHelper;
import com.yugabyte.yw.common.PlacementInfoUtil;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.common.alerts.AlertConfigurationService;
import com.yugabyte.yw.common.alerts.AlertService;
import com.yugabyte.yw.common.metrics.MetricService;
import com.yugabyte.yw.forms.AlertingData;
import com.yugabyte.yw.forms.AlertingFormData;
import com.yugabyte.yw.forms.CustomerDetailsData;
import com.yugabyte.yw.forms.FeatureUpdateFormData;
import com.yugabyte.yw.forms.MetricQueryParams;
import com.yugabyte.yw.forms.PlatformResults;
import com.yugabyte.yw.forms.PlatformResults.YBPError;
import com.yugabyte.yw.forms.PlatformResults.YBPSuccess;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.ClusterType;
import com.yugabyte.yw.metrics.MetricQueryHelper;
import com.yugabyte.yw.metrics.MetricSettings;
import com.yugabyte.yw.models.Alert;
import com.yugabyte.yw.models.Alert.State;
import com.yugabyte.yw.models.Audit;
import com.yugabyte.yw.models.AvailabilityZone;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.CustomerConfig;
import com.yugabyte.yw.models.Provider;
import com.yugabyte.yw.models.Region;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.Users;
import com.yugabyte.yw.models.XClusterConfig;
import com.yugabyte.yw.models.extended.UserWithFeatures;
import com.yugabyte.yw.models.filters.AlertFilter;
import com.yugabyte.yw.models.helpers.CommonUtils;
import com.yugabyte.yw.models.helpers.NodeDetails;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.data.Form;
import play.libs.Json;
import play.mvc.Result;

@Api(
    value = "Customer management",
    authorizations = @Authorization(AbstractPlatformController.API_KEY_AUTH))
public class CustomerController extends AuthenticatedController {

  public static final Logger LOG = LoggerFactory.getLogger(CustomerController.class);

  @Inject private AlertService alertService;

  @Inject private MetricService metricService;

  @Inject private MetricQueryHelper metricQueryHelper;

  @Inject private CloudQueryHelper cloudQueryHelper;

  @Inject private AlertConfigurationService alertConfigurationService;

  private static boolean checkNonNullMountRoots(NodeDetails n) {
    return n.cloudInfo != null
        && n.cloudInfo.mount_roots != null
        && !n.cloudInfo.mount_roots.isEmpty();
  }

  @Deprecated
  @ApiOperation(
      value = "UI_ONLY",
      response = UUID.class,
      responseContainer = "List",
      hidden = true,
      nickname = "ListOfCustomersUUID")
  public Result listUuids() {
    ArrayNode responseJson = Json.newArray();
    Customer.getAll().forEach(c -> responseJson.add(c.getUuid().toString()));
    return ok(responseJson);
  }

  @ApiOperation(
      value = "List customers",
      response = Customer.class,
      responseContainer = "List",
      nickname = "ListOfCustomers")
  public Result list() {
    return PlatformResults.withData(Customer.getAll());
  }

  @ApiOperation(
      value = "Get a customer's details",
      response = CustomerDetailsData.class,
      nickname = "CustomerDetail")
  public Result index(UUID customerUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);

    ObjectNode responseJson = (ObjectNode) Json.toJson(customer);
    CustomerConfig config = CustomerConfig.getAlertConfig(customerUUID);
    // TODO: get rid of this
    if (config != null) {
      responseJson.set("alertingData", config.getMaskedData());
    } else {
      responseJson.set("alertingData", null);
    }
    CustomerConfig smtpConfig = CustomerConfig.getSmtpConfig(customerUUID);
    if (smtpConfig != null) {
      responseJson.set("smtpData", smtpConfig.getMaskedData());
    } else {
      responseJson.set("smtpData", null);
    }
    responseJson.put(
        "callhomeLevel", CustomerConfig.getOrCreateCallhomeLevel(customerUUID).toString());

    UserWithFeatures user = (UserWithFeatures) ctx().args.get("user");
    if (customer.getFeatures().size() != 0 && user.getFeatures().size() != 0) {
      JsonNode featureSet = user.getFeatures();
      CommonUtils.deepMerge(featureSet, customer.getFeatures());
      responseJson.put("features", featureSet);
    } else if (customer.getFeatures().size() != 0) {
      responseJson.put("features", customer.getFeatures());
    } else {
      responseJson.put("features", user.getFeatures());
    }

    return ok(responseJson);
  }

  @ApiOperation(value = "Update a customer", response = Customer.class, nickname = "UpdateCustomer")
  @ApiImplicitParams({
    @ApiImplicitParam(
        name = "Customer",
        value = "Customer data to be updated",
        required = true,
        dataType = "com.yugabyte.yw.forms.AlertingFormData",
        paramType = "body")
  })
  public Result update(UUID customerUUID) {

    Customer customer = Customer.getOrBadRequest(customerUUID);

    JsonNode request = request().body().asJson();
    Form<AlertingFormData> formData = formFactory.getFormDataOrBadRequest(AlertingFormData.class);
    AlertingFormData alertingFormData = formData.get();

    if (alertingFormData.name != null) {
      customer.name = alertingFormData.name;
      customer.save();
    }

    if (request.has("alertingData") || request.has("smtpData")) {

      CustomerConfig config = CustomerConfig.getAlertConfig(customerUUID);
      if (alertingFormData.alertingData != null) {
        long activeAlertNotificationPeriod =
            alertingFormData.alertingData.activeAlertNotificationIntervalMs;
        long oldActiveAlertNotificationPeriod = 0;
        if (config == null) {
          CustomerConfig.createAlertConfig(
              customerUUID, Json.toJson(alertingFormData.alertingData));
        } else {
          AlertingData oldData = Json.fromJson(config.getData(), AlertingData.class);
          if (oldData != null) {
            oldActiveAlertNotificationPeriod = oldData.activeAlertNotificationIntervalMs;
          }
          config.unmaskAndSetData((ObjectNode) Json.toJson(alertingFormData.alertingData));
          config.update();
        }

        if (activeAlertNotificationPeriod != oldActiveAlertNotificationPeriod) {
          AlertFilter alertFilter =
              AlertFilter.builder().customerUuid(customerUUID).state(State.ACTIVE).build();
          List<Alert> activeAlerts = alertService.list(alertFilter);
          List<Alert> alertsToUpdate;
          if (activeAlertNotificationPeriod > 0) {
            // In case there was notification attempt - setting to last attempt time
            // + interval as next notification attempt. Even if it's before now -
            // instant notification will happen - which is what we need.
            alertsToUpdate =
                activeAlerts
                    .stream()
                    .filter(
                        alert ->
                            alert.getNextNotificationTime() == null
                                && alert.getNotificationAttemptTime() != null)
                    .map(
                        alert ->
                            alert.setNextNotificationTime(
                                datePlus(
                                    alert.getNotificationAttemptTime(),
                                    activeAlertNotificationPeriod,
                                    ChronoUnit.MILLIS)))
                    .collect(Collectors.toList());
          } else {
            // In case we already notified on ACTIVE state and scheduled subsequent notification
            // - clean that
            alertsToUpdate =
                activeAlerts
                    .stream()
                    .filter(
                        alert ->
                            alert.getNextNotificationTime() != null
                                && alert.getNotifiedState() != null)
                    .map(alert -> alert.setNextNotificationTime(null))
                    .collect(Collectors.toList());
          }
          alertService.save(alertsToUpdate);
        }
      }

      CustomerConfig smtpConfig = CustomerConfig.getSmtpConfig(customerUUID);
      if (smtpConfig == null && alertingFormData.smtpData != null) {
        CustomerConfig.createSmtpConfig(customerUUID, Json.toJson(alertingFormData.smtpData));
      } else if (smtpConfig != null && alertingFormData.smtpData != null) {
        smtpConfig.unmaskAndSetData((ObjectNode) Json.toJson(alertingFormData.smtpData));
        smtpConfig.update();
      } // In case we want to reset the smtpData and use the default mailing server.
      else if (request.has("smtpData") && alertingFormData.smtpData == null) {
        if (smtpConfig != null) {
          smtpConfig.delete();
        }
      }
    }

    // Features would be a nested json, so we should fetch it differently.
    JsonNode requestBody = request().body().asJson();
    if (requestBody.has("features")) {
      customer.upsertFeatures(requestBody.get("features"));
    }

    CustomerConfig.upsertCallhomeConfig(customerUUID, alertingFormData.callhomeLevel);
    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            Audit.TargetType.Customer,
            customerUUID.toString(),
            Audit.ActionType.Update,
            Json.toJson(formData));
    return ok(Json.toJson(customer));
  }

  @ApiOperation(
      value = "Delete a customer",
      response = YBPSuccess.class,
      nickname = "deleteCustomer")
  public Result delete(UUID customerUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);

    List<Users> users = Users.getAll(customerUUID);
    for (Users user : users) {
      user.delete();
    }

    if (!customer.delete()) {
      throw new PlatformServiceException(
          INTERNAL_SERVER_ERROR, "Unable to delete Customer UUID: " + customerUUID);
    }

    metricService.markSourceRemoved(customerUUID, null);

    auditService()
        .createAuditEntryWithReqBody(
            ctx(), Audit.TargetType.Customer, customerUUID.toString(), Audit.ActionType.Delete);
    return YBPSuccess.empty();
  }

  @ApiOperation(
      value = "Create or update a customer's features",
      hidden = true,
      responseContainer = "Map",
      response = Object.class)
  @ApiImplicitParams({
    @ApiImplicitParam(
        name = "Feature",
        value = "Feature to be upserted",
        required = true,
        dataType = "com.yugabyte.yw.forms.FeatureUpdateFormData",
        paramType = "body")
  })
  public Result upsertFeatures(UUID customerUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);

    JsonNode requestBody = request().body().asJson();
    ObjectMapper mapper = new ObjectMapper();
    FeatureUpdateFormData formData;
    try {
      formData = mapper.treeToValue(requestBody, FeatureUpdateFormData.class);
    } catch (RuntimeException | JsonProcessingException e) {
      throw new PlatformServiceException(BAD_REQUEST, "Invalid JSON");
    }

    customer.upsertFeatures(formData.features);

    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            Audit.TargetType.Customer,
            customerUUID.toString(),
            Audit.ActionType.UpsertCustomerFeatures,
            requestBody);
    return ok(customer.getFeatures());
  }

  @ApiOperation(
      value = "Add metrics to a customer",
      response = Object.class,
      responseContainer = "Map")
  @ApiResponses(
      @io.swagger.annotations.ApiResponse(
          code = BAD_REQUEST,
          message = "When request fails validations.",
          response = YBPError.class))
  @ApiImplicitParams({
    @ApiImplicitParam(
        name = "Metrics",
        value = "Metrics to be added",
        required = true,
        dataType = "com.yugabyte.yw.forms.MetricQueryParams",
        paramType = "body")
  })
  public Result metrics(UUID customerUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);

    Form<MetricQueryParams> formData = formFactory.getFormDataOrBadRequest(MetricQueryParams.class);
    MetricQueryParams metricQueryParams = formData.get();

    if (CollectionUtils.isEmpty(metricQueryParams.getMetrics())
        && CollectionUtils.isEmpty(metricQueryParams.getMetricsWithSettings())) {
      throw new PlatformServiceException(
          BAD_REQUEST, "Either metrics or metricsWithSettings should not be empty");
    }

    Map<String, MetricSettings> metricSettingsMap = new LinkedHashMap<>();
    if (CollectionUtils.isNotEmpty(metricQueryParams.getMetrics())) {
      metricQueryParams
          .getMetrics()
          .stream()
          .map(MetricSettings::defaultSettings)
          .forEach(
              metricSettings -> metricSettingsMap.put(metricSettings.getMetric(), metricSettings));
    }
    if (CollectionUtils.isNotEmpty(metricQueryParams.getMetricsWithSettings())) {
      metricQueryParams
          .getMetricsWithSettings()
          .forEach(
              metricSettings -> metricSettingsMap.put(metricSettings.getMetric(), metricSettings));
    }

    Map<String, String> params = new HashMap<>(formData.rawData());
    HashMap<String, Map<String, String>> filterOverrides = new HashMap<>();
    // Given we have a limitation on not being able to rename the pod labels in
    // kubernetes cadvisor metrics, we try to see if the metric being queried is for
    // container or not, and use pod_name vs exported_instance accordingly.
    // Expect for container metrics, all the metrics would with node_prefix and exported_instance.
    boolean hasContainerMetric =
        metricSettingsMap.keySet().stream().anyMatch(s -> s.startsWith("container"));
    String universeFilterLabel = hasContainerMetric ? "namespace" : "node_prefix";
    String nodeFilterLabel = hasContainerMetric ? "pod_name" : "exported_instance";
    String containerLabel = "container_name";
    String pvcLabel = "persistentvolumeclaim";

    ObjectNode filterJson = Json.newObject();
    if (!params.containsKey("nodePrefix")) {
      String universePrefixes =
          customer
              .getUniverses()
              .stream()
              .map((universe -> universe.getUniverseDetails().nodePrefix))
              .collect(Collectors.joining("|"));
      filterJson.put(universeFilterLabel, String.join("|", universePrefixes));
    } else {
      // Check if it is a Kubernetes deployment.
      if (hasContainerMetric) {
        final String nodePrefix = params.remove("nodePrefix");
        // Check if the universe is using newNamingStyle.
        List<Universe> universes =
            customer
                .getUniverses()
                .stream()
                .filter(universe -> universe.getUniverseDetails().nodePrefix.equals(nodePrefix))
                .collect(Collectors.toList());
        if (universes.isEmpty()) {
          throw new PlatformServiceException(
              INTERNAL_SERVER_ERROR,
              String.format("No universe found with nodePrefix %s.", nodePrefix));
        }
        if (universes.size() > 1) {
          LOG.warn("Found mulitple universes with nodePrefix {}, using first one.", nodePrefix);
        }
        Universe universe = universes.get(0);

        boolean newNamingStyle = universe.getUniverseDetails().useNewHelmNamingStyle;

        if (params.containsKey("nodeName")) {
          String nodeName = params.remove("nodeName");
          NodeDetails node =
              universe
                  .maybeGetNode(nodeName)
                  .orElseThrow(() -> new PlatformServiceException(NOT_FOUND, nodeName));

          String podFQDN = node.cloudInfo.private_ip;
          if (StringUtils.isBlank(podFQDN)) {
            throw new PlatformServiceException(
                INTERNAL_SERVER_ERROR, nodeName + " has a blank FQDN");
          }

          String podName = podFQDN.split("\\.")[0];
          String namespace = podFQDN.split("\\.")[2];
          // The pod name is of the format
          // <helm_prefix>-yb-<server>-<replica_num> and we just need
          // the container, which is yb-<server>.
          String containerName = podName.contains("yb-master") ? "yb-master" : "yb-tserver";
          String pvcName = String.format("(.*)-%s", podName);
          filterJson.put(nodeFilterLabel, podName);
          filterJson.put(containerLabel, containerName);
          filterJson.put(pvcLabel, pvcName);
          filterJson.put(universeFilterLabel, namespace);
        } else {
          filterJson.put(
              universeFilterLabel, getNamespacesFilter(universe, nodePrefix, newNamingStyle));
          // Check if the universe is using newNamingStyle.
          if (newNamingStyle) {
            // TODO(bhavin192): account for max character limit in
            // Helm release name, which is 53 characters.
            // The default value in metrics.yml is yb-tserver-(.*)
            filterJson.put(nodeFilterLabel, nodePrefix + "-(.*)-yb-tserver-(.*)");
          }
        }
      } else {
        final String nodePrefix = params.remove("nodePrefix");
        filterJson.put(universeFilterLabel, nodePrefix);
        if (params.containsKey("nodeName")) {
          filterJson.put(nodeFilterLabel, params.remove("nodeName"));
        }

        filterOverrides.putAll(
            getFilterOverrides(customer, nodePrefix, metricSettingsMap.keySet()));
      }
    }
    if (params.containsKey("tableName")) {
      filterJson.put("table_name", params.remove("tableName"));
    }
    if (params.containsKey("xClusterConfigUuid")) {
      XClusterConfig xClusterConfig =
          XClusterConfig.getOrBadRequest(UUID.fromString(params.remove("xClusterConfigUuid")));
      String tableIdRegex = String.join("|", xClusterConfig.getTables());
      filterJson.put("table_id", tableIdRegex);
    }
    params.put("filters", Json.stringify(filterJson));
    JsonNode response;
    response =
        metricQueryHelper.query(
            new ArrayList<>(metricSettingsMap.values()),
            params,
            filterOverrides,
            metricQueryParams.getIsRecharts());

    if (response.has("error")) {
      throw new PlatformServiceException(BAD_REQUEST, response.get("error"));
    }
    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            Audit.TargetType.Customer,
            customerUUID.toString(),
            Audit.ActionType.AddMetrics,
            request().body().asJson());
    return PlatformResults.withRawData(response);
  }

  // Return a regex string for filtering the metrics based on
  // namespaces of the given universe and nodePrefix. Should be used
  // for Kubernetes universes only.
  private String getNamespacesFilter(Universe universe, String nodePrefix, boolean newNamingStyle) {
    // We need to figure out the correct namespace for each AZ.  We do
    // that by getting the the universe's provider and then go through
    // the azConfigs.
    List<String> namespaces = new ArrayList<>();

    for (UniverseDefinitionTaskParams.Cluster cluster : universe.getUniverseDetails().clusters) {
      Provider provider = Provider.getOrBadRequest(UUID.fromString(cluster.userIntent.provider));
      for (Region r : Region.getByProvider(provider.uuid)) {
        for (AvailabilityZone az : AvailabilityZone.getAZsForRegion(r.uuid)) {
          boolean isMultiAZ = PlacementInfoUtil.isMultiAZ(provider);
          namespaces.add(
              PlacementInfoUtil.getKubernetesNamespace(
                  isMultiAZ,
                  nodePrefix,
                  az.code,
                  az.getUnmaskedConfig(),
                  newNamingStyle,
                  cluster.clusterType == ClusterType.ASYNC));
        }
      }
    }

    return String.join("|", namespaces);
  }

  @ApiOperation(
      value = "Get a customer's host info",
      responseContainer = "Map",
      response = Object.class)
  public Result getHostInfo(UUID customerUUID) {
    Customer.getOrBadRequest(customerUUID);
    ObjectNode hostInfo = Json.newObject();
    hostInfo.put(
        Common.CloudType.aws.name(),
        cloudQueryHelper.currentHostInfo(
            Common.CloudType.aws,
            ImmutableList.of("instance-id", "vpc-id", "privateIp", "region")));
    hostInfo.put(
        Common.CloudType.gcp.name(), cloudQueryHelper.currentHostInfo(Common.CloudType.gcp, null));

    return PlatformResults.withRawData(hostInfo);
  }

  private HashMap<String, HashMap<String, String>> getFilterOverrides(
      Customer customer, String nodePrefix, Set<String> metricNames) {

    HashMap<String, HashMap<String, String>> filterOverrides = new HashMap<>();
    // For a disk usage metric query, the mount point has to be modified to match the actual
    // mount point for an onprem universe.
    if (metricNames.contains("disk_usage")) {
      List<Universe> universes =
          customer
              .getUniverses()
              .stream()
              .filter(
                  u ->
                      u.getUniverseDetails().nodePrefix != null
                          && u.getUniverseDetails().nodePrefix.equals(nodePrefix))
              .collect(Collectors.toList());
      if (universes.get(0).getUniverseDetails().getPrimaryCluster().userIntent.providerType
          == CloudType.onprem) {
        final String mountRoots =
            universes
                .get(0)
                .getNodes()
                .stream()
                .filter(CustomerController::checkNonNullMountRoots)
                .map(n -> n.cloudInfo.mount_roots)
                .findFirst()
                .orElse("");
        // TODO: technically, this code is based on the primary cluster being onprem
        // and will return inaccurate results if the universe has a read replica that is
        // not onprem.
        if (!mountRoots.isEmpty()) {
          HashMap<String, String> mountFilters = new HashMap<>();
          mountFilters.put("mountpoint", mountRoots.replace(',', '|'));
          // convert "/storage1,/bar" to the filter "/storage1|/bar"
          filterOverrides.put("disk_usage", mountFilters);
        } else {
          LOG.debug("No mount points found in onprem universe {}", nodePrefix);
        }
      }
    }
    return filterOverrides;
  }
}

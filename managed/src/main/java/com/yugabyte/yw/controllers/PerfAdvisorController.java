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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigSyntax;
import com.yugabyte.yw.commissioner.PerfAdvisorScheduler;
import com.yugabyte.yw.commissioner.PerfAdvisorScheduler.RunResult;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.common.config.RuntimeConfService;
import com.yugabyte.yw.common.config.impl.SettableRuntimeConfigFactory;
import com.yugabyte.yw.forms.PerfAdvisorSettingsFormData;
import com.yugabyte.yw.forms.PerfAdvisorSettingsWithDefaults;
import com.yugabyte.yw.forms.PlatformResults;
import com.yugabyte.yw.forms.PlatformResults.YBPSuccess;
import com.yugabyte.yw.models.Audit;
import com.yugabyte.yw.models.Audit.ActionType;
import com.yugabyte.yw.models.Audit.TargetType;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.extended.UserWithFeatures;
import io.ebean.annotation.Transactional;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.yb.perf_advisor.filters.PerformanceRecommendationFilter;
import org.yb.perf_advisor.filters.StateChangeAuditInfoFilter;
import org.yb.perf_advisor.models.PerformanceRecommendation;
import org.yb.perf_advisor.models.PerformanceRecommendation.RecommendationState;
import org.yb.perf_advisor.models.paging.PerformanceRecommendationPagedQuery;
import org.yb.perf_advisor.models.paging.PerformanceRecommendationPagedResponse;
import org.yb.perf_advisor.models.paging.StateChangeAuditInfoPagedQuery;
import org.yb.perf_advisor.models.paging.StateChangeAuditInfoPagedResponse;
import org.yb.perf_advisor.services.db.PerformanceRecommendationService;
import org.yb.perf_advisor.services.db.StateChangeAuditInfoService;
import org.yb.perf_advisor.services.db.ValidationException;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;

@Api(
    value = "Performance Advisor",
    authorizations = @Authorization(AbstractPlatformController.API_KEY_AUTH))
@Slf4j
public class PerfAdvisorController extends AuthenticatedController {

  private static final String PERF_ADVISOR_SETTINGS_KEY = "yb.perf_advisor";

  @Inject private PerformanceRecommendationService performanceRecommendationService;
  @Inject private StateChangeAuditInfoService stateChangeAuditInfoService;
  @Inject private SettableRuntimeConfigFactory configFactory;
  @Inject private RuntimeConfService runtimeConfService;

  @Inject private PerfAdvisorScheduler perfAdvisorScheduler;

  @ApiOperation(
      value = "Get performance recommendation details",
      response = PerformanceRecommendation.class)
  public Result get(UUID customerUUID, UUID recommendationUuid) {
    Customer.getOrBadRequest(customerUUID);

    PerformanceRecommendation recommendation =
        convertException(
            () -> performanceRecommendationService.getOrThrow(recommendationUuid),
            "Get performance recommendation with id " + recommendationUuid);

    return PlatformResults.withData(recommendation);
  }

  @ApiOperation(
      value = "List performance recommendations (paginated)",
      response = PerformanceRecommendationPagedResponse.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "PagePerformanceRecommendationRequest",
          paramType = "body",
          dataType = "org.yb.perf_advisor.models.paging.PerformanceRecommendationPagedQuery",
          required = true))
  public Result page(UUID customerUUID) {
    Customer.getOrBadRequest(customerUUID);

    PerformanceRecommendationPagedQuery inputQuery =
        parseJsonAndValidate(PerformanceRecommendationPagedQuery.class);
    PerformanceRecommendationFilter inputFilter = inputQuery.getFilter();
    PerformanceRecommendationFilter filter =
        inputFilter.toBuilder().customerId(customerUUID).build();
    PerformanceRecommendationPagedQuery query =
        inputQuery.copyWithFilter(filter, PerformanceRecommendationPagedQuery.class);

    PerformanceRecommendationPagedResponse response =
        convertException(
            () -> performanceRecommendationService.pagedList(query),
            "Get performance recommendation page " + query);

    return PlatformResults.withData(response);
  }

  @ApiOperation(value = "Hide performance recommendations", response = YBPSuccess.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "HidePerformanceRecommendationsRequest",
          paramType = "body",
          dataType = "org.yb.perf_advisor.filters.PerformanceRecommendationFilter",
          required = true))
  public Result hide(UUID customerUUID) {
    return updateRecommendations(
        customerUUID,
        recommendation -> recommendation.setRecommendationState(RecommendationState.HIDDEN));
  }

  @ApiOperation(value = "Resolve performance recommendations", response = YBPSuccess.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "ResolvePerformanceRecommendationsRequest",
          paramType = "body",
          dataType = "org.yb.perf_advisor.filters.PerformanceRecommendationFilter",
          required = true))
  public Result resolve(UUID customerUUID) {
    return updateRecommendations(
        customerUUID,
        recommendation -> recommendation.setRecommendationState(RecommendationState.RESOLVED));
  }

  @ApiOperation(value = "Delete performance recommendations", response = YBPSuccess.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "DeletePerformanceRecommendationsRequest",
          paramType = "body",
          dataType = "org.yb.perf_advisor.filters.PerformanceRecommendationFilter",
          required = true))
  public Result delete(UUID customerUUID) {
    Customer.getOrBadRequest(customerUUID);

    PerformanceRecommendationFilter inputFilter =
        parseJsonAndValidate(PerformanceRecommendationFilter.class);
    PerformanceRecommendationFilter filter =
        inputFilter.toBuilder().customerId(customerUUID).build();

    convertException(
        () -> performanceRecommendationService.delete(filter),
        "Delete performance recommendations " + filter);

    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            TargetType.PerformanceRecommendation,
            null,
            Audit.ActionType.Delete,
            request().body().asJson());
    return YBPSuccess.empty();
  }

  private Result updateRecommendations(
      UUID customerUUID, Consumer<PerformanceRecommendation> updater) {
    UserWithFeatures user = (UserWithFeatures) Http.Context.current().args.get("user");
    Customer.getOrBadRequest(customerUUID);

    PerformanceRecommendationFilter inputFilter =
        parseJsonAndValidate(PerformanceRecommendationFilter.class);
    PerformanceRecommendationFilter filter =
        inputFilter.toBuilder().customerId(customerUUID).build();

    List<PerformanceRecommendation> recommendations =
        convertException(
            () -> performanceRecommendationService.list(filter),
            "List performance recommendations " + filter);

    recommendations.forEach(updater);

    convertException(
        () -> performanceRecommendationService.save(recommendations, user.getUser().uuid),
        "Save performance recommendations");

    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            TargetType.PerformanceRecommendation,
            null,
            ActionType.Update,
            request().body().asJson());
    return YBPSuccess.empty();
  }

  @ApiOperation(
      value = "List performance recommendations state change audit events (paginated)",
      response = StateChangeAuditInfoPagedResponse.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "PageStateChangeAuditInfoRequest",
          paramType = "body",
          dataType = "org.yb.perf_advisor.models.paging.StateChangeAuditInfoPagedQuery",
          required = true))
  public Result pageAuditInfo(UUID customerUUID) {
    Customer.getOrBadRequest(customerUUID);

    StateChangeAuditInfoPagedQuery inputQuery =
        parseJsonAndValidate(StateChangeAuditInfoPagedQuery.class);
    StateChangeAuditInfoFilter inputFilter = inputQuery.getFilter();
    StateChangeAuditInfoFilter filter = inputFilter.toBuilder().customerId(customerUUID).build();
    StateChangeAuditInfoPagedQuery query =
        inputQuery.copyWithFilter(filter, StateChangeAuditInfoPagedQuery.class);

    StateChangeAuditInfoPagedResponse response =
        convertException(
            () -> stateChangeAuditInfoService.pagedList(query),
            "Get performance recommendation page " + query);

    return PlatformResults.withData(response);
  }

  @ApiOperation(
      value = "Get universe performance advisor settings",
      response = PerfAdvisorSettingsWithDefaults.class)
  public Result getSettings(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getOrBadRequest(universeUUID);
    if (!customer.getCustomerId().equals(universe.customerId)) {
      throw new PlatformServiceException(
          BAD_REQUEST, "Universe " + universeUUID + " does not belong to customer " + customerUUID);
    }

    String jsonDefaultSettings =
        configFactory
            .forCustomer(customer)
            .getValue(PERF_ADVISOR_SETTINGS_KEY)
            .render(ConfigRenderOptions.concise());
    PerfAdvisorSettingsFormData defaultSettings =
        Json.fromJson(Json.parse(jsonDefaultSettings), PerfAdvisorSettingsFormData.class);
    PerfAdvisorSettingsWithDefaults result =
        new PerfAdvisorSettingsWithDefaults().setDefaultSettings(defaultSettings);

    String configString =
        runtimeConfService.getKeyIfPresent(customerUUID, universeUUID, PERF_ADVISOR_SETTINGS_KEY);
    if (StringUtils.isEmpty(configString)) {
      return PlatformResults.withData(result);
    }

    String jsonUniverseSettings =
        ConfigFactory.parseString(configString).root().render(ConfigRenderOptions.concise());
    PerfAdvisorSettingsFormData universeSettings =
        Json.fromJson(Json.parse(jsonUniverseSettings), PerfAdvisorSettingsFormData.class);
    result.setUniverseSettings(universeSettings);

    return PlatformResults.withData(result);
  }

  @ApiOperation(value = "Update universe performance advisor settings", response = YBPSuccess.class)
  @ApiImplicitParams(
      @ApiImplicitParam(
          name = "PerformanceAdvisorSettingsRequest",
          paramType = "body",
          dataType = "com.yugabyte.yw.forms.PerfAdvisorSettingsFormData",
          required = true))
  @Transactional
  public Result updateSettings(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getOrBadRequest(universeUUID);
    if (!customer.getCustomerId().equals(universe.customerId)) {
      throw new PlatformServiceException(
          BAD_REQUEST, "Universe " + universeUUID + " does not belong to customer " + customerUUID);
    }
    PerfAdvisorSettingsFormData settings = parseJsonAndValidate(PerfAdvisorSettingsFormData.class);
    String settingsJsonString = Json.stringify(Json.toJson(settings));
    String settingsString =
        ConfigFactory.parseString(
                settingsJsonString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
            .root()
            .render();

    runtimeConfService.setKey(
        customerUUID, universeUUID, PERF_ADVISOR_SETTINGS_KEY, settingsString);

    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            TargetType.PerformanceAdvisorSettings,
            null,
            ActionType.Update,
            request().body().asJson());
    return YBPSuccess.empty();
  }

  @ApiOperation(value = "Start performance advisor run for universe", response = YBPSuccess.class)
  public Result runPerfAdvisor(UUID customerUUID, UUID universeUUID) {
    Customer customer = Customer.getOrBadRequest(customerUUID);
    Universe universe = Universe.getOrBadRequest(universeUUID);
    if (!customer.getCustomerId().equals(universe.customerId)) {
      throw new PlatformServiceException(
          BAD_REQUEST, "Universe " + universeUUID + " does not belong to customer " + customerUUID);
    }

    RunResult result = perfAdvisorScheduler.runPerfAdvisor(universe);
    auditService()
        .createAuditEntryWithReqBody(
            ctx(),
            TargetType.PerformanceAdvisorRun,
            null,
            ActionType.Create,
            request().body().asJson());
    if (result.isStarted()) {
      return YBPSuccess.empty();
    } else {
      throw new PlatformServiceException(PRECONDITION_FAILED, result.getFailureReason());
    }
  }

  private <T> T convertException(Callable<T> operation, String operationName) {
    try {
      return operation.call();
    } catch (ValidationException e) {
      JsonNode errJson = Json.toJson(e.getValidationErrors());
      throw new PlatformServiceException(BAD_REQUEST, errJson);
    } catch (Exception e) {
      log.error(operationName + " failed", e);
      throw new PlatformServiceException(INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }
}

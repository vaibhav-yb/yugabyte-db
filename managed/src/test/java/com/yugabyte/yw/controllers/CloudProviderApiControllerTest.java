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

import static com.yugabyte.yw.common.AssertHelper.assertAuditEntry;
import static com.yugabyte.yw.common.AssertHelper.assertBadRequest;
import static com.yugabyte.yw.common.AssertHelper.assertInternalServerError;
import static com.yugabyte.yw.common.AssertHelper.assertOk;
import static com.yugabyte.yw.common.AssertHelper.assertPlatformException;
import static com.yugabyte.yw.common.AssertHelper.assertValue;
import static com.yugabyte.yw.common.AssertHelper.assertValues;
import static com.yugabyte.yw.common.AssertHelper.assertYBPSuccess;
import static com.yugabyte.yw.common.ModelFactory.createUniverse;
import static com.yugabyte.yw.common.TestHelper.createTempFile;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static play.test.Helpers.contentAsString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.yugabyte.yw.cloud.CloudAPI;
import com.yugabyte.yw.cloud.gcp.GCPCloudImpl;
import com.yugabyte.yw.commissioner.Common;
import com.yugabyte.yw.commissioner.Common.CloudType;
import com.yugabyte.yw.commissioner.tasks.CloudBootstrap;
import com.yugabyte.yw.common.ApiUtils;
import com.yugabyte.yw.common.ConfigHelper;
import com.yugabyte.yw.common.FakeApiHelper;
import com.yugabyte.yw.common.FakeDBApplication;
import com.yugabyte.yw.common.ModelFactory;
import com.yugabyte.yw.common.ShellResponse;
import com.yugabyte.yw.forms.PlatformResults.YBPTask;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.models.AccessKey;
import com.yugabyte.yw.models.AvailabilityZone;
import com.yugabyte.yw.models.helpers.CloudInfoInterface;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.InstanceType;
import com.yugabyte.yw.models.Provider;
import com.yugabyte.yw.models.ProviderDetails;
import com.yugabyte.yw.models.Region;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.Users;
import com.yugabyte.yw.models.helpers.TaskType;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.libs.Json;
import play.mvc.Result;

@RunWith(JUnitParamsRunner.class)
@Slf4j
public class CloudProviderApiControllerTest extends FakeDBApplication {
  public static final Logger LOG = LoggerFactory.getLogger(CloudProviderApiControllerTest.class);
  private static final ImmutableList<String> REGION_CODES_FROM_CLOUD_API =
      ImmutableList.of("region1", "region2");

  @Mock Config mockConfig;
  @Mock ConfigHelper mockConfigHelper;
  @Mock private play.Configuration appConfig;

  Customer customer;
  Users user;

  @Before
  public void setUp() {
    customer = ModelFactory.testCustomer();
    user = ModelFactory.testUser(customer);
    try {
      String kubeFile = createTempFile("test2.conf", "test5678");
      //      when(mockAccessManager.createKubernetesConfig(anyString(), anyMap(), anyBoolean()))
      //          .thenReturn(kubeFile);
    } catch (Exception e) {
      // Do nothing
    }
  }

  private Result listProviders() {
    return FakeApiHelper.doRequestWithAuthToken(
        "GET", "/api/customers/" + customer.uuid + "/providers", user.createAuthToken());
  }

  private Result createProvider(JsonNode bodyJson) {
    return FakeApiHelper.doRequestWithAuthTokenAndBody(
        "POST", "/api/customers/" + customer.uuid + "/providers", user.createAuthToken(), bodyJson);
  }

  private Result createKubernetesProvider(JsonNode bodyJson) {
    return FakeApiHelper.doRequestWithAuthTokenAndBody(
        "POST",
        "/api/customers/" + customer.uuid + "/providers/kubernetes",
        user.createAuthToken(),
        bodyJson);
  }

  private Result getKubernetesSuggestedConfig() {
    return FakeApiHelper.doRequestWithAuthToken(
        "GET",
        "/api/customers/" + customer.uuid + "/providers/suggested_kubernetes_config",
        user.createAuthToken());
  }

  private Result getProvider(UUID providerUUID) {
    return FakeApiHelper.doRequestWithAuthToken(
        "GET",
        "/api/customers/" + customer.uuid + "/providers/" + providerUUID,
        user.createAuthToken());
  }

  private Result deleteProvider(UUID providerUUID) {
    return FakeApiHelper.doRequestWithAuthToken(
        "DELETE",
        "/api/customers/" + customer.uuid + "/providers/" + providerUUID,
        user.createAuthToken());
  }

  private Result editProvider(JsonNode bodyJson, UUID providerUUID) {
    return FakeApiHelper.doRequestWithAuthTokenAndBody(
        "PUT",
        "/api/customers/" + customer.uuid + "/providers/" + providerUUID + "/edit",
        user.createAuthToken(),
        bodyJson);
  }

  private Result bootstrapProviderXX(JsonNode bodyJson, Provider provider) {
    return FakeApiHelper.doRequestWithAuthTokenAndBody(
        "POST",
        "/api/customers/" + customer.uuid + "/providers/" + provider.uuid + "/bootstrap",
        user.createAuthToken(),
        bodyJson);
  }

  //  @Test
  public void testListEmptyProviders() {
    Result result = listProviders();
    JsonNode json = Json.parse(contentAsString(result));

    assertOk(result);
    assertTrue(json.isArray());
    assertEquals(0, json.size());
    assertAuditEntry(0, customer.uuid);
  }

  //  @Test
  public void testListProviders() {
    Provider p1 = ModelFactory.awsProvider(customer);
    p1.setConfig(
        ImmutableMap.of("MY_KEY_DATA", "SENSITIVE_DATA", "MY_SECRET_DATA", "SENSITIVE_DATA"));
    p1.save();
    Provider p2 = ModelFactory.gcpProvider(customer);
    p2.setConfig(ImmutableMap.of("FOO", "BAR"));
    p2.save();
    Result result = listProviders();
    JsonNode json = Json.parse(contentAsString(result));

    assertOk(result);
    assertAuditEntry(0, customer.uuid);
    assertEquals(2, json.size());
    assertValues(json, "uuid", ImmutableList.of(p1.uuid.toString(), p2.uuid.toString()));
    assertValues(json, "name", ImmutableList.of(p1.name, p2.name));
    json.forEach(
        (providerJson) -> {
          JsonNode config = providerJson.get("config");
          if (UUID.fromString(providerJson.get("uuid").asText()).equals(p1.uuid)) {
            assertValue(config, "MY_KEY_DATA", "SE**********TA");
            assertValue(config, "MY_SECRET_DATA", "SE**********TA");
          } else {
            assertValue(config, "FOO", "BAR");
          }
        });
  }

  //  @Test
  public void testListProvidersWithValidCustomer() {
    Provider.create(UUID.randomUUID(), Common.CloudType.aws, "Amazon");
    Provider p = ModelFactory.gcpProvider(customer);
    Result result = listProviders();
    JsonNode json = Json.parse(contentAsString(result));

    assertOk(result);
    assertEquals(1, json.size());
    assertValues(json, "uuid", ImmutableList.of(p.uuid.toString()));
    assertValues(json, "name", ImmutableList.of(p.name));
    assertAuditEntry(0, customer.uuid);
  }

  @Test
  public void testCreateProvider() {
    createProviderTest(
        buildProviderReq("azu", "Microsoft"),
        ImmutableList.of("region1", "region2"),
        UUID.randomUUID());
  }

  @Test
  public void testCreateGCPProviderSomeRegionInput() {
    when(mockCloudQueryHelper.getCurrentHostInfo(eq(CloudType.gcp)))
        .thenReturn(Json.newObject().put("network", "234234").put("host_project", "PROJ"));
    Provider provider = buildProviderReq("gcp", "Google");
    Region region = new Region();
    region.name = "region1";
    region.provider = provider;
    region.code = "region1";
    provider.regions = ImmutableList.of(region);
    provider = createProviderTest(provider, ImmutableList.of(), UUID.randomUUID());
    assertNull(provider.destVpcId);
    assertNull(provider.hostVpcId);
  }

  @Test
  public void testCreateGCPProviderHostVPC() {
    when(mockCloudQueryHelper.getCurrentHostInfo(eq(CloudType.gcp)))
        .thenReturn(Json.newObject().put("network", "234234").put("host_project", "PROJ"));
    Provider provider = buildProviderReq("gcp", "Google");
    Map<String, String> reqConfig = new HashMap<>();
    reqConfig.put("use_host_vpc", "true");
    reqConfig.put("use_host_credentials", "true");
    CloudInfoInterface.setCloudProviderInfoFromConfig(provider, reqConfig);
    provider = createProviderTest(provider, ImmutableList.of("region1"), UUID.randomUUID());
    Map<String, String> config = CloudInfoInterface.fetchEnvVars(provider);
    assertEquals("234234", provider.hostVpcId);
    assertEquals("234234", provider.destVpcId);
    assertEquals("PROJ", config.get(GCPCloudImpl.GCE_PROJECT_PROPERTY));
    assertEquals("234234", config.get(GCPCloudImpl.CUSTOM_GCE_NETWORK_PROPERTY));
  }

  @Test
  public void testCreateAWSProviderHostVPC() {
    when(mockCloudQueryHelper.getCurrentHostInfo(eq(CloudType.aws)))
        .thenReturn(Json.newObject().put("vpc-id", "234234").put("region", "VPCreg"));
    Provider provider = buildProviderReq("aws", "AWS");
    provider = createProviderTest(provider, ImmutableList.of("region1"), UUID.randomUUID());
    assertEquals("234234", provider.hostVpcId);
    assertNull(provider.destVpcId);
    assertEquals("VPCreg", provider.hostVpcRegion);
  }

  @Test
  public void testCreateGCPProviderNoRegionInput() {
    createProviderTest(
        buildProviderReq("gcp", "Google"),
        ImmutableList.of("region1", "region2"),
        UUID.randomUUID());
  }

  @Test
  public void testAwsBootstrapWithDestVpcId() {
    Provider providerReq = buildProviderReq("aws", "Amazon");
    providerReq.destVpcId = "nofail";
    createProviderTest(providerReq, ImmutableList.of("region1", "region2"), UUID.randomUUID());
  }

  private Provider createProviderTest(
      Provider provider, ImmutableList<String> regionCodesFromCloudAPI, UUID actualTaskUUID) {
    JsonNode bodyJson = Json.toJson(provider);
    boolean isOnprem = CloudType.onprem.name().equals(provider.code);
    when(mockCommissioner.submit(any(TaskType.class), any(CloudBootstrap.Params.class)))
        .thenReturn(actualTaskUUID);
    if (!isOnprem) {
      when(mockCloudQueryHelper.getRegionCodes(provider)).thenReturn(regionCodesFromCloudAPI);
    }
    Result result = createProvider(bodyJson);
    assertOk(result);
    YBPTask ybpTask = Json.fromJson(Json.parse(contentAsString(result)), YBPTask.class);
    if (!isOnprem) {
      // When regions not supplied in request then we expect a call to cloud API to get region codes
      verify(mockCloudQueryHelper, times(provider.regions.isEmpty() ? 1 : 0)).getRegionCodes(any());
    }
    assertEquals(actualTaskUUID, ybpTask.taskUUID);
    Provider createdProvider = Provider.get(customer.uuid, ybpTask.resourceUUID);
    assertEquals(provider.code, createdProvider.code);
    assertEquals(provider.name, createdProvider.name);
    assertAuditEntry(1, customer.uuid);
    return createdProvider; // Note this is still partially created since our commissioner is fake.
  }

  private Provider buildProviderReq(String actualProviderCode, String actualProviderName) {
    Provider provider = new Provider();
    provider.code = actualProviderCode;
    provider.name = actualProviderName;
    provider.details = new ProviderDetails();
    return provider;
  }

  @Test
  public void testCreateMultiInstanceProvider() {
    ModelFactory.awsProvider(customer);
    createProviderTest(
        buildProviderReq("aws", "Amazon1"),
        ImmutableList.of("region1", "region2"),
        UUID.randomUUID());
  }

  @Test
  public void testCreateMultiInstanceProviderWithSameNameAndCloud() {
    ModelFactory.awsProvider(customer);
    Result result =
        assertPlatformException(
            () ->
                createProviderTest(
                    buildProviderReq("aws", "Amazon"),
                    ImmutableList.of("region1", "region2"),
                    UUID.randomUUID()));
    assertBadRequest(result, "Provider with the name Amazon already exists");
  }

  @Test
  public void testCreateMultiInstanceProviderWithSameNameButDifferentCloud() {
    ModelFactory.awsProvider(customer);
    createProviderTest(
        buildProviderReq("gcp", "Amazon1"),
        ImmutableList.of("region1", "region2"),
        UUID.randomUUID());
  }

  @Test
  public void testCreateProviderSameNameDiffCustomer() {
    Provider.create(UUID.randomUUID(), Common.CloudType.aws, "Amazon");
    createProviderTest(
        buildProviderReq("aws", "Amazon"), REGION_CODES_FROM_CLOUD_API, UUID.randomUUID());
  }

  @Test
  public void testCreateWithInvalidParams() {
    ObjectNode bodyJson = Json.newObject();
    bodyJson.put("code", "aws");
    Result result =
        assertPlatformException(
            () ->
                createProviderTest(
                    buildProviderReq("aws", null), REGION_CODES_FROM_CLOUD_API, UUID.randomUUID()));
    assertBadRequest(result, "\"name\":[\"error.required\"]}");
    assertAuditEntry(0, customer.uuid);
  }

  @Test
  @Parameters({"aws", "gcp", "onprem"})
  public void testCreateProviderWithConfig(String code) {
    String providerName = code + "-Provider";
    Provider providerReq = buildProviderReq(code, providerName);
    Map<String, String> reqConfig = new HashMap<>();
    when(mockCloudQueryHelper.getCurrentHostInfo(eq(CloudType.gcp)))
        .thenReturn(Json.newObject().put("network", "234234").put("host_project", "PROJ"));
    if (code.equals("gcp")) {
      // Note that we do not wrap the GCP config in API requests. Caller should do extracting
      // config file details and putting it in the config map
      reqConfig.put("use_host_credentials", "true");
      reqConfig.put("use_host_vpc", "true");
      reqConfig.put("host_project_id", "project");
      reqConfig.put("config_file_path", "/tmp/credentials.json");
    } else if (code.equals("aws")) {
      reqConfig.put("AWS_ACCESS_KEY_ID", "bar");
      reqConfig.put("AWS_SECRET_ACCESS_KEY", "bar2");
    } else {
      reqConfig.put("YB_HOME_DIR", "/bar");
    }
    providerReq.customerUUID = customer.uuid;
    CloudInfoInterface.setCloudProviderInfoFromConfig(providerReq, reqConfig);
    Provider createdProvider =
        createProviderTest(providerReq, REGION_CODES_FROM_CLOUD_API, UUID.randomUUID());
    Map<String, String> config = CloudInfoInterface.fetchEnvVars(createdProvider);
    assertFalse(config.isEmpty());
    if (code.equals("gcp")) {
      assertEquals(reqConfig.size() - 1, config.size());
    } else {
      assertEquals(reqConfig.size(), config.size());
    }
  }

  // Following tests wont be migrated because no k8s multi-region support:
  //  public void testCreateKubernetesMultiRegionProvider() {
  //  public void testCreateKubernetesMultiRegionProviderFailure() {
  //  public void testGetK8sSuggestedConfig() {
  //  public void testGetK8sSuggestedConfigWithoutPullSecret() {
  //  public void testGetKubernetesConfigsDiscoveryFailure() {

  //  @Test
  public void testDeleteProviderWithAccessKey() {
    Provider p = ModelFactory.awsProvider(customer);
    AccessKey ak = AccessKey.create(p.uuid, "access-key-code", new AccessKey.KeyInfo());
    Result result = deleteProvider(p.uuid);
    assertYBPSuccess(result, "Deleted provider: " + p.uuid);
    assertEquals(0, AccessKey.getAll(p.uuid).size());
    assertNull(Provider.get(p.uuid));
    verify(mockAccessManager, times(1))
        .deleteKeyByProvider(p, ak.getKeyCode(), ak.getKeyInfo().deleteRemote);
    assertAuditEntry(1, customer.uuid);
  }

  //  @Test
  public void testDeleteProviderWithInstanceType() {
    Provider p = ModelFactory.onpremProvider(customer);

    when(mockConfigHelper.getAWSInstancePrefixesSupported())
        .thenReturn(ImmutableList.of("m3.", "c5.", "c5d.", "c4.", "c3.", "i3."));
    ObjectNode metaData = Json.newObject();
    metaData.put("numCores", 4);
    metaData.put("memSizeGB", 300);
    InstanceType.InstanceTypeDetails instanceTypeDetails = new InstanceType.InstanceTypeDetails();
    instanceTypeDetails.volumeDetailsList = new ArrayList<>();
    InstanceType.VolumeDetails volumeDetails = new InstanceType.VolumeDetails();
    volumeDetails.volumeSizeGB = 20;
    volumeDetails.volumeType = InstanceType.VolumeType.SSD;
    instanceTypeDetails.volumeDetailsList.add(volumeDetails);
    metaData.put("longitude", -119.417932);
    metaData.put("ybImage", "yb-image-1");
    metaData.set("instanceTypeDetails", Json.toJson(instanceTypeDetails));

    InstanceType.createWithMetadata(p.uuid, "region-1", metaData);
    AccessKey ak = AccessKey.create(p.uuid, "access-key-code", new AccessKey.KeyInfo());
    Result result = deleteProvider(p.uuid);
    assertYBPSuccess(result, "Deleted provider: " + p.uuid);

    assertEquals(0, InstanceType.findByProvider(p, mockConfig, mockConfigHelper).size());
    assertNull(Provider.get(p.uuid));
  }

  //  @Test
  public void testDeleteProviderWithMultiRegionAccessKey() {
    Provider p = ModelFactory.awsProvider(customer);
    AccessKey ak = AccessKey.create(p.uuid, "access-key-code", new AccessKey.KeyInfo());
    Result result = deleteProvider(p.uuid);
    assertYBPSuccess(result, "Deleted provider: " + p.uuid);
    assertEquals(0, AccessKey.getAll(p.uuid).size());
    assertNull(Provider.get(p.uuid));
    verify(mockAccessManager, times(1))
        .deleteKeyByProvider(p, ak.getKeyCode(), ak.getKeyInfo().deleteRemote);
    assertAuditEntry(1, customer.uuid);
  }

  //  @Test
  public void testDeleteProviderWithInvalidProviderUUID() {
    UUID providerUUID = UUID.randomUUID();
    Result result = assertPlatformException(() -> deleteProvider(providerUUID));
    assertBadRequest(result, "Invalid Provider UUID: " + providerUUID);
    assertAuditEntry(0, customer.uuid);
  }

  //  @Test
  public void testDeleteProviderWithUniverses() {
    Provider p = ModelFactory.awsProvider(customer);
    Universe universe = createUniverse(customer.getCustomerId());
    UniverseDefinitionTaskParams.UserIntent userIntent =
        new UniverseDefinitionTaskParams.UserIntent();
    userIntent.provider = p.uuid.toString();
    Region r = Region.create(p, "region-1", "PlacementRegion 1", "default-image");
    AvailabilityZone az1 = AvailabilityZone.createOrThrow(r, "az-1", "PlacementAZ 1", "subnet-1");
    AvailabilityZone az2 = AvailabilityZone.createOrThrow(r, "az-2", "PlacementAZ 2", "subnet-2");
    userIntent.regionList = new ArrayList<>();
    userIntent.regionList.add(r.uuid);
    universe =
        Universe.saveDetails(universe.universeUUID, ApiUtils.mockUniverseUpdater(userIntent));
    Result result = assertPlatformException(() -> deleteProvider(p.uuid));
    assertBadRequest(result, "Cannot delete Provider with Universes");
    assertAuditEntry(0, customer.uuid);
  }

  //  @Test
  public void testDeleteProviderWithoutAccessKey() {
    Provider p = ModelFactory.awsProvider(customer);
    Result result = deleteProvider(p.uuid);
    assertYBPSuccess(result, "Deleted provider: " + p.uuid);
    assertNull(Provider.get(p.uuid));
    assertAuditEntry(1, customer.uuid);
  }

  //  @Test
  public void testDeleteProviderWithProvisionScript() {
    Provider p = ModelFactory.newProvider(customer, Common.CloudType.onprem);
    AccessKey.KeyInfo keyInfo = new AccessKey.KeyInfo();
    String scriptFile = createTempFile("provision_instance.py", "some script");
    keyInfo.provisionInstanceScript = scriptFile;
    AccessKey.create(p.uuid, "access-key-code", keyInfo);
    Result result = deleteProvider(p.uuid);
    assertOk(result);
    assertFalse(new File(scriptFile).exists());
    assertAuditEntry(1, customer.uuid);
  }

  //  @Test
  public void testCreateAwsProviderWithInvalidAWSCredentials() {
    ObjectNode bodyJson = Json.newObject();
    bodyJson.put("code", "aws");
    bodyJson.put("name", "aws-Provider");
    bodyJson.put("region", "ap-south-1");
    ObjectNode detailsJson = Json.newObject();
    ObjectNode CloudInfoJson = Json.newObject();
    CloudInfoJson.put("AWS_ACCESS_KEY_ID", "test");
    CloudInfoJson.put("AWS_SECRET_ACCESS_KEY", "secret");
    CloudInfoJson.put("AWS_HOSTED_ZONE_ID", "1234");
    detailsJson.set("cloudInfo", CloudInfoJson);
    bodyJson.set("details", detailsJson);
    CloudAPI mockCloudAPI = mock(CloudAPI.class);
    when(mockCloudAPIFactory.get(any())).thenReturn(mockCloudAPI);
    Result result = assertPlatformException(() -> createProvider(bodyJson));
    assertBadRequest(result, "Invalid AWS Credentials.");
    assertAuditEntry(0, customer.uuid);
  }

  @Test
  public void testCreateAwsProviderWithInvalidDevopsReply() {
    ObjectNode bodyJson = Json.newObject();
    bodyJson.put("code", "aws");
    bodyJson.put("name", "aws-Provider");
    ObjectNode detailsJson = Json.newObject();
    ObjectNode CloudInfoJson = Json.newObject();
    ObjectNode awsCloudInfoJson = Json.newObject();
    awsCloudInfoJson.put("HOSTED_ZONE_ID", "1234");
    CloudInfoJson.set("aws", awsCloudInfoJson);
    detailsJson.set("cloudInfo", CloudInfoJson);
    bodyJson.set("details", detailsJson);
    mockDnsManagerListFailure("fail", 0);
    Result result = assertPlatformException(() -> createProvider(bodyJson));
    verify(mockDnsManager, times(1)).listDnsRecord(any(), any());
    assertInternalServerError(result, "Invalid devops API response: ");
    assertAuditEntry(0, customer.uuid);
  }

  @Test
  public void testCreateAwsProviderWithInvalidHostedZoneId() {
    ObjectNode bodyJson = Json.newObject();
    bodyJson.put("code", "aws");
    bodyJson.put("name", "aws-Provider");
    ObjectNode detailsJson = Json.newObject();
    ObjectNode CloudInfoJson = Json.newObject();
    ObjectNode awsCloudInfoJson = Json.newObject();
    awsCloudInfoJson.put("HOSTED_ZONE_ID", "1234");
    CloudInfoJson.set("aws", awsCloudInfoJson);
    detailsJson.set("cloudInfo", CloudInfoJson);
    bodyJson.set("details", detailsJson);

    mockDnsManagerListFailure("fail", 1);
    Result result = assertPlatformException(() -> createProvider(bodyJson));
    verify(mockDnsManager, times(1)).listDnsRecord(any(), any());
    assertInternalServerError(result, "Invalid devops API response: ");
    assertAuditEntry(0, customer.uuid);
  }

  private void mockDnsManagerListSuccess() {
    mockDnsManagerListSuccess("test");
  }

  private void mockDnsManagerListSuccess(String mockDnsName) {
    ShellResponse shellResponse = new ShellResponse();
    shellResponse.message = "{\"name\": \"" + mockDnsName + "\"}";
    shellResponse.code = 0;
    when(mockDnsManager.listDnsRecord(any(), any())).thenReturn(shellResponse);
  }

  private void mockDnsManagerListFailure(String mockFailureMessage, int successCode) {
    ShellResponse shellResponse = new ShellResponse();
    shellResponse.message = "{\"wrong_key\": \"" + mockFailureMessage + "\"}";
    shellResponse.code = successCode;
    when(mockDnsManager.listDnsRecord(any(), any())).thenReturn(shellResponse);
  }

  @Test
  public void testAddRegion() {
    when(mockCommissioner.submit(any(TaskType.class), any(CloudBootstrap.Params.class)))
        .thenReturn(UUID.randomUUID());
    Provider provider = Provider.create(customer.uuid, Common.CloudType.aws, "test");
    AccessKey.create(provider.uuid, AccessKey.getDefaultKeyCode(provider), new AccessKey.KeyInfo());
    String jsonString =
        String.format(
            "{\"code\":\"aws\",\"name\":\"test\",\"regions\":[{\"name\":\"us-west-1\""
                + ",\"code\":\"us-west-1\", \"details\": {\"cloudInfo\": { \"aws\": {"
                + "\"vnetName\":\"vpc-foo\","
                + "\"securityGroupId\":\"sg-foo\" }}}, "
                + "\"zones\":[{\"code\":\"us-west-1a\",\"name\":\"us-west-1a\","
                + "\"secondarySubnet\":\"subnet-foo\",\"subnet\":\"subnet-foo\"}]}],"
                + "\"version\": %d}",
            provider.getVersion());
    Result result = editProvider(Json.parse(jsonString), provider.uuid);
    assertOk(result);
  }

  @Test
  public void testAddExistingRegionFail() {
    when(mockCommissioner.submit(any(TaskType.class), any(CloudBootstrap.Params.class)))
        .thenReturn(UUID.randomUUID());
    ProviderDetails providerDetails = new ProviderDetails();
    Provider provider =
        Provider.create(customer.uuid, Common.CloudType.aws, "test", providerDetails);
    Region.create(provider, "us-west-1", "us-west-1", "foo");
    String jsonString =
        String.format(
            "{\"code\":\"aws\",\"name\":\"test\",\"regions\":[{\"name\":\"us-west-1\""
                + ",\"code\":\"us-west-1\", \"details\": {\"cloudInfo\": { \"aws\": {"
                + "\"vnetName\":\"vpc-foo\","
                + "\"securityGroupId\":\"sg-foo\" }}}, "
                + "\"zones\":[{\"code\":\"us-west-1a\",\"name\":\"us-west-1a\","
                + "\"secondarySubnet\":\"subnet-foo\",\"subnet\":\"subnet-foo\"}]}],"
                + "\"version\": %d}",
            provider.getVersion());
    Result result =
        assertPlatformException(() -> editProvider(Json.parse(jsonString), provider.uuid));
    assertBadRequest(result, "No changes to be made for provider type: aws");
  }

  @Test
  public void testIncorrectFieldsForAddRegionFail() {
    when(mockCommissioner.submit(any(TaskType.class), any(CloudBootstrap.Params.class)))
        .thenReturn(UUID.randomUUID());
    Provider provider = Provider.create(customer.uuid, Common.CloudType.aws, "test");
    AccessKey.create(provider.uuid, AccessKey.getDefaultKeyCode(provider), new AccessKey.KeyInfo());
    String jsonString =
        "{\"code\":\"aws\",\"name\":\"test\",\"regions\":[{\"name\":\"us-west-1\""
            + ",\"code\":\"us-west-1\", \"details\": {\"cloudInfo\": { \"aws\": {"
            + "\"securityGroupId\":\"sg-foo\" }}}, "
            + "\"zones\":[{\"code\":\"us-west-1a\",\"name\":\"us-west-1a\","
            + "\"secondarySubnet\":\"subnet-foo\",\"subnet\":\"subnet-foo\"}]}]}";

    Result result =
        assertPlatformException(() -> editProvider(Json.parse(jsonString), provider.uuid));
    assertBadRequest(result, "Required field vnet name (VPC ID) for region: us-west-1");
  }

  @Test
  public void testAddRegionNoAccessKeyFail() {
    when(mockCommissioner.submit(any(TaskType.class), any(CloudBootstrap.Params.class)))
        .thenReturn(UUID.randomUUID());
    Provider provider = Provider.create(customer.uuid, Common.CloudType.aws, "test");
    String jsonString =
        "{\"code\":\"aws\",\"name\":\"test\",\"regions\":[{\"name\":\"us-west-1\""
            + ",\"code\":\"us-west-1\", \"details\": {\"cloudInfo\": { \"aws\": {"
            + "\"securityGroupId\":\"sg-foo\" }}}, "
            + "\"zones\":[{\"code\":\"us-west-1a\",\"name\":\"us-west-1a\","
            + "\"secondarySubnet\":\"subnet-foo\",\"subnet\":\"subnet-foo\"}]}]}";

    Result result =
        assertPlatformException(() -> editProvider(Json.parse(jsonString), provider.uuid));
    assertBadRequest(result, "KeyCode not found: " + AccessKey.getDefaultKeyCode(provider));
  }
}

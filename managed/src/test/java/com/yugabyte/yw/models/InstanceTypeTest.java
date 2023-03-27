// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.models;

import static com.yugabyte.yw.common.AssertHelper.assertValue;
import static com.yugabyte.yw.common.AssertHelper.assertValueAtPath;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.AllOf.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.yugabyte.yw.common.ConfigHelper;
import com.yugabyte.yw.common.FakeDBApplication;
import com.yugabyte.yw.common.ModelFactory;
import com.yugabyte.yw.models.InstanceType.VolumeType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import play.libs.Json;

@RunWith(MockitoJUnitRunner.class)
public class InstanceTypeTest extends FakeDBApplication {
  private Provider defaultProvider;
  private Provider onpremProvider;
  private Customer defaultCustomer;
  private InstanceType.InstanceTypeDetails defaultDetails;

  @Mock Config mockConfig;
  @Mock ConfigHelper mockConfigHelper;

  @Before
  public void setUp() {
    defaultCustomer = ModelFactory.testCustomer();
    defaultProvider = ModelFactory.awsProvider(defaultCustomer);
    onpremProvider = ModelFactory.onpremProvider(defaultCustomer);
    InstanceType.VolumeDetails volumeDetails = new InstanceType.VolumeDetails();
    volumeDetails.volumeSizeGB = 100;
    volumeDetails.volumeType = InstanceType.VolumeType.EBS;
    defaultDetails = new InstanceType.InstanceTypeDetails();
    defaultDetails.volumeDetailsList.add(volumeDetails);
    defaultDetails.setDefaultMountPaths();
  }

  @Test
  public void testCreate() {
    InstanceType i1 = InstanceType.upsert(defaultProvider.uuid, "foo", 3, 10.0, defaultDetails);
    assertNotNull(i1);
    assertEquals("foo", i1.getInstanceTypeCode());
  }

  @Test
  public void testGetNonDefaultInstanceTypeDetails() {
    int volumeCount = 3;
    int volumeSizeGB = 100;
    VolumeType volumeType = VolumeType.SSD;
    InstanceType.InstanceTypeDetails itDetails = new InstanceType.InstanceTypeDetails();
    itDetails.setVolumeDetailsList(volumeCount, volumeSizeGB, volumeType);
    assertNotNull(itDetails.volumeDetailsList);
    for (int i = 0; i < volumeCount; i++) {
      InstanceType.VolumeDetails v = itDetails.volumeDetailsList.get(i);
      assertEquals(volumeSizeGB, v.volumeSizeGB.intValue());
      assertEquals(volumeType, v.volumeType);
      assertEquals(String.format("/mnt/d%d", i), v.mountPath);
    }
  }

  @Test
  public void testGetDefaultInstanceTypeDetails() {
    InstanceType.InstanceTypeDetails itDetails =
        InstanceType.InstanceTypeDetails.createGCPDefault();
    assertNotNull(itDetails);
    assertNotNull(itDetails.volumeDetailsList);
    for (int i = 0; i < InstanceType.InstanceTypeDetails.DEFAULT_VOLUME_COUNT; i++) {
      InstanceType.VolumeDetails v = itDetails.volumeDetailsList.get(i);
      assertEquals(
          InstanceType.InstanceTypeDetails.DEFAULT_GCP_VOLUME_SIZE_GB, v.volumeSizeGB.intValue());
      assertEquals(InstanceType.VolumeType.SSD, v.volumeType);
      assertEquals(String.format("/mnt/d%d", i), v.mountPath);
    }
  }

  @Test
  public void testFindByProvider() {
    Provider newProvider = ModelFactory.gcpProvider(defaultCustomer);
    when(mockConfigHelper.getAWSInstancePrefixesSupported())
        .thenReturn(ImmutableList.of("m3.", "c5.", "c5d.", "c4.", "c3.", "i3."));
    InstanceType.upsert(defaultProvider.uuid, "c3.medium", 3, 10.0, defaultDetails);
    InstanceType.upsert(defaultProvider.uuid, "c3.large", 3, 10.0, defaultDetails);
    InstanceType.upsert(defaultProvider.uuid, "c3.xlarge", 3, 10.0, defaultDetails);
    InstanceType instanceType = InstanceType.get(defaultProvider.uuid, "c3.xlarge");
    instanceType.setActive(false);
    instanceType.save();
    InstanceType.upsert(newProvider.uuid, "bar", 2, 10.0, defaultDetails);
    List<InstanceType> instanceTypeList =
        InstanceType.findByProvider(defaultProvider, mockConfig, mockConfigHelper);
    assertNotNull(instanceTypeList);
    assertEquals(2, instanceTypeList.size());
    Set<String> possibleTypes = new HashSet<>();
    possibleTypes.add("c3.medium");
    possibleTypes.add("c3.large");
    for (InstanceType it : instanceTypeList) {
      assertTrue(possibleTypes.contains(it.getInstanceTypeCode()));
      assertNotNull(it.instanceTypeDetails);
    }
  }

  @Test
  public void testFindByProviderOnprem() {
    InstanceType.upsert(onpremProvider.uuid, "bar", 2, 10.0, defaultDetails);
    List<InstanceType> instanceTypeList =
        InstanceType.findByProvider(onpremProvider, mockConfig, mockConfigHelper);
    assertEquals(1, instanceTypeList.size());

    InstanceType it = instanceTypeList.get(0);
    assertTrue(it.getInstanceTypeCode().equals("bar"));
    assertNotNull(it.instanceTypeDetails);
  }

  @Test
  public void testFindByProviderWithUnSupportedInstances() {
    when(mockConfigHelper.getAWSInstancePrefixesSupported())
        .thenReturn(ImmutableList.of("m3.", "c5.", "c5d.", "c4.", "c3.", "i3."));
    InstanceType.upsert(defaultProvider.uuid, "t2.medium", 3, 10.0, defaultDetails);
    InstanceType.upsert(defaultProvider.uuid, "c3.medium", 2, 10.0, defaultDetails);
    List<InstanceType> instanceTypeList =
        InstanceType.findByProvider(defaultProvider, mockConfig, mockConfigHelper);
    assertNotNull(instanceTypeList);
    assertEquals(1, instanceTypeList.size());
    assertThat(
        instanceTypeList.get(0).getInstanceTypeCode(), allOf(notNullValue(), equalTo("c3.medium")));
  }

  @Test
  public void testFindByProviderWithEmptyInstanceTypeDetails() {
    InstanceType.upsert(
        defaultProvider.uuid, "c5.medium", 3, 10.0, new InstanceType.InstanceTypeDetails());
    when(mockConfig.getInt(InstanceType.YB_AWS_DEFAULT_VOLUME_COUNT_KEY)).thenReturn(1);
    when(mockConfig.getInt(InstanceType.YB_AWS_DEFAULT_VOLUME_SIZE_GB_KEY)).thenReturn(250);
    when(mockConfigHelper.getAWSInstancePrefixesSupported())
        .thenReturn(ImmutableList.of("m3.", "c5.", "c5d.", "c4.", "c3.", "i3."));
    List<InstanceType> instanceTypeList =
        InstanceType.findByProvider(defaultProvider, mockConfig, mockConfigHelper);
    assertNotNull(instanceTypeList);
    InstanceType.VolumeDetails volumeDetails =
        instanceTypeList.get(0).instanceTypeDetails.volumeDetailsList.get(0);
    assertEquals(250, volumeDetails.volumeSizeGB.intValue());
    assertEquals(InstanceType.VolumeType.EBS, volumeDetails.volumeType);
    assertEquals(String.format("/mnt/d%d", 0), volumeDetails.mountPath);
    assertThat(
        instanceTypeList.get(0).instanceTypeDetails.volumeDetailsList.size(),
        allOf(notNullValue(), equalTo(1)));
  }

  @Test
  public void testFindByProviderWithNullInstanceTypeDetails() {
    InstanceType.upsert(defaultProvider.uuid, "c5.medium", 3, 10.0, null);
    when(mockConfig.getInt(InstanceType.YB_AWS_DEFAULT_VOLUME_COUNT_KEY)).thenReturn(1);
    when(mockConfig.getInt(InstanceType.YB_AWS_DEFAULT_VOLUME_SIZE_GB_KEY)).thenReturn(250);
    when(mockConfigHelper.getAWSInstancePrefixesSupported())
        .thenReturn(ImmutableList.of("m3.", "c5.", "c5d.", "c4.", "c3.", "i3."));
    List<InstanceType> instanceTypeList =
        InstanceType.findByProvider(defaultProvider, mockConfig, mockConfigHelper);
    assertNotNull(instanceTypeList);
    InstanceType.VolumeDetails volumeDetails =
        instanceTypeList.get(0).instanceTypeDetails.volumeDetailsList.get(0);
    assertEquals(250, volumeDetails.volumeSizeGB.intValue());
    assertEquals(InstanceType.VolumeType.EBS, volumeDetails.volumeType);
    assertEquals(String.format("/mnt/d%d", 0), volumeDetails.mountPath);
    assertThat(
        instanceTypeList.get(0).instanceTypeDetails.volumeDetailsList.size(),
        allOf(notNullValue(), equalTo(1)));
  }

  @Test
  public void testFindByKeys() {
    InstanceType medium = InstanceType.upsert(defaultProvider.uuid, "c5.medium", 3, 10.0, null);
    InstanceType large = InstanceType.upsert(defaultProvider.uuid, "c5.large", 3, 10.0, null);
    InstanceType xlarge = InstanceType.upsert(defaultProvider.uuid, "c5.xlarge", 3, 10.0, null);
    List<InstanceType> instanceTypeList =
        InstanceType.findByKeys(
            ImmutableList.of(
                new InstanceTypeKey()
                    .setProviderUuid(defaultProvider.uuid)
                    .setInstanceTypeCode("c5.medium"),
                new InstanceTypeKey()
                    .setProviderUuid(defaultProvider.uuid)
                    .setInstanceTypeCode("c5.large")));
    assertNotNull(instanceTypeList);
    assertThat(instanceTypeList, Matchers.containsInAnyOrder(medium, large));
  }

  @Test
  public void testDeleteByProvider() {
    Provider newProvider = ModelFactory.gcpProvider(defaultCustomer);
    InstanceType.upsert(newProvider.uuid, "bar", 2, 10.0, defaultDetails);
    InstanceType.deleteInstanceTypesForProvider(newProvider, mockConfig, mockConfigHelper);
    List<InstanceType> instanceTypeList =
        InstanceType.findByProvider(newProvider, mockConfig, mockConfigHelper);
    assertEquals(0, instanceTypeList.size());
  }

  @Test
  public void testGravitonProvider() {
    when(mockConfigHelper.getAWSInstancePrefixesSupported())
        .thenReturn(ImmutableList.of("m3.", "c5.", "c5d.", "c4.", "c3.", "i3.", "m6g."));
    InstanceType.upsert(defaultProvider.uuid, "t2.medium", 3, 10.0, defaultDetails);
    InstanceType.upsert(defaultProvider.uuid, "m6g.small", 2, 10.0, defaultDetails);
    List<InstanceType> instanceTypeList =
        InstanceType.findByProvider(defaultProvider, mockConfig, mockConfigHelper);
    assertNotNull(instanceTypeList);
    assertEquals(1, instanceTypeList.size());
    assertThat(
        instanceTypeList.get(0).getInstanceTypeCode(), allOf(notNullValue(), equalTo("m6g.small")));
  }

  @Test
  public void testCreateWithValidMetadata() {
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
    InstanceType it = InstanceType.createWithMetadata(defaultProvider.uuid, "it-1", metaData);
    assertNotNull(it);
    JsonNode itJson = Json.toJson(it);
    assertValueAtPath(itJson, "/idKey/providerUuid", defaultProvider.uuid.toString());
    assertValueAtPath(itJson, "/idKey/instanceTypeCode", "it-1");
    assertValue(itJson, "numCores", "4.0");
    assertValue(itJson, "memSizeGB", "300.0");
    JsonNode volumeDetailsList = itJson.get("instanceTypeDetails").get("volumeDetailsList");
    assertTrue(volumeDetailsList.isArray());
    assertEquals(1, volumeDetailsList.size());
    assertValue(volumeDetailsList.get(0), "volumeSizeGB", "20");
    assertValue(volumeDetailsList.get(0), "volumeType", "SSD");
  }
}

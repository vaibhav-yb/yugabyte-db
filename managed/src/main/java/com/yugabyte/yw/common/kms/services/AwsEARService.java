/*
 * Copyright 2019 YugaByte, Inc. and Contributors
 *
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *     https://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

package com.yugabyte.yw.common.kms.services;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.yugabyte.yw.common.config.RuntimeConfGetter;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.common.kms.algorithms.AwsAlgorithm;
import com.yugabyte.yw.common.kms.util.AwsEARServiceUtil;
import com.yugabyte.yw.common.kms.util.KeyProvider;
import com.yugabyte.yw.common.kms.util.AwsEARServiceUtil.AwsKmsAuthConfigField;
import com.yugabyte.yw.forms.EncryptionAtRestConfig;
import com.yugabyte.yw.models.Universe;

import java.util.UUID;

/**
 * An implementation of EncryptionAtRestService to communicate with AWS KMS
 * https://aws.amazon.com/kms/
 */
public class AwsEARService extends EncryptionAtRestService<AwsAlgorithm> {

  private final RuntimeConfGetter confGetter;

  public AwsEARService(RuntimeConfGetter confGetter) {
    super(KeyProvider.AWS);
    this.confGetter = confGetter;
  }

  private byte[] generateUniverseDataKey(
      UUID configUUID, UUID universeUUID, String algorithm, int keySize, String cmkId) {
    byte[] result = null;
    try {
      final ObjectNode validateResult = validateEncryptionKeyParams(algorithm, keySize);
      if (!validateResult.get("result").asBoolean()) {
        final String errMsg =
            String.format(
                "Invalid encryption key parameters detected for create/rotate data key"
                    + " operation in universe %s: %s",
                universeUUID, validateResult.get("errors").asText());
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      result = AwsEARServiceUtil.generateDataKey(configUUID, cmkId, algorithm, keySize);
    } catch (Exception e) {
      LOG.error(
          String.format("Error generating universe key for universe %s", universeUUID.toString()));
    }
    return result;
  }

  @Override
  protected AwsAlgorithm[] getSupportedAlgorithms() {
    return AwsAlgorithm.values();
  }

  @Override
  protected ObjectNode createAuthConfigWithService(UUID configUUID, ObjectNode config) {
    // Skip creating a CMK for the KMS Configuration if the user inputted one
    if (config.get(AwsKmsAuthConfigField.CMK_ID.fieldName) != null) return config;
    final String description =
        String.format("Yugabyte Master Key for KMS Configuration %s", configUUID.toString());
    ObjectNode result = null;
    try {
      final String inputtedCMKPolicy =
          config.get(AwsKmsAuthConfigField.CMK_POLICY.fieldName) == null
              ? null
              : config.get(AwsKmsAuthConfigField.CMK_POLICY.fieldName).asText();
      final String cmkId =
          AwsEARServiceUtil.createCMK(configUUID, description, inputtedCMKPolicy)
              .getKeyMetadata()
              .getKeyId();
      if (cmkId != null) {
        config.remove(AwsKmsAuthConfigField.CMK_POLICY.fieldName);
        config.put(AwsKmsAuthConfigField.CMK_ID.fieldName, cmkId);
      }
      result = config;
    } catch (Exception e) {
      final String errMsg =
          String.format(
              "Error attempting to create CMK with AWS KMS with config %s", configUUID.toString());
      LOG.error(errMsg, e);
    }
    return result;
  }

  @Override
  protected byte[] createKeyWithService(
      UUID universeUUID, UUID configUUID, EncryptionAtRestConfig config) {
    byte[] result = null;
    final String cmkId = AwsEARServiceUtil.getCMKId(configUUID);
    if (cmkId != null) {
      // Skip for YBM use case
      boolean cloudEnabled =
          confGetter.getConfForScope(
              Universe.getOrBadRequest(universeUUID), UniverseConfKeys.cloudEnabled);
      if (!cloudEnabled) {
        // Ensure an alias exists from KMS CMK to universe UUID for all YBA use cases
        AwsEARServiceUtil.createOrUpdateCMKAlias(configUUID, cmkId, universeUUID.toString());
      }
      switch (config.type) {
        case CMK:
          result = AwsEARServiceUtil.getCMK(configUUID, cmkId).getKeyArn().getBytes();
          break;
        default:
        case DATA_KEY:
          String algorithm = "AES";
          int keySize = 256;
          result = generateUniverseDataKey(configUUID, universeUUID, algorithm, keySize, cmkId);
          break;
      }
    }
    return result;
  }

  @Override
  protected byte[] rotateKeyWithService(
      UUID universeUUID, UUID configUUID, EncryptionAtRestConfig config) {
    byte[] result = null;
    final String cmkId = AwsEARServiceUtil.getCMKId(configUUID);
    if (cmkId != null) {
      // Skip for YBM use case
      boolean cloudEnabled =
          confGetter.getConfForScope(
              Universe.getOrBadRequest(universeUUID), UniverseConfKeys.cloudEnabled);
      if (!cloudEnabled) {
        // Ensure an alias exists from KMS CMK to universe UUID
        AwsEARServiceUtil.createOrUpdateCMKAlias(configUUID, cmkId, universeUUID.toString());
      }
      String algorithm = "AES";
      int keySize = 256;
      result = generateUniverseDataKey(configUUID, universeUUID, algorithm, keySize, cmkId);
    }
    return result;
  }

  @Override
  public byte[] retrieveKeyWithService(
      UUID universeUUID, UUID configUUID, byte[] keyRef, EncryptionAtRestConfig config) {
    byte[] keyVal = null;
    try {
      switch (config.type) {
        case CMK:
          keyVal = keyRef;
          break;
        default:
        case DATA_KEY:
          keyVal = AwsEARServiceUtil.decryptUniverseKey(configUUID, keyRef, null);
          if (keyVal == null) {
            LOG.warn("Could not retrieve key from key ref through AWS KMS");
          }
          break;
      }
    } catch (Exception e) {
      final String errMsg = "Error occurred retrieving encryption key";
      LOG.error(errMsg, e);
      throw new RuntimeException(errMsg, e);
    }
    return keyVal;
  }

  @Override
  protected byte[] validateRetrieveKeyWithService(
      UUID universeUUID,
      UUID configUUID,
      byte[] keyRef,
      EncryptionAtRestConfig config,
      ObjectNode authConfig) {
    byte[] keyVal = null;
    try {
      switch (config.type) {
        case CMK:
          keyVal = keyRef;
          break;
        default:
        case DATA_KEY:
          keyVal = AwsEARServiceUtil.decryptUniverseKey(configUUID, keyRef, authConfig);
          if (keyVal == null) {
            LOG.warn("Could not retrieve key from key ref through AWS KMS");
          }
          break;
      }
    } catch (Exception e) {
      final String errMsg = "Error occurred retrieving encryption key";
      LOG.error(errMsg, e);
      throw new RuntimeException(errMsg, e);
    }
    return keyVal;
  }

  @Override
  protected void cleanupWithService(UUID universeUUID, UUID configUUID) {
    // Skip and do nothing for YBM use case
    boolean cloudEnabled =
        confGetter.getConfForScope(
            Universe.getOrBadRequest(universeUUID), UniverseConfKeys.cloudEnabled);
    if (!cloudEnabled) {
      final String aliasName = AwsEARServiceUtil.generateAliasName(universeUUID.toString());
      if (AwsEARServiceUtil.getAlias(configUUID, aliasName) != null) {
        AwsEARServiceUtil.deleteAlias(configUUID, aliasName);
      }
    }
  }
}

/*
 * Copyright 2019 YugaByte, Inc. and Contributors
 *
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *     https://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

package com.yugabyte.yw.common.kms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.yugabyte.yw.common.config.RuntimeConfGetter;
import com.yugabyte.yw.common.kms.algorithms.SupportedAlgorithmInterface;
import com.yugabyte.yw.common.kms.services.EncryptionAtRestService;
import com.yugabyte.yw.common.kms.util.EncryptionAtRestUtil;
import com.yugabyte.yw.common.kms.util.EncryptionAtRestUtil.BackupEntry;
import com.yugabyte.yw.common.utils.FileUtils;
import com.yugabyte.yw.common.kms.util.KeyProvider;
import com.yugabyte.yw.forms.EncryptionAtRestConfig;
import com.yugabyte.yw.models.KmsConfig;
import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.libs.Json;

@Singleton
public class EncryptionAtRestManager {
  public static final Logger LOG = LoggerFactory.getLogger(EncryptionAtRestManager.class);
  private final RuntimeConfGetter confGetter;

  @Inject
  public EncryptionAtRestManager(RuntimeConfGetter confGetter) {
    this.confGetter = confGetter;
  }

  public enum RestoreKeyResult {
    RESTORE_SKIPPED,
    RESTORE_FAILED,
    RESTORE_SUCCEEDED;
  }

  public <T extends EncryptionAtRestService<? extends SupportedAlgorithmInterface>>
      T getServiceInstance(String keyProvider) {
    return getServiceInstance(keyProvider, false);
  }

  public <T extends EncryptionAtRestService<? extends SupportedAlgorithmInterface>>
      T getServiceInstance(String keyProvider, boolean forceNewInstance) {
    KeyProvider serviceProvider = null;
    T serviceInstance;
    try {
      for (KeyProvider providerImpl : KeyProvider.values()) {
        if (providerImpl.name().equals(keyProvider)) {
          serviceProvider = providerImpl;
          break;
        }
      }
      if (serviceProvider == null) {
        final String errMsg =
            String.format("Encryption service provider %s is not supported", keyProvider);
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }

      serviceInstance = serviceProvider.getServiceInstance();
      if (forceNewInstance || serviceInstance == null) {
        final Class<T> serviceClass = serviceProvider.getProviderService();

        if (serviceClass == null) {
          final String errMsg =
              String.format(
                  "Encryption service provider %s has not been implemented yet", keyProvider);
          LOG.error(errMsg);
          throw new IllegalArgumentException(errMsg);
        }

        final Constructor<T> serviceConstructor = EncryptionAtRestUtil.getConstructor(serviceClass);

        if (serviceConstructor == null) {
          final String errMsg =
              String.format(
                  "No suitable public constructor found for service provider %s", keyProvider);
          LOG.error(errMsg);
          throw new InstantiationException(errMsg);
        }

        // Not able to inject this at the lower level class such as AWSEarService.java bcs it is
        // instantiated separately. Hence we have to pass it down from here.
        serviceInstance = serviceConstructor.newInstance(confGetter);
        serviceProvider.setServiceInstance(serviceInstance);
      }
    } catch (Exception e) {
      final String errMsg =
          String.format(
              "Error occurred attempting to retrieve encryption key service for "
                  + "key provider %s",
              keyProvider);
      LOG.error(errMsg, e);
      serviceInstance = null;
    }
    return serviceInstance;
  }

  public <T extends EncryptionAtRestService<? extends SupportedAlgorithmInterface>>
      byte[] generateUniverseKey(
          UUID configUUID, UUID universeUUID, EncryptionAtRestConfig config) {
    T keyService;
    KmsConfig kmsConfig;
    byte[] universeKeyRef = null;
    try {
      kmsConfig = KmsConfig.get(configUUID);
      keyService = getServiceInstance(kmsConfig.keyProvider.name());
      if (EncryptionAtRestUtil.getNumKeyRotations(universeUUID, configUUID) == 0) {
        LOG.info(String.format("Creating universe key for universe %s", universeUUID.toString()));
        universeKeyRef = keyService.createKey(universeUUID, configUUID, config);
      } else {
        LOG.info(String.format("Rotating universe key for universe %s", universeUUID.toString()));
        universeKeyRef = keyService.rotateKey(universeUUID, configUUID, config);
      }
      if (universeKeyRef != null && universeKeyRef.length > 0) {
        EncryptionAtRestUtil.addKeyRef(universeUUID, configUUID, universeKeyRef);
      }
    } catch (Exception e) {
      String errMsg =
          String.format(
              "Error attempting to generate universe key for universe %s", universeUUID.toString());
      LOG.error(errMsg, e);
    }
    return universeKeyRef;
  }

  public byte[] getUniverseKey(UUID universeUUID, UUID configUUID, byte[] keyRef) {
    return getUniverseKey(universeUUID, configUUID, keyRef, null);
  }

  public <T extends EncryptionAtRestService<? extends SupportedAlgorithmInterface>>
      byte[] getUniverseKey(
          UUID universeUUID, UUID configUUID, byte[] keyRef, EncryptionAtRestConfig config) {
    byte[] keyVal = null;
    T keyService;
    try {
      keyService = getServiceInstance(KmsConfig.get(configUUID).keyProvider.name());
      keyVal =
          config == null
              ? keyService.retrieveKey(universeUUID, configUUID, keyRef)
              : keyService.retrieveKey(universeUUID, configUUID, keyRef, config);
    } catch (Exception e) {
      String errMsg =
          String.format(
              "Error attempting to retrieve the current universe key for universe %s "
                  + "with config %s",
              universeUUID.toString(), configUUID.toString());
      LOG.error(errMsg, e);
    }
    return keyVal;
  }

  public void cleanupEncryptionAtRest(UUID customerUUID, UUID universeUUID) {
    // this calls for all configs for provider X universe, regardless of config actually used
    // behavior is handled in cleanup call
    KmsConfig.listKMSConfigs(customerUUID)
        .forEach(
            config ->
                getServiceInstance(config.keyProvider.name())
                    .cleanup(universeUUID, config.configUUID));
  }

  // Build up list of objects to backup
  public List<ObjectNode> getUniverseKeyRefsForBackup(UUID universeUUID) {
    return EncryptionAtRestUtil.getAllUniverseKeys(universeUUID)
        .stream()
        .map(
            history -> {
              BackupEntry entry = null;
              try {
                KmsConfig config = KmsConfig.get(history.configUuid);
                entry = getServiceInstance(config.keyProvider.name()).getBackupEntry(history);
              } catch (Exception e) {
                String errMsg =
                    String.format(
                        "Error backing up universe key %s for universe %s",
                        history.uuid.keyRef, universeUUID.toString());
                LOG.error(errMsg, e);
              }
              return entry;
            })
        .filter(Objects::nonNull)
        .map(BackupEntry::toJson)
        .collect(Collectors.toList());
  }

  // Backup universe key metadata to file
  public void backupUniverseKeyHistory(UUID universeUUID, String storageLocation) throws Exception {
    ObjectNode backup = Json.newObject();
    ArrayNode universeKeys = backup.putArray("universe_keys");
    List<ObjectNode> universeKeyRefs = getUniverseKeyRefsForBackup(universeUUID);
    if (universeKeyRefs.size() > 0) {
      universeKeyRefs.forEach(universeKeys::add);
      ObjectMapper mapper = new ObjectMapper();
      String backupContent = mapper.writeValueAsString(backup);
      File backupKeysFile = EncryptionAtRestUtil.getUniverseBackupKeysFile(storageLocation);
      File backupKeysDir = backupKeysFile.getParentFile();

      // Skip writing key content to file since it already exists and has been written to.
      if (backupKeysFile.exists() && backupKeysFile.isFile()) {
        return;
      }

      if ((!backupKeysDir.isDirectory() && !backupKeysDir.mkdirs())
          || !backupKeysFile.createNewFile()) {
        throw new RuntimeException("Error creating backup encryption key file!");
      }

      FileUtils.writeStringToFile(backupKeysFile, backupContent);
    }
  }

  /**
   * Function to get universe keys history as ObjectNode, for use in YB-Controller extended args.
   *
   * @param universeUUID
   * @return ObjectNode consisting of universe key history.
   * @throws Exception
   */
  public ObjectNode backupUniverseKeyHistory(UUID universeUUID) throws Exception {
    ObjectNode backup = Json.newObject();
    ArrayNode universeKeys = backup.putArray("universe_keys");
    List<ObjectNode> universeKeyRefs = getUniverseKeyRefsForBackup(universeUUID);
    if (universeKeyRefs.size() > 0) {
      universeKeyRefs.forEach(universeKeys::add);
      return backup;
    }
    return null;
  }

  // Restore universe keys from metadata file
  public RestoreKeyResult restoreUniverseKeyHistory(
      String storageLocation, Consumer<JsonNode> restorer) {
    RestoreKeyResult result = RestoreKeyResult.RESTORE_FAILED;
    try {
      File backupKeysFile = EncryptionAtRestUtil.getUniverseBackupKeysFile(storageLocation);
      File backupKeysDir = backupKeysFile.getParentFile();

      // Nothing to do if no key metadata file exists
      if (!backupKeysDir.isDirectory() || !backupKeysDir.exists() || !backupKeysFile.exists()) {
        result = RestoreKeyResult.RESTORE_SKIPPED;
        return result;
      }

      byte[] backupContents = Files.readAllBytes(backupKeysFile.toPath());

      // Nothing to do if the key metadata file is empty
      if (backupContents.length == 0) {
        result = RestoreKeyResult.RESTORE_SKIPPED;
        return result;
      }

      ObjectMapper mapper = new ObjectMapper();
      JsonNode backup = mapper.readTree(backupContents);
      JsonNode universeKeys = backup.get("universe_keys");
      if (universeKeys != null && universeKeys.isArray()) {

        // Cleanup encrypted key metadata file since it is no longer needed
        backupKeysFile.delete();
        result = restoreUniverseKeyHistory(universeKeys, restorer);
      }
    } catch (Exception e) {
      LOG.error("Error occurred restoring universe key history", e);
    }

    return result;
  }

  public RestoreKeyResult restoreUniverseKeyHistory(
      JsonNode universeKeys, Consumer<JsonNode> restorer) {
    RestoreKeyResult result = RestoreKeyResult.RESTORE_FAILED;
    try {
      if (universeKeys != null && universeKeys.isArray()) {
        universeKeys.forEach(restorer);

        LOG.info("Restore universe keys succeeded!");
        result = RestoreKeyResult.RESTORE_SUCCEEDED;
      }
    } catch (Exception e) {
      LOG.error("Error occurred restoring universe key history", e);
    }
    return result;
  }
}

/*
 * Copyright 2024 YugaByte, Inc. and Contributors
 *
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

package com.yugabyte.yw.commissioner.tasks;

import static com.yugabyte.yw.common.Util.NULL_UUID;
import static play.mvc.Http.Status.INTERNAL_SERVER_ERROR;

import api.v2.models.YbaComponent;
import com.google.inject.Inject;
import com.yugabyte.yw.commissioner.AbstractTaskBase;
import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.Commissioner;
import com.yugabyte.yw.common.CloudUtil;
import com.yugabyte.yw.common.CloudUtilFactory;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.common.ShellResponse;
import com.yugabyte.yw.common.ha.PlatformReplicationHelper;
import com.yugabyte.yw.common.ha.PlatformReplicationManager;
import com.yugabyte.yw.forms.AbstractTaskParams;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.CustomerTask;
import com.yugabyte.yw.models.Schedule;
import com.yugabyte.yw.models.ScheduleTask;
import com.yugabyte.yw.models.configs.CustomerConfig;
import com.yugabyte.yw.models.helpers.TaskType;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import play.libs.Json;

@Slf4j
public class CreateYbaBackup extends AbstractTaskBase {

  private final CloudUtilFactory cloudUtilFactory;
  private final PlatformReplicationHelper replicationHelper;
  private final PlatformReplicationManager replicationManager;

  @Inject
  protected CreateYbaBackup(
      BaseTaskDependencies baseTaskDependencies,
      PlatformReplicationHelper replicationHelper,
      PlatformReplicationManager replicationManager,
      CloudUtilFactory cloudUtilFactory) {
    super(baseTaskDependencies);
    this.replicationHelper = replicationHelper;
    this.replicationManager = replicationManager;
    this.cloudUtilFactory = cloudUtilFactory;
  }

  public static class Params extends AbstractTaskParams {
    public UUID storageConfigUUID;
    public String dirName;
    public List<YbaComponent> components;
  }

  @Override
  protected Params taskParams() {
    return (Params) taskParams;
  }

  public void runScheduledBackup(
      Schedule schedule, Commissioner commissioner, boolean alreadyRunning) {
    log.info("Execution of scheduled YBA backup");
    if (alreadyRunning) {
      log.info("Continuous backup already running, skipping.");
      return;
    }
    UUID customerUUID = schedule.getCustomerUUID();
    Customer customer = Customer.get(customerUUID);
    CreateYbaBackup.Params taskParams =
        Json.fromJson(schedule.getTaskParams(), CreateYbaBackup.Params.class);

    if (schedule.isBacklogStatus()) {
      schedule.updateBacklogStatus(false);
    }

    UUID taskUUID = commissioner.submit(TaskType.CreateYbaBackup, taskParams);
    ScheduleTask.create(taskUUID, schedule.getScheduleUUID());
    CustomerTask.create(
        customer,
        NULL_UUID,
        taskUUID,
        CustomerTask.TargetType.Yba,
        CustomerTask.TaskType.CreateYbaBackup,
        // TODO: Actually get platform IP
        "platform_ip");
    log.info("Submitted continuous yba backup creation with task uuid = {}.", taskUUID);
  }

  @Override
  public void run() {
    log.info("Execution of CreateYbaBackup");
    CreateYbaBackup.Params taskParams = taskParams();
    if (taskParams.storageConfigUUID == null) {
      log.info("No storage config UUID set, skipping creation of YBA backup.");
      return;
    }
    log.debug("Creating platform backup...");
    ShellResponse response =
        replicationHelper.runCommand(replicationManager.new CreatePlatformBackupParams());

    if (response.code != 0) {
      log.error("Backup failed: " + response.message);
      throw new PlatformServiceException(
          INTERNAL_SERVER_ERROR, "Backup failed: " + response.message);
    }
    Optional<File> backupOpt = replicationHelper.getMostRecentBackup();
    if (!backupOpt.isPresent()) {
      throw new PlatformServiceException(INTERNAL_SERVER_ERROR, "could not find backup file");
    }
    File backup = backupOpt.get();
    CustomerConfig customerConfig = CustomerConfig.get(taskParams.storageConfigUUID);
    if (customerConfig == null) {
      throw new PlatformServiceException(
          INTERNAL_SERVER_ERROR,
          "Could not find customer config with provided storage config UUID during create.");
    }
    CloudUtil cloudUtil = cloudUtilFactory.getCloudUtil(customerConfig.getName());
    if (!cloudUtil.uploadYbaBackup(customerConfig.getDataObject(), backup, taskParams.dirName)) {
      throw new PlatformServiceException(
          INTERNAL_SERVER_ERROR, "Could not upload YBA backup to cloud storage.");
    }

    if (!cloudUtil.cleanupUploadedBackups(customerConfig.getDataObject(), taskParams.dirName)) {
      log.warn(
          "Error cleaning up uploaded backups to cloud storage, please delete manually to avoid"
              + " incurring unexpected costs.");
    }

    // Cleanup backups
    replicationHelper.cleanupCreatedBackups();
    log.info(backup.getAbsolutePath());
    return;
  }
}

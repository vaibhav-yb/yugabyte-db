// Copyright (c) YugaByte, Inc.

import static com.yugabyte.yw.models.MetricConfig.METRICS_CONFIG_PATH;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import com.yugabyte.yw.cloud.aws.AWSInitializer;
import com.yugabyte.yw.commissioner.BackupGarbageCollector;
import com.yugabyte.yw.commissioner.CallHome;
import com.yugabyte.yw.commissioner.HealthChecker;
import com.yugabyte.yw.commissioner.NodeAgentPoller;
import com.yugabyte.yw.commissioner.PerfAdvisorScheduler;
import com.yugabyte.yw.commissioner.PitrConfigPoller;
import com.yugabyte.yw.commissioner.PerfAdvisorGarbageCollector;
import com.yugabyte.yw.commissioner.RefreshKmsService;
import com.yugabyte.yw.commissioner.SetUniverseKey;
import com.yugabyte.yw.commissioner.SupportBundleCleanup;
import com.yugabyte.yw.commissioner.TaskGarbageCollector;
import com.yugabyte.yw.commissioner.YbcUpgrade;
import com.yugabyte.yw.common.ConfigHelper;
import com.yugabyte.yw.common.CustomerTaskManager;
import com.yugabyte.yw.common.ExtraMigrationManager;
import com.yugabyte.yw.common.ReleaseManager;
import com.yugabyte.yw.common.ShellLogsManager;
import com.yugabyte.yw.common.SnapshotCleanup;
import com.yugabyte.yw.common.YamlWrapper;
import com.yugabyte.yw.common.alerts.AlertConfigurationService;
import com.yugabyte.yw.common.alerts.AlertConfigurationWriter;
import com.yugabyte.yw.common.alerts.AlertDestinationService;
import com.yugabyte.yw.common.alerts.AlertsGarbageCollector;
import com.yugabyte.yw.common.alerts.QueryAlerts;
import com.yugabyte.yw.common.certmgmt.CertificateHelper;
import com.yugabyte.yw.common.ha.PlatformReplicationManager;
import com.yugabyte.yw.common.metrics.PlatformMetricsProcessor;
import com.yugabyte.yw.common.metrics.SwamperTargetsFileUpdater;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.ExtraMigration;
import com.yugabyte.yw.models.InstanceType;
import com.yugabyte.yw.models.MetricConfig;
import com.yugabyte.yw.models.Provider;
import com.yugabyte.yw.queries.QueryHelper;
import com.yugabyte.yw.scheduler.Scheduler;
import io.ebean.Ebean;
import io.prometheus.client.hotspot.DefaultExports;
import java.util.List;
import java.util.Map;
import play.Application;
import play.Configuration;
import play.Environment;
import play.Logger;

/** We will use this singleton to do actions specific to the app environment, like db seed etc. */
@Singleton
public class AppInit {

  private static final long MAX_APP_INITIALIZATION_TIME = 30;

  @Inject
  public AppInit(
      Environment environment,
      Application application,
      ConfigHelper configHelper,
      ReleaseManager releaseManager,
      AWSInitializer awsInitializer,
      CustomerTaskManager taskManager,
      YamlWrapper yaml,
      ExtraMigrationManager extraMigrationManager,
      PitrConfigPoller pitrConfigPoller,
      TaskGarbageCollector taskGC,
      SetUniverseKey setUniverseKey,
      RefreshKmsService refreshKmsService,
      BackupGarbageCollector backupGC,
      PerfAdvisorScheduler perfAdvisorScheduler,
      PlatformReplicationManager replicationManager,
      AlertsGarbageCollector alertsGC,
      QueryAlerts queryAlerts,
      AlertConfigurationWriter alertConfigurationWriter,
      SwamperTargetsFileUpdater swamperTargetsFileUpdater,
      AlertConfigurationService alertConfigurationService,
      AlertDestinationService alertDestinationService,
      QueryHelper queryHelper,
      PlatformMetricsProcessor platformMetricsProcessor,
      Scheduler scheduler,
      CallHome callHome,
      HealthChecker healthChecker,
      ShellLogsManager shellLogsManager,
      Config config,
      SupportBundleCleanup supportBundleCleanup,
      NodeAgentPoller nodeAgentPoller,
      YbcUpgrade ybcUpgrade,
      PerfAdvisorGarbageCollector perfRecGC,
      SnapshotCleanup snapshotCleanup,
      @Named("AppStartupTimeMs") Long startupTime)
      throws ReflectiveOperationException {
    Logger.info("Yugaware Application has started");

    Configuration appConfig = application.configuration();
    String mode = appConfig.getString("yb.mode", "PLATFORM");

    if (!environment.isTest()) {
      // Check if we have provider data, if not, we need to seed the database
      if (Customer.find.query().where().findCount() == 0
          && appConfig.getBoolean("yb.seedData", false)) {
        Logger.debug("Seed the Yugaware DB");

        List<?> all =
            yaml.load(environment.resourceAsStream("db_seed.yml"), application.classloader());
        Ebean.saveAll(all);
        Customer customer = Customer.getAll().get(0);
        alertDestinationService.createDefaultDestination(customer.uuid);
        alertConfigurationService.createDefaultConfigs(customer);
      }

      boolean ywFileDataSynced =
          Boolean.parseBoolean(
              configHelper
                  .getConfig(ConfigHelper.ConfigType.FileDataSync)
                  .getOrDefault("synced", "false")
                  .toString());
      String storagePath = appConfig.getString("yb.storage.path");
      configHelper.syncFileData(storagePath, ywFileDataSynced);

      if (mode.equals("PLATFORM")) {
        String devopsHome = appConfig.getString("yb.devops.home");
        if (devopsHome == null || devopsHome.length() == 0) {
          throw new RuntimeException("yb.devops.home is not set in application.conf");
        }

        if (storagePath == null || storagePath.length() == 0) {
          throw new RuntimeException(("yb.storage.path is not set in application.conf"));
        }
      }

      // temporarily revert due to PLAT-2434
      // LogUtil.updateApplicationLoggingFromConfig(sConfigFactory, config);
      // LogUtil.updateAuditLoggingFromConfig(sConfigFactory, config);

      // Initialize AWS if any of its instance types have an empty volumeDetailsList
      List<Provider> providerList = Provider.find.query().where().findList();
      for (Provider provider : providerList) {
        if (provider.code.equals("aws")) {
          for (InstanceType instanceType :
              InstanceType.findByProvider(provider, application.config(), configHelper)) {
            if (instanceType.instanceTypeDetails != null
                && (instanceType.instanceTypeDetails.volumeDetailsList == null)) {
              awsInitializer.initialize(provider.customerUUID, provider.uuid);
              break;
            }
          }
          break;
        }
      }

      // Load metrics configurations.
      Map<String, Object> configs =
          yaml.load(environment.resourceAsStream(METRICS_CONFIG_PATH), application.classloader());
      MetricConfig.loadConfig(configs);

      // Enter all the configuration data. This is the first thing that should be
      // done as the other init steps may depend on this data.
      configHelper.loadConfigsToDB(application);
      configHelper.loadSoftwareVersiontoDB(application);

      // Run and delete any extra migrations.
      for (ExtraMigration m : ExtraMigration.getAll()) {
        m.run(extraMigrationManager);
      }

      // Import new local releases into release metadata
      releaseManager.importLocalReleases();
      releaseManager.updateCurrentReleases();
      // Background thread to query for latest ARM release version.
      Thread armReleaseThread =
          new Thread(
              () -> {
                try {
                  Logger.info("Attempting to query latest ARM release link.");
                  releaseManager.findLatestArmRelease(configHelper.getCurrentVersion(application));
                  Logger.info("Imported ARM release download link.");
                } catch (Exception e) {
                  Logger.warn("Error importing ARM release download link", e);
                }
              });
      armReleaseThread.start();

      // initialize prometheus exports
      DefaultExports.initialize();

      // Handle incomplete tasks
      taskManager.handleAllPendingTasks();

      // Schedule garbage collection of old completed tasks in database.
      taskGC.start();
      alertsGC.start();
      perfRecGC.start();

      setUniverseKey.start();
      // Refreshes all the KMS providers. Useful for renewing tokens, ttls, etc.
      refreshKmsService.start();

      // Schedule garbage collection of backups
      backupGC.start();

      // Cleanup orphan snapshots
      snapshotCleanup.start();

      perfAdvisorScheduler.start();

      // Cleanup old support bundles
      supportBundleCleanup.start();

      platformMetricsProcessor.start();
      alertConfigurationWriter.start();
      swamperTargetsFileUpdater.start();

      replicationManager.init();

      scheduler.init();
      callHome.start();
      queryAlerts.start();
      healthChecker.initialize();
      shellLogsManager.startLogsGC();
      nodeAgentPoller.init();
      pitrConfigPoller.start();

      ybcUpgrade.start();

      // Add checksums for all certificates that don't have a checksum.
      CertificateHelper.createChecksums();

      long elapsed = (System.currentTimeMillis() - startupTime) / 1000;
      String elapsedStr = String.valueOf(elapsed);
      if (elapsed > MAX_APP_INITIALIZATION_TIME) {
        Logger.warn("Completed initialization in " + elapsedStr + " seconds.");
      } else {
        Logger.info("Completed initialization in " + elapsedStr + " seconds.");
      }

      Logger.info("AppInit completed");
    }
  }
}

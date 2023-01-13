package com.yugabyte.yw.common.supportbundle;

import com.typesafe.config.Config;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yugabyte.yw.common.SupportBundleUtil;
import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.NodeDetails;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

@Slf4j
@Singleton
public class ApplicationLogsComponent implements SupportBundleComponent {

  protected final Config config;
  private final SupportBundleUtil supportBundleUtil;

  @Inject
  public ApplicationLogsComponent(
      BaseTaskDependencies baseTaskDependencies, SupportBundleUtil supportBundleUtil) {
    this.config = baseTaskDependencies.getConfig();
    this.supportBundleUtil = supportBundleUtil;
  }

  @Override
  public void downloadComponent(
      Customer customer, Universe universe, Path bundlePath, NodeDetails node) throws IOException {
    String appHomeDir =
        config.hasPath("application.home") ? config.getString("application.home") : ".";
    String logDir =
        config.hasPath("log.override.path")
            ? config.getString("log.override.path")
            : String.format("%s/logs", appHomeDir);
    String destDir = bundlePath.toString() + "/" + "application_logs";
    Path destPath = Paths.get(destDir);
    Files.createDirectories(destPath);
    File source = new File(logDir);
    File dest = new File(destDir);
    FileUtils.copyDirectory(source, dest);
    log.debug("Downloaded application logs to {}", destDir);
  }

  @Override
  public void downloadComponentBetweenDates(
      Customer customer,
      Universe universe,
      Path bundlePath,
      Date startDate,
      Date endDate,
      NodeDetails node)
      throws IOException, ParseException {

    // Get application configured locations
    String appHomeDir =
        config.hasPath("application.home") ? config.getString("application.home") : ".";
    log.info("[ApplicationLogsComponent] appHomeDir = '{}'", appHomeDir);
    String logDir =
        config.hasPath("log.override.path")
            ? config.getString("log.override.path")
            : String.format("%s/logs", appHomeDir);
    Path logPath = Paths.get(logDir);
    String logDirAbsolute = logPath.toAbsolutePath().toString();
    log.info("[ApplicationLogsComponent] logDir = '{}'", logDir);
    log.info("[ApplicationLogsComponent] logDirAbsolute = '{}'", logDirAbsolute);

    // Create "application_logs" folder inside the support bundle folder
    String destDir = bundlePath.toString() + "/" + "application_logs";
    Files.createDirectories(Paths.get(destDir));
    File source = new File(logDirAbsolute);
    File dest = new File(destDir);

    // Get all the log file names present in source directory
    List<String> logFiles = new ArrayList<String>();
    File[] sourceFiles = source.listFiles();
    if (sourceFiles == null) {
      log.info("[ApplicationLogsComponent] sourceFiles = null");
    } else {
      log.info(
          "[ApplicationLogsComponent] sourceFiles.length = '{}'",
          String.valueOf(sourceFiles.length));
    }
    for (File sourceFile : sourceFiles) {
      if (sourceFile.isFile()) {
        logFiles.add(sourceFile.getName());
      }
    }

    // All the logs file names that we want to keep after filtering within start and end date
    List<String> filteredLogFiles = new ArrayList<String>();

    // "application.log" is the latest log file that is being updated at the moment
    Date dateToday = supportBundleUtil.getTodaysDate();
    if (logFiles.contains("application.log")
        && supportBundleUtil.checkDateBetweenDates(dateToday, startDate, endDate)) {
      filteredLogFiles.add("application.log");
    }

    // Filter the log files by a preliminary check of the name format
    String applicationLogsRegexPattern =
        config.getString("yb.support_bundle.application_logs_regex_pattern");
    logFiles =
        supportBundleUtil
            .filterList(
                logFiles.stream().map(Paths::get).collect(Collectors.toList()),
                Arrays.asList(applicationLogsRegexPattern))
            .stream()
            .map(Path::toString)
            .collect(Collectors.toList());

    // Filters the log files whether it is between startDate and endDate
    for (String logFile : logFiles) {
      String applicationLogsSdfPattern =
          config.getString("yb.support_bundle.application_logs_sdf_pattern");
      Date fileDate = new SimpleDateFormat(applicationLogsSdfPattern).parse(logFile);
      if (supportBundleUtil.checkDateBetweenDates(fileDate, startDate, endDate)) {
        filteredLogFiles.add(logFile);
      }
    }

    // Copy individual files from source directory to the support bundle folder
    for (String filteredLogFile : filteredLogFiles) {
      Path sourceFilePath = Paths.get(source.toString(), filteredLogFile);
      Path destFilePath = Paths.get(dest.toString(), filteredLogFile);
      Files.copy(sourceFilePath, destFilePath, StandardCopyOption.REPLACE_EXISTING);
    }

    log.debug("Downloaded application logs to {}, between {} and {}", destDir, startDate, endDate);
  };
}

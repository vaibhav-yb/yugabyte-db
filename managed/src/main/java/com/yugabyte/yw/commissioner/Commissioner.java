// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.commissioner;

import static play.mvc.Http.Status.BAD_REQUEST;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yugabyte.yw.commissioner.TaskExecutor.RunnableTask;
import com.yugabyte.yw.commissioner.TaskExecutor.TaskExecutionListener;
import com.yugabyte.yw.common.BackupUtil;
import com.yugabyte.yw.common.PlatformExecutorFactory;
import com.yugabyte.yw.common.PlatformScheduler;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.common.ProviderEditRestrictionManager;
import com.yugabyte.yw.common.config.RuntimeConfigFactory;
import com.yugabyte.yw.forms.ITaskParams;
import com.yugabyte.yw.models.Backup;
import com.yugabyte.yw.models.Backup.BackupState;
import com.yugabyte.yw.models.CustomerTask;
import com.yugabyte.yw.models.TaskInfo;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.TaskType;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import play.inject.ApplicationLifecycle;
import play.libs.Json;

@Singleton
public class Commissioner {

  public static final String SUBTASK_ABORT_POSITION_PROPERTY = "subtask-abort-position";
  public static final String SUBTASK_PAUSE_POSITION_PROPERTY = "subtask-pause-position";

  public static final Logger LOG = LoggerFactory.getLogger(Commissioner.class);

  private final ExecutorService executor;

  private final TaskExecutor taskExecutor;

  // A map of all task UUID's to the task runnable objects for all the user tasks that are currently
  // active. Recently completed tasks are also in this list, their completion percentage should be
  // persisted before removing the task from this map.
  private final Map<UUID, RunnableTask> runningTasks = new ConcurrentHashMap<>();

  // A map of task UUIDs to latches for currently paused tasks.
  private final Map<UUID, CountDownLatch> pauseLatches = new ConcurrentHashMap<>();

  private final ProviderEditRestrictionManager providerEditRestrictionManager;

  @Inject
  public Commissioner(
      ProgressMonitor progressMonitor,
      ApplicationLifecycle lifecycle,
      PlatformExecutorFactory platformExecutorFactory,
      TaskExecutor taskExecutor,
      ProviderEditRestrictionManager providerEditRestrictionManager) {
    ThreadFactory namedThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("TaskPool-%d").build();
    this.taskExecutor = taskExecutor;
    this.providerEditRestrictionManager = providerEditRestrictionManager;
    executor = platformExecutorFactory.createExecutor("commissioner", namedThreadFactory);
    LOG.info("Started Commissioner TaskPool.");
    progressMonitor.start(runningTasks);
    LOG.info("Started TaskProgressMonitor thread.");
  }

  /**
   * Returns true if the task identified by the task type is abortable.
   *
   * @param taskType the task type.
   * @return true if abortable.
   */
  public boolean isTaskAbortable(TaskType taskType) {
    return TaskExecutor.isTaskAbortable(taskType.getTaskClass());
  }

  /**
   * Returns true if the task identified by the task type is retryable.
   *
   * @param taskType the task type.
   * @return true if retryable.
   */
  public boolean isTaskRetryable(TaskType taskType) {
    return TaskExecutor.isTaskRetryable(taskType.getTaskClass());
  }

  /**
   * Creates a new task runnable to run the required task, and submits it to the TaskExecutor.
   *
   * @param taskType the task type.
   * @param taskParams the task parameters.
   */
  public UUID submit(TaskType taskType, ITaskParams taskParams) {
    RunnableTask taskRunnable = null;
    try {
      // Create the task runnable object based on the various parameters passed in.
      taskRunnable = taskExecutor.createRunnableTask(taskType, taskParams);
      // Add the consumer to handle before task if available.
      taskRunnable.setTaskExecutionListener(getTaskExecutionListener());
      onTaskCreated(taskRunnable, taskParams);
      UUID taskUUID = taskExecutor.submit(taskRunnable, executor);
      // Add this task to our queue.
      runningTasks.put(taskUUID, taskRunnable);
      return taskRunnable.getTaskUUID();
    } catch (Throwable t) {
      if (taskRunnable != null) {
        // Destroy the task initialization in case of failure.
        taskRunnable.task.terminate();
        TaskInfo taskInfo = taskRunnable.taskInfo;
        if (taskInfo.getTaskState() != TaskInfo.State.Failure) {
          taskInfo.setTaskState(TaskInfo.State.Failure);
          taskInfo.save();
        }
      }
      String msg = "Error processing " + taskType + " task for " + taskParams.toString();
      LOG.error(msg, t);
      if (t instanceof PlatformServiceException) {
        throw t;
      }
      throw new RuntimeException(msg, t);
    }
  }

  private void onTaskCreated(RunnableTask taskRunnable, ITaskParams taskParams) {
    providerEditRestrictionManager.onTaskCreated(
        taskRunnable.getTaskUUID(), taskRunnable.task, taskParams);
  }

  /**
   * Triggers task abort asynchronously. It can take some time for the task to abort. Caller can
   * check the task status for the final state.
   *
   * @param taskUUID the UUID of the task to be aborted.
   * @return true if the task is found running and abort is triggered successfully, else false.
   */
  public boolean abortTask(UUID taskUUID) {
    TaskInfo taskInfo = TaskInfo.getOrBadRequest(taskUUID);
    if (!isTaskAbortable(taskInfo.getTaskType())) {
      throw new PlatformServiceException(
          BAD_REQUEST, String.format("Invalid task type: Task %s cannot be aborted", taskUUID));
    }

    if (taskInfo.getTaskState() != TaskInfo.State.Running) {
      LOG.warn("Task {} is not running", taskUUID);
      return false;
    }
    CountDownLatch latch = pauseLatches.get(taskUUID);
    if (latch != null) {
      // Resume if it is already paused to abort faster.
      latch.countDown();
    }
    Optional<TaskInfo> optional = taskExecutor.abort(taskUUID);
    boolean success = optional.isPresent();
    if (success && BackupUtil.BACKUP_TASK_TYPES.contains(taskInfo.getTaskType())) {
      Backup.fetchAllBackupsByTaskUUID(taskUUID)
          .forEach((backup) -> backup.transitionState(BackupState.Stopping));
    }
    return success;
  }

  /**
   * Resumes a paused task. This is useful for fault injection to pause a task at a predefined
   * position (e.g 0) and get the list of subtasks to set the abort position during resume.
   *
   * @param taskUUID the UUID of the task to be resumed.
   * @return true if the task is found to be paused else false.
   */
  public boolean resumeTask(UUID taskUUID) {
    TaskInfo.getOrBadRequest(taskUUID);
    CountDownLatch latch = pauseLatches.get(taskUUID);
    if (latch == null) {
      return false;
    }
    RunnableTask runnableTask = runningTasks.get(taskUUID);
    if (runnableTask != null) {
      runnableTask.setTaskExecutionListener(getTaskExecutionListener());
    }
    latch.countDown();
    return true;
  }

  public ObjectNode getStatusOrBadRequest(UUID taskUUID) {
    return mayGetStatus(taskUUID)
        .orElseThrow(
            () -> new PlatformServiceException(BAD_REQUEST, "Not able to find task " + taskUUID));
  }

  public Optional<ObjectNode> buildTaskStatus(
      CustomerTask task, TaskInfo taskInfo, Map<UUID, CustomerTask> lastTaskByTarget) {
    if (task == null || taskInfo == null) {
      return Optional.empty();
    }
    ObjectNode responseJson = Json.newObject();
    // Add some generic information about the task
    responseJson.put("title", task.getFriendlyDescription());
    responseJson.put("createTime", task.getCreateTime().toString());
    responseJson.put("target", task.getTargetName());
    responseJson.put("targetUUID", task.getTargetUUID().toString());
    responseJson.put("type", task.getType().name());
    // Find out the state of the task.
    responseJson.put("status", taskInfo.getTaskState().toString());
    // Get the percentage of subtasks that ran and completed
    responseJson.put("percent", taskInfo.getPercentCompleted());
    String correlationId = task.getCorrelationId();
    if (!Strings.isNullOrEmpty(correlationId)) {
      responseJson.put("correlationId", correlationId);
    }
    responseJson.put("userEmail", task.getUserEmail());

    // Get subtask groups and add other details to it if applicable.
    UserTaskDetails userTaskDetails;
    RunnableTask runnable = runningTasks.get(taskInfo.getTaskUUID());
    if (runnable != null) {
      userTaskDetails = taskInfo.getUserTaskDetails(runnable.getTaskCache());
    } else {
      userTaskDetails = taskInfo.getUserTaskDetails();
    }
    responseJson.set("details", Json.toJson(userTaskDetails));

    // Set abortable if eligible.
    responseJson.put("abortable", false);
    if (taskExecutor.isTaskRunning(task.getTaskUUID())) {
      // Task is abortable only when it is running.
      responseJson.put("abortable", isTaskAbortable(taskInfo.getTaskType()));
    }

    // Set retryable if eligible.
    responseJson.put("retryable", false);
    if (isTaskRetryable(taskInfo.getTaskType())
        && TaskInfo.ERROR_STATES.contains(taskInfo.getTaskState())) {
      if (task.getTarget() == CustomerTask.TargetType.Provider) {
        CustomerTask lastTask =
            lastTaskByTarget.computeIfAbsent(
                task.getTargetUUID(), tId -> CustomerTask.getLastTaskByTargetUuid(tId));
        responseJson.put(
            "retryable", lastTask != null && lastTask.getTaskUUID().equals(task.getTaskUUID()));
      } else {
        // Retryable depends on the updating Task UUID in the Universe.
        Universe.getUniverseDetailsField(String.class, task.getTargetUUID(), "updatingTaskUUID")
            .ifPresent(
                updatingTask -> {
                  responseJson.put(
                      "retryable", taskInfo.getTaskUUID().equals(UUID.fromString(updatingTask)));
                });
      }
    }
    if (pauseLatches.containsKey(taskInfo.getTaskUUID())) {
      // Set this only if it is true. The thread is just parking. From the task state
      // perspective, it is still running.
      responseJson.put("paused", true);
    }
    return Optional.of(responseJson);
  }

  public Optional<ObjectNode> mayGetStatus(UUID taskUUID) {
    CustomerTask task = CustomerTask.find.query().where().eq("task_uuid", taskUUID).findOne();
    // Check if the task is in the DB.
    TaskInfo taskInfo = TaskInfo.get(taskUUID);
    if (task == null || taskInfo == null) {
      // We are not able to find the task. Report an error.
      LOG.error("Error fetching task progress for {}. TaskInfo is not found", taskUUID);
      return Optional.empty();
    }
    return buildTaskStatus(task, taskInfo, new HashMap<>());
  }

  public JsonNode getTaskDetails(UUID taskUUID) {
    TaskInfo taskInfo = TaskInfo.get(taskUUID);
    if (taskInfo != null) {
      return taskInfo.getTaskDetails();
    } else {
      // TODO: push this down to TaskInfo
      throw new PlatformServiceException(
          BAD_REQUEST, "Failed to retrieve task params for Task UUID: " + taskUUID);
    }
  }

  private int getSubTaskPositionFromContext(String property) {
    int position = -1;
    String value = MDC.get(property);
    if (!Strings.isNullOrEmpty(value)) {
      try {
        position = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        LOG.warn("Error in parsing subtask position for {}, ignoring it.", property, e);
        position = -1;
      }
    }
    return position;
  }

  // Returns the TaskExecutionListener instance.
  private TaskExecutionListener getTaskExecutionListener() {
    final Consumer<TaskInfo> beforeTaskConsumer = getBeforeTaskConsumer();
    TaskExecutionListener listener =
        new TaskExecutionListener() {
          @Override
          public void beforeTask(TaskInfo taskInfo) {
            LOG.info("About to execute task {}", taskInfo);
            if (beforeTaskConsumer != null) {
              beforeTaskConsumer.accept(taskInfo);
            }
          }

          @Override
          public void afterTask(TaskInfo taskInfo, Throwable t) {
            LOG.info("Task {} is completed", taskInfo);
            providerEditRestrictionManager.onTaskFinished(taskInfo.getTaskUUID());
          }
        };
    return listener;
  }

  // Returns the composed for before task callback of TaskExecutionListener.
  private Consumer<TaskInfo> getBeforeTaskConsumer() {
    Consumer<TaskInfo> consumer = null;
    final int subTaskAbortPosition = getSubTaskPositionFromContext(SUBTASK_ABORT_POSITION_PROPERTY);
    final int subTaskPausePosition = getSubTaskPositionFromContext(SUBTASK_PAUSE_POSITION_PROPERTY);
    if (subTaskAbortPosition >= 0) {
      // Handle abort of subtask.
      consumer =
          taskInfo -> {
            if (taskInfo.getPosition() >= subTaskAbortPosition) {
              LOG.debug("Aborting task {} at position {}", taskInfo, taskInfo.getPosition());
              throw new CancellationException("Subtask cancelled");
            }
          };
    }
    if (subTaskPausePosition >= 0) {
      // Handle pause of subtask.
      Consumer<TaskInfo> pauseConsumer =
          taskInfo -> {
            if (taskInfo.getPosition() >= subTaskPausePosition) {
              LOG.debug("Pausing task {} at position {}", taskInfo, taskInfo.getPosition());
              final UUID subTaskUUID = taskInfo.getParentUUID();
              try {
                // Insert if absent and get the latch.
                pauseLatches.computeIfAbsent(subTaskUUID, k -> new CountDownLatch(1)).await();
              } catch (InterruptedException e) {
                throw new CancellationException("Subtask cancelled: " + e.getMessage());
              } finally {
                pauseLatches.remove(subTaskUUID);
              }
            }
          };
      consumer = consumer == null ? pauseConsumer : consumer.andThen(pauseConsumer);
    }
    return consumer;
  }

  /**
   * A progress monitor to constantly write a last updated timestamp in the DB so that this process
   * and all its subtasks are considered to be alive.
   */
  @Slf4j
  @Singleton
  private static class ProgressMonitor {

    private static final String YB_COMMISSIONER_PROGRESS_CHECK_INTERVAL =
        "yb.commissioner.progress_check_interval";
    private final PlatformScheduler platformScheduler;
    private final RuntimeConfigFactory runtimeConfigFactory;

    @Inject
    public ProgressMonitor(
        PlatformScheduler platformScheduler, RuntimeConfigFactory runtimeConfigFactory) {
      this.platformScheduler = platformScheduler;
      this.runtimeConfigFactory = runtimeConfigFactory;
    }

    public void start(Map<UUID, RunnableTask> runningTasks) {
      Duration checkInterval = this.progressCheckInterval();
      if (checkInterval.isZero()) {
        log.info(YB_COMMISSIONER_PROGRESS_CHECK_INTERVAL + " set to 0.");
        log.warn("!!! TASK GC DISABLED !!!");
      } else {
        log.info("Scheduling Progress Check every " + checkInterval);
        platformScheduler.schedule(
            getClass().getSimpleName(),
            Duration.ZERO, // InitialDelay
            checkInterval,
            () -> scheduleRunner(runningTasks));
      }
    }

    private void scheduleRunner(Map<UUID, RunnableTask> runningTasks) {
      // Loop through all the active tasks.
      try {
        Iterator<Entry<UUID, RunnableTask>> iter = runningTasks.entrySet().iterator();
        while (iter.hasNext()) {
          Entry<UUID, RunnableTask> entry = iter.next();
          RunnableTask taskRunnable = entry.getValue();
          // If the task is still running, update its latest timestamp as a part of the heartbeat.
          if (taskRunnable.isTaskRunning()) {
            taskRunnable.doHeartbeat();
          } else if (taskRunnable.hasTaskCompleted()) {
            LOG.info(
                "Task {} has completed with {} state.", taskRunnable, taskRunnable.getTaskState());
            // Remove task from the set of live tasks.
            iter.remove();
          }
        }
        // TODO: Scan the DB for tasks that have failed to make progress and claim one if possible.
      } catch (Exception e) {
        log.error("Error running commissioner progress checker", e);
      }
    }

    private Duration progressCheckInterval() {
      return runtimeConfigFactory
          .staticApplicationConf()
          .getDuration(YB_COMMISSIONER_PROGRESS_CHECK_INTERVAL);
    }
  }
}

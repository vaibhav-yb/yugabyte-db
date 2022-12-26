---
title: Schedule YSQL data backups
headerTitle: Schedule YSQL data backups
linkTitle: Schedule data backups
description: Use YugabyteDB Anywhere to create scheduled backups of universe YSQL data.
aliases:
  - /preview/manage/enterprise-edition/schedule-backups/
  - /preview/manage/enterprise-edition/schedule-data-backup/
  - /preview/yugabyte-platform/back-up-restore-universes/schedule-data-backups/
menu:
  preview_yugabyte-platform:
    identifier: schedule-data-backups-1-ysql
    parent: back-up-restore-universes
    weight: 40
type: docs
---

<ul class="nav nav-tabs-alt nav-tabs-yb">

  <li >
    <a href="{{< relref "./ysql.md" >}}" class="nav-link active">
      <i class="icon-postgres" aria-hidden="true"></i>
      YSQL
    </a>
  </li>

  <li >
    <a href="{{< relref "./ycql.md" >}}" class="nav-link">
      <i class="icon-cassandra" aria-hidden="true"></i>
      YCQL
    </a>
  </li>

</ul>

<br>You can use YugabyteDB Anywhere to perform regularly scheduled backups of YugabyteDB universe data for all YSQL tables in a namespace.

To back up your universe YSQL data immediately, see [Back up universe YSQL data](../../back-up-universe-data/ysql/).

## Create a scheduled backup policy

Before scheduling a backup of your universe YSQL data, create a policy, as follows:

1. Navigate to **Universes**.

1. Select the name of the universe for which you want to schedule backups.

1. Select the **Tables** tab and click **Actions** to verify that backups are enabled. If disabled, click **Enable Backup**.

1. Select the **Backups** tab and then select **Scheduled Backup Policies**.

1. Click **Create Scheduled Backup Policy** to open the dialog shown in the following illustration:
   
   ![Create Backup form](/images/yp/scheduled-backup-ysql-1.png)<br>
   
1. Provide the backup policy name.

1. Select the backup storage configuration. Notice that the contents of the **Select the storage config you want to use for your backup** list depends on your existing backup storage configurations. For more information, see [Configure backup storage](../../configure-backup-storage/).

1. Select the database to backup. You may also choose to back up all databases associated with your universe.

1. Specify the period of time during which the backup is to be retained. Note that there's an option to never delete the backup.

1. Specify the interval between backups or select **Use cron expression (UTC)**.

1. For a YB Controller-powered universe, you can enable **Take incremental backups within full backup intervals** to instruct the schedule policy to take full backups periodically and incremental backups between those full backups. The incremental backups intervals be longer than the full scheduled backup frequency:

   ![Incremental Backup](/images/yp/scheduled-backup-ycql-incremental.png)<br>

   If you disable the full backup, the incremental backup stops. If you enable the full backup again, the incremental backup schedule starts on new full backups.

   If you delete the main full backup schedule, the incremental backup schedule is also deleted.

   You cannot modify any incremental backup-related property in the schedule; to overwrite any incremental backup property, you have to delete the existing schedule and create a new schedule if needed.

1. Specify the number of threads that should be available for the backup process.

1. Click **Create**.

Subsequent backups are created based on the value you specified for **Set backup intervals** or **Use cron expression**.

## Disable backups

You can disable all backups, including scheduled ones, as follows:

1. Navigate to the universe's **Tables** tab.
1. Click **Actions > Disable Backup**.

## Delete a scheduled backup

You can permanently remove a scheduled backup, as follows:

1. Navigate to the universe's **Backups** tab.
2. Find the scheduled backup in the **Backups** list and click **... > Delete Backup**.

To delete a policy, select **Scheduled Backup Policies**, find the policy and click its **Actions > Delete Policy**.


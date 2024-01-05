---
title: Schedule universe YCQL data backups
headerTitle: Schedule universe YCQL data backups
linkTitle: Schedule data backups
description: Use YugabyteDB Anywhere to create scheduled backups of universe YCQL data.
menu:
  v2.16_yugabyte-platform:
    identifier: schedule-data-backups-2-ycql
    parent: back-up-restore-universes
    weight: 15
type: docs
---

<ul class="nav nav-tabs-alt nav-tabs-yb">

  <li >
    <a href="{{< relref "./ysql.md" >}}" class="nav-link">
      <i class="icon-postgres" aria-hidden="true"></i>
      YSQL
    </a>
  </li>

  <li >
    <a href="{{< relref "./ycql.md" >}}" class="nav-link active">
      <i class="icon-cassandra" aria-hidden="true"></i>
      YCQL
    </a>
  </li>

</ul>

You can use YugabyteDB Anywhere to perform regularly scheduled backups of YugabyteDB universe data for all YCQL tables in a keyspace or only the specified YCQL tables.

To back up your universe YCQL data immediately, see [Back up universe YCQL data](../../back-up-universe-data/ycql).

## Create a scheduled backup policy

Before scheduling a backup of your universe YCQL data, create a policy, as follows:

1. Navigate to **Universes**.
1. Select the name of the universe for which you want to schedule backups.
1. Select the **Tables** tab and click **Actions** to verify that backups are enabled. If disabled, click **Enable Backup**.
1. Select the **Backups** tab and then select **Scheduled Backup Policies**.
1. Click **Create Scheduled Backup Policy** to open the dialog shown in the following illustration:

    ![Create Backup form](/images/yp/scheduled-backup-ycql.png)

1. Provide the backup policy name.
1. Specify the interval between backups or select **Use cron expression (UTC)**.
1. Set the API type as YCQL.
1. Select the database to back up.
1. Specify whether you want to back up all tables in the keyspace to which the database belongs or only  certain tables. If you choose **Select a subset of tables**, a **Select Tables** dialog opens allowing you to select one or more tables to back up.
1. Specify the period of time during which the backup is to be retained. Note that there's an option to never delete the backup.
1. Optionally, specify the number of threads that should be available for the backup process.
1. Click **Create**.

Subsequent backups are created based on the value you specified for **Set backup intervals** or **Use cron expression**.

## Disable backups

You can disable all backups, including scheduled ones, as follows:

1. Navigate to the universe's **Tables** tab.
2. Click **Actions > Disable Backup**.

<!--

## Delete a scheduled backup

You can permanently remove a scheduled backup, as follows:

1. Navigate to your universe and select the **Backups** tab.
1. Find the scheduled backup and click **Options**.
1. Click **Delete schedule**.
    -->

---
title: What's new in YugabyteDB Managed
linkTitle: What's new
description: YugabyteDB Managed release notes and known issues.
headcontent: New features, cloud provider regions, and known issues
image: /images/section_icons/index/quick_start.png
layout: single
type: docs
---

On this page:

- [Releases](#releases)
- [Cloud provider regions](#cloud-provider-regions)
- [Known issues](#known-issues)

## Releases

### March 16, 2023

**Database**

- [Stable release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to [version 2.14.7](../../releases/release-notes/v2.14/#v2.14.7.0) for dedicated clusters. New clusters use this version by default.

### March 1, 2023

**Database**

- [Stable release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) reset to [version 2.12.9](../../releases/release-notes/v2.12/#v2.12.9.0) for dedicated clusters. New clusters use this version by default.

### February 13, 2023

**Database**

- [Preview release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to version [2.17.1](../../releases/release-notes/v2.17/#v2.17.1.0). New Sandbox clusters automatically use the new version. Existing clusters will be upgraded in the coming weeks.

- [Stable release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) reset to [version 2.14.6](../../releases/release-notes/v2.14/#v2.14.6.0) for dedicated clusters. New clusters use this version by default.

### February 8, 2023

**New Features**

- Users can now request a [time-limited free trial](../managed-freetrial/) to explore all the YugabyteDB Managed features.

**Database**

- [Stable release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to [version 2.16.1](../../releases/release-notes/v2.16/#v2.16.1.0) for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### January 27, 2023

**New Features**

- [YugabyteDB Managed Terraform Provider](https://registry.terraform.io/providers/yugabyte/ybm/latest) generally available. Use the provider to deploy and manage your database clusters in YugabyteDB Managed.

### January 18, 2023

**Database**

- [Stable release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to [version 2.16.0](../../releases/release-notes/v2.16/#v2.16.0.1) for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### January 13, 2023

**Database**

- [Stable release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to [version 2.14.6](../../releases/release-notes/v2.14/#v2.14.6.0) for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### December 21, 2022

**New Features**

- Ability to add IP addresses to the cluster IP allow list during cluster creation. The **Create Cluster** wizard includes the new **Network Access** page to configure connectivity for your cluster. Automatically detect and add your current IP address or the addresses of any peered VPC to the cluster.
- Ability to connect to clusters deployed in VPCs from public IP addresses. For clusters deployed in VPCs, enable **Public Access** on the **Settings** tab to connect from addresses outside the peered network. When enabled, a public IP address is added to each region of the cluster. You can view the private and public host addresses under **Connection Parameters** on the cluster **Settings** tab.

**Database**

- [Stable release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to [version 2.14.5](../../releases/release-notes/v2.14/#v2.14.5.0) for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### December 12, 2022

**New Features**

- [YugabyteDB Managed REST API](https://yugabyte.stoplight.io/docs/managed-apis) generally available. Use the REST API to deploy and manage your database clusters in YugabyteDB Managed programmatically.

### November 28, 2022

**New Features**

- Support for multi-region clusters with [geo-partitioning](../../explore/multi-region-deployments/row-level-geo-partitioning/) using the new [Partition by region](../cloud-basics/create-clusters-topology/#partition-by-region) deployment. Geo-partitioning allows you to move data closer to users to achieve lower latency and higher performance, and meet data residency requirements to comply with regulations such as GDPR.
- Support for [read replicas](../cloud-basics/create-clusters-topology/#read-replicas). Use read replicas to lower latencies for read requests from remote regions.

**Database**

- [Preview release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to version 2.15.3. New Sandbox clusters automatically use the new version. Existing clusters will be upgraded in the coming weeks.
- Stable release updated to version 2.14.4 for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### November 15, 2022

**New Features**

- Ability to view cluster health. YugabyteDB Managed monitors the health of your clusters based on cluster alert conditions and displays the health as either Healthy, Needs Attention, or Unhealthy.
- Ability to set alerts for failed nodes. Get notified when the number of failed nodes exceeds the threshold.

### November 4, 2022

**New Features**

- Ability to reset slow queries for faster debugging of slow-running queries.
- Ability to set a preferred region to tune the read and write latency for specific regions. Designating one region as preferred can reduce the number of network hops needed to process requests. The preferred region can be assigned during cluster creation, and set or changed after cluster creation.
- Ability to view details of task progress for cluster edit operations for better monitoring.

### October 24, 2022

**New Features**

- Support for role-based API keys. Assign [roles](../cloud-admin/manage-access/#user-roles) to API keys; keys assigned a developer role can't be used to perform admin tasks. In addition, keys are no longer revoked if the user that created the key is deleted from the account.

### October 17, 2022

**New Features**

- Ability to set alerts for cluster memory use and YSQL connections. Get notified when memory use or the number of YSQL connections in a cluster exceeds the threshold. High memory use or number of YSQL connections can indicate problems with your workload, such as unoptimized queries or problems with your application connection code.

### September 28, 2022

**Database**

- [Preview release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to version 2.15.2. New Sandbox clusters automatically use the new version. Existing clusters will be upgraded in the coming weeks.

### September 19, 2022

**Database**

- Stable release updated to version 2.14.2 for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### August 17, 2022

**Database**

- Stable release updated to version 2.14.1 for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### July 8, 2022

**Database**

- Preview release updated to version 2.15.0. New Sandbox clusters automatically use the new version. Existing clusters will be upgraded in the coming weeks.

### June 27, 2022

**New Features**

- Performance Optimizer for scanning clusters for optimizations (preview release). Provides recommendations on index and schema improvements, and detects connection, query, and CPU skew to identify potentially hot nodes.
- [YugabyteDB Managed REST API](https://yugabyte.stoplight.io/docs/managed-apis) (preview release). Use the REST API to deploy and manage your database clusters in YugabyteDB Managed programmatically.
- API key management for creating and managing bearer tokens for use with the YugabyteDB Managed REST API.

### June 22, 2022

**New Features**

- Support for creating multi-region replicated clusters (preview release). Create clusters that are resilient to region-level outages, with data synchronously replicated across 3 regions.

**Database**

- Stable release updated to version 2.12.6 for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

**Infrastructure**

- Instance type for new Dedicated clusters on AWS updated to [m5](https://aws.amazon.com/ec2/instance-types/m5/).

### June 14, 2022

**New Features**

- Support for social logins. Sign up and log in to YugabyteDB Managed using your existing Google, LinkedIn, or GitHub account. Admin users can manage the available login methods from the **Users** tab on the **Admin** page.

**Database**

- Stable release updated to version 2.12.5 for dedicated clusters. New clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### May 18, 2022

**Enhancements**

- Faster cluster creation. Create most clusters in under five minutes.

**Database**

- [Preview release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to version 2.13.1. New Sandbox clusters automatically use the new version. Existing clusters will be upgraded in the coming weeks.
- Stable release updated to version 2.12.3 for Dedicated clusters. New Dedicated clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

**Fixes**

- Cloud Shell is now available for clusters in a VPC.

### March 31, 2022

**New Features**

- Self-guided quickstart incorporated in Cloud Shell. Launch Cloud Shell using the YSQL API to begin a [self-guided tutorial](../cloud-quickstart/qs-explore/) exploring distributed SQL.

**Enhancements**

- Support in Cloud Shell to allow longer sessions (up to one hour) and up to five concurrent sessions.

### March 10, 2022

**New Features**

- Ability to schedule the maintenance window and exclusion periods for upcoming maintenance and database upgrades. The maintenance window is a weekly four-hour time slot during which Yugabyte may maintain or upgrade clusters. Yugabyte does not maintain or upgrade clusters outside the scheduled maintenance window, or during exclusion periods. Manage maintenance windows on the cluster **Maintenance** tab.
- Ability to manually pause and resume clusters. To pause a cluster, select the cluster, click **Actions**, and choose **Pause Cluster**. Yugabyte suspends instance vCPU capacity charges for paused clusters; disk and backup storage are charged at the standard rate.

### February 3, 2022

**New Features**

- Ability to select the [version](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) of YugabyteDB to install on a cluster when [creating Dedicated clusters](../cloud-basics/create-clusters/).
- Automated notifications of upcoming database maintenance. The notification email includes the date and time of the maintenance window. An Upcoming Maintenance badge is also displayed on the cluster. Start an upgrade any time by signing in to YugabyteDB Managed, selecting the cluster, clicking the **Upcoming Maintenance** badge, and clicking **Upgrade Now**.

**Infrastructure**

- Instance type for new Sandbox clusters on AWS updated to [EC2 T3.small](https://aws.amazon.com/ec2/instance-types/t3/). Existing Sandbox clusters retain their EC2 T2.small instance type for their lifetime.
- Instance type for new Dedicated clusters on GCP updated to [n2-standard](https://cloud.google.com/compute/docs/general-purpose-machines#n2_machines).

**Database**

- [Preview release](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) updated to version 2.11.1. New Sandbox clusters automatically use the new version. Existing clusters will be upgraded in the coming weeks.
- Stable release updated to version 2.8.1 for Dedicated clusters. New Dedicated clusters use the new version by default. Existing clusters will be upgraded in the coming weeks.

### January 27, 2022

**New Features**

- Support for [alerts](../cloud-monitor/cloud-alerts/) to notify you and your team members when cluster and database resource usage exceeds predefined limits, or of potential billing issues. Configure alerts and view notifications on the **Alerts** page. When an alert triggers, YugabyteDB Managed sends an email notification and displays a notification on the **Notifications** tab. When the alert condition resolves, the notification dismisses automatically. Alerts are enabled for all clusters in your account.
- Sandbox clusters are now [paused](../../faq/yugabytedb-managed-faq/#why-is-my-sandbox-cluster-paused) after 21 days of inactivity. YugabyteDB Managed sends a notification when your cluster is paused. To keep a cluster from being paused, perform an action as described in [What qualifies as activity on a cluster?](../../faq/yugabytedb-managed-faq/#what-qualifies-as-activity-on-a-cluster) Sandbox clusters are deleted after 30 days of inactivity.
- Ability to see the [version](../../faq/yugabytedb-managed-faq/#what-version-of-yugabytedb-does-my-cluster-run-on) of YugabyteDB that your cluster is running on.

**Fixes**

- Windows and Firefox keyboard shortcuts work correctly in Cloud Shell.

### December 16, 2021

**New Features**

- Self service [Virtual Private Cloud (VPC) networking](../cloud-basics/cloud-vpcs/). Use VPC networks to lower network latencies and make your application and database infrastructure more secure. Create VPCs in AWS or GCP and peer them with application VPCs in the same cloud provider. VPC networking is managed on the **VPC Network** tab of the **Network Access** page.
- Ability to [enable pre-bundled extensions](../cloud-clusters/add-extensions/) using the `CREATE EXTENSION` command. YugabyteDB includes [pre-bundled PostgreSQL extensions](../../explore/ysql-language-features/pg-extensions/) that are tested to work with YSQL. Admin users now have additional permissions to allow them to enable these extensions in databases. (If you need to install a database extension that is not pre-bundled, contact {{% support-cloud %}}
.)

### December 2, 2021

**New Features**

- Additional [performance metrics](../cloud-monitor/overview/). The new cluster **Performance Metrics** tab features new metrics including YSQL and YCQL operations per second, YSQL and YCQL latency, network bytes per second, and more. Use these metrics to ensure the cluster configuration matches its performance requirements.
- Ability to review running queries using the [Live Queries](../cloud-monitor/cloud-queries-live/) on the cluster **Performance** tab. Use this information to visually identify relevant database operations and evaluate query execution times.
- Ability to review slow YSQL queries using the [YSQL Slow Queries](../cloud-monitor/cloud-queries-slow/) on the cluster **Performance** tab. You can use this information to identify slower-running database operations, look at query execution times over time, and discover potential queries for tuning.

**Database**

- YugabyteDB updated to version 2.8. New clusters automatically use the new version. Existing clusters will be upgraded in the coming weeks.

### November 18, 2021

**New Features**

- Support for auditing account activity using the new **Activity** tab on the **Admin** page. The tab provides a running audit of activity, including:

  - backups
  - cluster creation and modification
  - changes to account users and their roles
  - billing changes
  - allow list changes

- Support for multiple Admin users on your account, and Admin users can now change the role of existing users. You can also invite multiple users at once, and assign them a role (Developer or Admin) when you invite them. You manage users using the **Users** tab on the **Admin** page.
- Additional charts on the **Invoices** on the **Billing** tab, which break costs down by cluster and infrastructure (instance costs, storage, and data transfer) so that you can quickly evaluate your costs.

**Fixes**

- Developer users can now use Cloud Shell.

### October 5, 2021

**New Features**

- The [YugabyteDB Managed Status](https://status.yugabyte.cloud/) page shows the current uptime status of YugabyteDB Managed and the [Yugabyte Support Portal](https://support.yugabyte.com/), along with maintenance notices and incident reports.
- Ability to review cluster activity using the new cluster **Activity** tab.

### September 15, 2021

**New Features**

- Ability to [create clusters](../cloud-basics/create-clusters/) suitable for production workloads. YugabyteDB Managed clusters support horizontal and vertical scaling, VPC peering, and scheduled and manual backups.
- Billing support. Set up a billing profile, manage payment methods, and review invoices on the [Billing](../cloud-admin/cloud-billing-profile) tab. (You must create a billing profile and add a payment method before you can create any clusters apart from your Sandbox cluster.)

### September 8, 2021

This release includes the following features:

- Sandbox clusters (one per account)
- AWS and GCP cloud support
- IP allow lists for network security
- Cloud Shell for running SQL queries from your browser
- YSQL and YCQL API support
- Multiple users - invite additional users
- Encryption at rest and in transit

## Cloud provider regions

### GCP

The following **GCP regions** are available:

- Taiwan (asia-east1)
- Honk Kong (asia-east2)
- Tokyo (asia-northeast1)
- Osaka (asia-northeast2)
- Seoul (asia-northeast3)
- Mumbai (asia-south1)
- Delhi (asia-south2)
- Singapore (asia-southeast1)
- Jakarta (asia-southeast2)
- Sydney (australia-southeast1)
- Melbourne (australia-southeast2)
- Warsaw (europe-central2)
- Finland (europe-north1)
- Belgium (europe-west1)
- London (europe-west2)
- Frankfurt (europe-west3)
- Netherlands (europe-west4)
- Zurich (europe-west6)
- Montreal (northamerica-northeast1)
- Toronto (northamerica-northeast2)
- Sao Paulo (southamerica-east1)
- Iowa (us-central1)
- South Carolina (us-east1)
- N. Virginia (us-east4)
- Oregon (us-west1)
- Los Angeles (us-west2)
- Salt Lake City (us-west3)
- Las Vegas (us-west4)

### AWS

The following **AWS regions** are available:

- Cape Town (af-south-1)
- Hong Kong (ap-east-1)
- Tokyo (ap-northeast-1)
- Seoul (ap-northeast-2)
- Osaka (ap-northeast-3)
- Mumbai (ap-south-1)
- Singapore (ap-southeast-1)
- Sydney (ap-southeast-2)
- Central (ca-central-1)
- Frankfurt (eu-central-1)
- Stockholm (eu-north-1)
- Milan (eu-south-1)
- Ireland (eu-west-1)
- London (eu-west-2)
- Paris (eu-west-3)
- Bahrain (me-south-1)
- Sao Paulo (sa-east-1)
- N. Virginia (us-east-1)
- Ohio (us-east-2)
- N. California (us-west-1)*
- Oregon (us-west-2)

\* Region has 2 availability zones only

## Known issues

- **Missing Slow Queries** - On clusters with multiple nodes, in some circumstances some nodes may not return all query statements when requested. If this happens, the query statements will be missing from the Slow Queries page.
- **Slow Queries Reset** - When resetting Slow Queries, the query used for the reset remains in the table.
- **Tables** - In some instances in Sandbox clusters, the **Tables** tab will show all tables with a size of 0B.
- **Clusters** - No support for scaling vCPUs on single node clusters.
- **Metrics** - The **View Full Screen** option in charts on the cluster **Overview** and **Performance Metrics** pages does not work in some versions of Safari 14.0 (Big Sur).
- **Metrics** - Some clusters in European regions may show occasional spikes in the YSQL Operations/sec chart. This is due to cluster health checks and can be ignored.
- **Widely-dispersed regions** - For multi-region clusters with widely-dispersed regions, Performance Advisor, Slow Queries, and some metrics may not return any results.
- **Read replicas**
  - Read replicas currently need to use the same instance type as the Primary cluster.
  - For clusters in AWS, removed read replicas can't be added back to the same region if the cluster has other read replicas. To add a read replica back to the same region, first remove all the read replicas and then add them all back.

### Known issues in Cloud Shell

- If [Cloud Shell](../cloud-connect/connect-cloud-shell/) stops responding, close the browser tab and restart Cloud Shell.
- Occasionally, Cloud Shell will take longer than normal to load; subsequent loads will be faster.

---
title: YugabyteDB Anywhere
headerTitle: YugabyteDB Anywhere
linkTitle: YugabyteDB Anywhere
description: YugabyteDB delivered as a private database-as-a-service for enterprises.
aliases:
  - /preview/yugabyte-platform/overview/
menu:
  preview_yugabyte-platform:
    parent: yugabytedb-anywhere
    identifier: overview-yp
    weight: 10
type: indexpage
breadcrumbDisable: true
resourcesIntro: Quick Links
resources:
  - title: What's new
    url: /preview/releases/release-notes/latest-release/
  - title: FAQ
    url: /preview/faq/yugabyte-platform/
  - title: Free trial
    url: https://www.yugabyte.com/anywhere/
---

YugabyteDB Anywhere is best fit for mission-critical deployments, such as production or pre-production testing. The YugabyteDB Anywhere UI is used in a highly-available mode, allowing you to create and manage YugabyteDB universes, or clusters, on one or more regions across public cloud and private on-premises data centers.

YugabyteDB Anywhere is a containerized application that you can install using [Replicated](https://www.replicated.com/) for production, performance, or failure mode testing. For local development or functional testing you can also use [YugabyteDB](../quick-start/).

You can access YugabyteDB Anywhere via an Internet browser that has been supported by its maker in the past 24 months and that has a market share of at least 0.2%. In addition, you can access YugabyteDB Anywhere via most mobile browsers, except Opera Mini.

YugabyteDB Anywhere offers three levels of user accounts: Super Admin, Admin, and Read-only, with the latter having rather limited access to functionality. Unless otherwise specified, the YugabyteDB Anywhere documentation describes the functionality available to a Super Admin user.

{{< sections/2-boxes >}}
  {{< sections/bottom-image-box
    title="Install"
    description="Install YugabyteDB Anywhere on any environment, including Kubernetes, public cloud, or private cloud."
    buttonText="Learn more"
    buttonUrl="install-yugabyte-platform/"
    imageAlt="Yugabyte cloud" imageUrl="/images/homepage/yugabyte-in-cloud.svg"
  >}}

  {{< sections/bottom-image-box
    title="Configure"
    description="Confiure YugabyteDB Anywhere for various cloud providers."
    buttonText="Learn more"
    buttonUrl="configure-yugabyte-platform/"
    imageAlt="Locally Laptop" imageUrl="/images/homepage/locally-laptop.svg"
  >}}
{{< /sections/2-boxes >}}

## Use YugabyteDB Anywhere

{{< sections/3-boxes >}}
  {{< sections/3-box-card
    title="Deploy"
    description="Deploy multi-region, multi-zone, and multi-cloud universes."
    buttonText="Deploy"
    buttonUrl="create-deployments/"
  >}}

  {{< sections/3-box-card
    title="Manage"
    description="Modify universes and their nodes, upgrade YugabyteDB software."
    buttonText="Manage"
    buttonUrl="manage-deployments/"
  >}}

  {{< sections/3-box-card
    title="Back up"
    description="Configure storage, back up and restore universe data."
    buttonText="Back up"
    buttonUrl="back-up-restore-universes/"
  >}}
{{< /sections/3-boxes >}}

## Additional resources

{{< sections/3-boxes >}}
  {{< sections/3-box-card
	title="Build applications"
	description="Start coding in your favorite programming language using examples."
	buttonText="Get started"
	buttonUrl="../develop/build-apps/"
  >}}

  {{< sections/3-box-card
	title="Yugabyte University"
	description="Take free courses and workshops to learn YugabyteDB, YSQL, and YCQL."
	linkText1="Developer workshops"
	linkUrl1="https://university.yugabyte.com/collections/builder-workshop"
	linkTarget1="_blank"
	linkText2="YSQL exercises"
	linkUrl2="https://university.yugabyte.com/courses/ysql-exercises-simple-queries"
	linkTarget2="_blank"
  >}}

  {{< sections/3-box-card
	title="Support"
	description="Ask questions, request assistance from our team, participate in our journey."
	linkText1="Contact Support"
	linkUrl1="https://support.yugabyte.com/hc/en-us/requests/new?ticket_form_id=360001955891"
	linkTarget1="_blank"
	linkText2="Join our community"
	linkUrl2="https://communityinviter.com/apps/yugabyte-db/register"
	linkTarget2="_blank"
  >}}

{{< /sections/3-boxes >}}

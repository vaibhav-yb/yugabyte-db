---
title: Install YugabyteDB Anywhere Overview
headerTitle: Overview
linkTitle: Overview
description: Installing YugabyteDB Anywhere on public clouds
image: /images/section_icons/deploy/enterprise.png
aliases:
  - /preview/yugabyte-platform/overview/install/
menu:
  preview_yugabyte-platform:
    identifier: install-1-public-cloud
    parent: install-yugabyte-platform
    weight: 20
type: docs
---

For installation overview, select one of the following installation types:

<ul class="nav nav-tabs-alt nav-tabs-yb">
  <li >
    <a href="../public-cloud/" class="nav-link active">
      <i class="fa-solid fa-cloud"></i>
      Public Cloud
    </a>
  </li>

  <li>
    <a href="../kubernetes/" class="nav-link">
      <i class="fa-regular fa-dharmachakra" aria-hidden="true"></i>
      Kubernetes
    </a>
  </li>

  <li >
    <a href="../private-cloud/" class="nav-link">
      <i class="fa-solid fa-link-slash"></i>
      Private Cloud
    </a>
  </li>
</ul>

The following diagram depicts the YugabyteDB Anywhere installation process in a public cloud:

<div class="image-with-map">
<img src="/images/ee/flowchart/yb-install-public-cloud.png" usemap="#image-map">

<map name="image-map">
    <area alt="Install YugabyteDB Anywhere" title="Install YugabyteDB Anywhere" href="/preview/yugabyte-platform/install-yugabyte-platform/" coords="397,199,371,90,450,48,523,90,518,174,518,175,453,214,453,213" shape="poly" style="width: 18.3%;height: 8.7%;top: 2.6%;left: 41%;">
    <area alt="AWS prep environment" title="AWS prep environment" href="/preview/yugabyte-platform/install-yugabyte-platform/prepare-environment/aws/" coords="166,404,296,480" shape="rect" style="width: 16.5%;height: 4.1%;top: 21.2%;left: 17.5%;">
    <area alt="GCP prep environment" title="GCP prep environment" href="/preview/yugabyte-platform/install-yugabyte-platform/prepare-environment/gcp/" coords="378,404,521,480" shape="rect" style="width: 16.5%;height: 4.1%;top: 21.2%;left: 41.8%;">
    <area alt="Azure prep environment" title="Azure prep environment" href="/preview/yugabyte-platform/install-yugabyte-platform/prepare-environment/azure/" coords="590,404,746,480" shape="rect" style="width: 16.5%;height: 4.1%;top: 21.2%;left: 66%;">
    <area alt="Pre reqs platform" title="Pre reqs platform" href="/preview/yugabyte-platform/install-yugabyte-platform/prerequisites/" coords="324,558,574,711" shape="rect" style="width: 81%;height: 3.2%;top: 40.3%;left: 9.5%;">
    <area alt="Online installation" title="Online installation" href="/preview/yugabyte-platform/install-yugabyte-platform/install-software/default/" coords="236,1054,394,1112" shape="rect" style="width: 19%;height: 3.4%;top: 55.4%;left: 25.5%;">
    <area alt="Airgapped installation" title="Airgapped installation" href="/preview/yugabyte-platform/install-yugabyte-platform/install-software/airgapped/" coords="502,1053,666,1114" shape="rect" style="width: 19%;height: 3.4%;top: 55.4%;left: 55.5%;">
</map>
</div>

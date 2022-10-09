---
title: Install YugabyteDB Anywhere software - Kubernetes
headerTitle: Install YugabyteDB Anywhere software - Kubernetes
linkTitle: Install software
description: Install YugabyteDB Anywhere software in your Kubernetes environment.
menu:
  preview_yugabyte-platform:
    parent: install-yugabyte-platform
    identifier: install-software-2-kubernetes
    weight: 77
type: docs
---

<ul class="nav nav-tabs-alt nav-tabs-yb">

  <li>
    <a href="../default/" class="nav-link">
      <i class="fas fa-cloud"></i>Default</a>
  </li>

  <li>
    <a href="../kubernetes/" class="nav-link active">
      <i class="fas fa-cubes" aria-hidden="true"></i>Kubernetes</a>
  </li>

  <li>
    <a href="../airgapped/" class="nav-link">
      <i class="fas fa-unlink"></i>Airgapped</a>
  </li>

  <li>
    <a href="../openshift/" class="nav-link">
      <i class="fas fa-cubes"></i>OpenShift</a>
  </li>

</ul>

## Install YugabyteDB Anywhere on a Kubernetes cluster

You install YugabyteDB Anywhere on a Kubernetes cluster as follows:

1. Create a namespace by executing the following `kubectl create namespace` command:

    ```sh
    kubectl create namespace yb-platform
    ```

1. Apply the YugabyteDB Anywhere secret that you obtained from [Yugabyte](https://www.yugabyte.com/platform/#request-trial-form) by running the following `kubectl create` command:

    ```sh
    kubectl create -f yugabyte-k8s-secret.yml -n yb-platform
    ```

    Expect the following output notifying you that the secret was created:

    ```output
    secret/yugabyte-k8s-pull-secret created
    ```

1. Run the following `helm repo add` command to clone the [YugabyteDB charts repository](https://charts.yugabyte.com/):

    ```sh
    helm repo add yugabytedb https://charts.yugabyte.com
    ```

    A message similar to the following should appear:

    ```output
    "yugabytedb" has been added to your repositories
    ```

    To search for the available chart version, run the following command:

    ```sh
    helm search repo yugabytedb/yugaware --version {{<yb-version version="preview" format="short">}}
    ```

    The latest Helm chart version and application version is displayed via the output similar to the following:

    ```output
    NAME                 CHART VERSION  APP VERSION  DESCRIPTION
    yugabytedb/yugaware {{<yb-version version="preview" format="short">}}          {{<yb-version version="preview" format="build">}}  YugaWare is YugaByte Database's Orchestration a...
    ```

1. Run the following `helm install` command to install the YugabyteDB Anywhere (`yugaware`) Helm chart:

    ```sh
    helm install yw-test yugabytedb/yugaware --version {{<yb-version version="preview" format="short">}} -n yb-platform --wait
    ```

1. Optionally, set the TLS version for Nginx frontend by using `ssl_protocols` operational directive in the Helm installation, as follows:

    ```sh
    helm install yw-test yugabytedb/yugaware --version {{<yb-version version="preview" format="short">}} -n yb-platform --wait --set tls.sslProtocols="TLSv1.2"
    ```

    In addition, you may provide a custom TLS certificate in the Helm chart, as follows:

    ```sh
    helm install yw-test yugabytedb/yugaware --version {{<yb-version version="preview" format="short">}} -n yb-platform --wait --set tls.sslProtocols="TLSv1.2" tls.certificate="LS0tLS1CRUdJTiBDRVJUSUZJQ..." tls.key="LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0t..."
    ```

    where `certificate` and `key` are set to full string values of your custom certificate and its corresponding key.

1. Use the following command to check the service:

    ```sh
    kubectl get svc -n yb-platform
    ```

    The following output should appear:

    ```output
    NAME                  TYPE           CLUSTER-IP     EXTERNAL-IP    PORT(S)                       AGE
    yw-test-yugaware-ui   LoadBalancer   10.111.241.9   34.93.169.64   80:32006/TCP,9090:30691/TCP   2m12s
    ```

1. Use the following command to check that all the pods have been initialized and are running:

    ```sh
    kubectl get pods -n yb-platform
    ```

    The following output should appear:

    ```output
    NAME                 READY   STATUS    RESTARTS   AGE
    yw-test-yugaware-0   4/4     Running   0          12s
    ```

    Note that even though the preceding output indicates that the `yw-test-yugaware-0` pod is running, it does not mean that YugabyteDB Anywhere is ready to accept your queries. If you open `localhost:80` and see an error (such as 502), it means that `yugaware` is still being initialized. You can check readiness of `yugaware` by executing the following command:

    ```sh
    kubectl logs --follow -n yb-platform yw-test-yugaware-0 yugaware
    ```

    An output similar to the following would confirm that there are no errors and that the server is running:

    ```
    [info] AkkaHttpServer.scala:447 [main] Listening for HTTP on /0.0.0.0:9000
    ```

    If YugabyteDB Anywhere fails to start for the first time, verify that your system meets the installation requirements, as per [Prepare the Kubernetes environment](../../prepare-environment/kubernetes/).

## Customize YugabyteDB Anywhere

You can customize YugabyteDB Anywhere on a Kubernetes cluster in a number of ways, such as the following:

- You can change CPU and memory resources by executing a command similar to the following:

  ```sh
  helm install yw-test yugabytedb/yugaware -n yb-platform \
    --version {{<yb-version version="preview" format="short">}} \
    --set yugaware.resources.requests.cpu=2 \
    --set yugaware.resources.requests.memory=4Gi \
    --set yugaware.resources.limits.cpu=2 \
    --set yugaware.resources.limits.memory=4Gi \
    --set prometheus.resources.requests.mem=6Gi
  ```

- You can disable the public-facing load balancer by providing the annotations to YugabyteDB Anywhere service for disabling that load balancer. Since every cloud provider has different annontations for doing this, refer to the following documentation:

  - For Google Cloud, see [GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/internal-load-balancing).
  - For Azure, see [AKS](https://docs.microsoft.com/en-us/azure/aks/internal-lb).
  - For AWS, see [EKS](https://docs.aws.amazon.com/eks/latest/userguide/load-balancing.html).

  For example, for a GKE version earlier than 1.17, you would run a command similar to the following:
  
  ```sh
  helm install yw-test yugabytedb/yugaware -n yb-platform \
    --version {{<yb-version version="preview" format="short">}} \
    --set yugaware.service.annotations."cloud\.google\.com\/load-balancer-type"="Internal"
  ```

## Control placement of YugabyteDB Anywhere Pod

The Helm chart allows you to control the placement of the pod when installing YugabyteDB Anywhere in your Kubernetes cluster via `nodeSelector`, `zoneAffinity` and `toleration`. When you use these mechanisms to restrict placement of the YugabyteDB Anywhere pod, you should delay the creation of storage volumes (known as PersistentVolumeClaim (PVC)) until the pod has been placed. To do this, you would use a `StorageClass` with its `VolumeBindingMode` set to `WaitForFirstConsumer`, as described in [Configure storage class volume binding](../../../troubleshoot/universe-issues/#configure-storage-class-volume-binding). The following is a storage class YAML file for Google Kubernetes Engine (GKE):

```yaml
kind: StorageClass
metadata:
  name: yb-storage
provisioner: kubernetes.io/gce-pd
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: Delete
parameters:
  type: pd-ssd
  fstype: xfs
```
If you do not delay the creation of the PVC, it may be created in a location that is not accessible to the pod, resulting in a failure to bring up the pod to a running state.

### nodeSelector

The Kubernetes `nodeSelector` field provides the means to constrain pods to nodes with specific labels, allowing you to restrict the placement of YugabyteDB Anywhere pod on a particular node, as demonstrated by the following example:

```sh
helm install yw-test yugabytedb/yugaware/ -n yb-platform \
--version 2.15.2 --set yugaware.storageClass=yb-storage \
--set nodeSelector.kubernetes\\.io/hostname=node-name-1
```

### zoneAffinity

Kubernetes provides a flexible `nodeAffinity` construct to constrain the placement of pods to nodes in a given zone.

When your Kubernetes cluster nodes are spread across multiple zones, you can use this command to explicitly place the YugabyteDB Anywhere pod on specific zones, as demonstrated by the following example:

```sh
helm install yw-test yugabytedb/yugaware/ -n yb-platform \
--version 2.15.2 --set yugaware.storageClass=yb-storage \
--set "zoneAffinity={us-west1-a,us-west1-b}"
```

### toleration

Kubernetes nodes could have `taints` that repel pods from being placed on it. Only pods with a `toleration` for the same `taint` are permitted. For more information, see [Taints and Tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/).

For example, if some of the nodes in your Kubernetes cluster are earmarked for experimentation and have a taint `dedicated=experimental:NoSchedule`, only pods with the matching toleration will be allowed; other pods will be prevented from being placed on these nodes.

```sh
helm install yw-test yugabytedb/yugaware/ -n yb-platform \
--version 2.15.2 --set yugaware.storageClass=yb-storage \
--values=/tmp/overrides.yaml
```

Where the `/tmp/overrides.yaml` has the contents:

```yaml
tolerations:
- key: "dedicated"
  operator: "Equal"
  value: "experimental"
  effect: "NoSchedule"
```

## Delete the Helm Installation of YugabyteDB Anywhere

To delete the Helm installation, run the following command:

```sh
helm uninstall yw-test -n yb-platform
```

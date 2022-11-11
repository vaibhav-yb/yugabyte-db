// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.common;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.api.model.events.v1.Event;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableMap;

@Singleton
@Slf4j
public class NativeKubernetesManager extends KubernetesManager {
  private KubernetesClient getClient(Map<String, String> config) {
    if (config.containsKey("KUBECONFIG") && !config.get("KUBECONFIG").isEmpty()) {
      try {
        String kubeConfigContents =
            new String(Files.readAllBytes(Paths.get(config.get("KUBECONFIG"))));
        Config kubernetesConfig = Config.fromKubeconfig(kubeConfigContents);
        return new DefaultKubernetesClient(kubernetesConfig);
      } catch (IOException e) {
        throw new RuntimeException("Unable to resolve Kubernetes Client: ", e);
      }
    }
    return new DefaultKubernetesClient();
  }

  @Override
  public void createNamespace(Map<String, String> config, String namespace) {
    try (KubernetesClient client = getClient(config)) {
      client
          .namespaces()
          .createOrReplace(
              new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build());
    }
  }

  @Override
  public void applySecret(Map<String, String> config, String namespace, String pullSecret) {
    try (KubernetesClient client = getClient(config);
        InputStream pullSecretStream =
            Files.newInputStream(Paths.get(pullSecret), StandardOpenOption.READ); ) {
      client.load(pullSecretStream).inNamespace(namespace).createOrReplace();
    } catch (IOException e) {
      throw new RuntimeException("Unable to get the pullSecret ", e);
    }
  }

  @Override
  public List<Pod> getPodInfos(
      Map<String, String> config, String universePrefix, String namespace) {
    // Implementation specific helm release name.
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    try (KubernetesClient client = getClient(config)) {
      return client
          .pods()
          .inNamespace(namespace)
          .withLabel("release", helmReleaseName)
          .list()
          .getItems();
    }
  }

  @Override
  public List<Service> getServices(
      Map<String, String> config, String universePrefix, String namespace) {
    // Implementation specific helm release name.
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    try (KubernetesClient client = getClient(config)) {
      return client
          .services()
          .inNamespace(namespace)
          .withLabel("release", helmReleaseName)
          .list()
          .getItems();
    }
  }

  @Override
  public PodStatus getPodStatus(Map<String, String> config, String namespace, String podName) {
    try (KubernetesClient client = getClient(config)) {
      return client.pods().inNamespace(namespace).withName(podName).get().getStatus();
    }
  }

  @Override
  public Pod getPodObject(Map<String, String> config, String namespace, String podName) {
    try (KubernetesClient client = getClient(config)) {
      return client.pods().inNamespace(namespace).withName(podName).get();
    }
  }

  @Override
  public String getPreferredServiceIP(
      Map<String, String> config,
      String universePrefix,
      String namespace,
      boolean isMaster,
      boolean newNamingStyle) {
    String appLabel = newNamingStyle ? "app.kubernetes.io/name" : "app";
    String appName = isMaster ? "yb-master" : "yb-tserver";
    try (KubernetesClient client = getClient(config)) {
      // TODO(bhavin192): this might need to be changed when we
      // support multi-cluster environments.
      List<Service> services =
          client
              .services()
              .inNamespace(namespace)
              .withLabel(appLabel, appName)
              .withLabel("release", universePrefix)
              .withoutLabel("service-type", "headless")
              .list()
              .getItems();
      if (services.size() != 1) {
        throw new RuntimeException(
            "There must be exactly one Master or TServer endpoint service, got " + services.size());
      }
      return getIp(services.get(0));
    }
  }

  @Override
  public List<Node> getNodeInfos(Map<String, String> config) {
    try (KubernetesClient client = getClient(config)) {
      return client.nodes().list().getItems();
    }
  }

  @Override
  public Secret getSecret(
      Map<String, String> config, String secretName, @Nullable String namespace) {
    try (KubernetesClient client = getClient(config)) {
      if (namespace == null) {
        return client.secrets().withName(secretName).get();
      }
      return client.secrets().inNamespace(namespace).withName(secretName).get();
    }
  }

  @Override
  public void updateNumNodes(
      Map<String, String> config,
      String universePrefix,
      String namespace,
      int numNodes,
      boolean newNamingStyle) {
    String appLabel = newNamingStyle ? "app.kubernetes.io/name" : "app";
    try (KubernetesClient client = getClient(config)) {
      // https://github.com/fabric8io/kubernetes-client/issues/3948
      MixedOperation<StatefulSet, StatefulSetList, RollableScalableResource<StatefulSet>>
          statefulSets = client.apps().statefulSets();
      statefulSets
          .inNamespace(namespace)
          .withLabel("release", universePrefix)
          .withLabel(appLabel, "yb-tserver")
          .list()
          .getItems()
          .forEach(
              s ->
                  statefulSets
                      .inNamespace(namespace)
                      .withName(s.getMetadata().getName())
                      .scale(numNodes));
    }
  }

  @Override
  public void deleteStorage(Map<String, String> config, String universePrefix, String namespace) {
    // Implementation specific helm release name.
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    try (KubernetesClient client = getClient(config)) {
      client
          .persistentVolumeClaims()
          .inNamespace(namespace)
          .withLabel("release", helmReleaseName)
          .delete();
    }
  }

  @Override
  public void deleteNamespace(Map<String, String> config, String namespace) {
    try (KubernetesClient client = getClient(config)) {
      client.namespaces().withName(namespace).delete();
    }
  }

  @Override
  public void deletePod(Map<String, String> config, String namespace, String podName) {
    try (KubernetesClient client = getClient(config)) {
      client.pods().inNamespace(namespace).withName(podName).delete();
    }
  }

  @Override
  public List<Event> getEvents(Map<String, String> config, String namespace) {
    try (KubernetesClient client = getClient(config)) {
      return client.events().v1().events().inNamespace(namespace).list().getItems();
    }
  }

  @Override
  public boolean deleteStatefulSet(Map<String, String> config, String namespace, String stsName) {
    try (KubernetesClient client = getClient(config)) {
      return client
          .apps()
          .statefulSets()
          .inNamespace(namespace)
          .withName(stsName)
          .withPropagationPolicy(DeletionPropagation.ORPHAN)
          .delete();
    }
  }

  @Override
  public boolean expandPVC(
      Map<String, String> config,
      String namespace,
      String universePrefix,
      String appLabel,
      String newDiskSize) {
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    Map<String, String> labels = ImmutableMap.of("app", appLabel, "release", helmReleaseName);
    try (KubernetesClient client = getClient(config)) {
      List<PersistentVolumeClaim> pvcs =
          client
              .persistentVolumeClaims()
              .inNamespace(namespace)
              .withLabels(labels)
              .list()
              .getItems();
      for (PersistentVolumeClaim pvc : pvcs) {
        log.info("Updating PVC size for {} to {}", pvc.getMetadata().getName(), newDiskSize);
        pvc.getSpec().getResources().getRequests().put("storage", new Quantity(newDiskSize));
        client.persistentVolumeClaims().patch(PatchContext.of(PatchType.STRATEGIC_MERGE), pvc);
      }
      return true;
    }
  }

  @Override
  public String getStorageClassName(
      Map<String, String> config, String namespace, String universePrefix, boolean forMaster) {
    // TODO: Implement when switching to native client implementation
    return null;
  }

  @Override
  public boolean storageClassAllowsExpansion(Map<String, String> config, String storageClassName) {
    // TODO: Implement when switching to native client implementation
    return true;
  }

  @Override
  public void diff(Map<String, String> config, String inputYamlFilePath) {
    // TODO(anijhawan): Implement this when we get a chance.
  }

  @Override
  public String getCurrentContext(Map<String, String> azConfig) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String execCommandProcessErrors(Map<String, String> config, List<String> commandList) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getK8sResource(
      Map<String, String> config, String k8sResource, String namespace, String outputFormat) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getEvents(Map<String, String> config, String namespace, String string) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getK8sVersion(Map<String, String> config, String outputFormat) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getPlatformNamespace() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getHelmValues(
      Map<String, String> config, String namespace, String helmReleaseName, String outputFormat) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<RoleData> getAllRoleDataForServiceAccountName(
      Map<String, String> config, String serviceAccountName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getServiceAccountPermissions(
      Map<String, String> config, RoleData roleData, String outputFormat) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getStorageClass(
      Map<String, String> config, String storageClassName, String namespace, String outputFormat) {
    // TODO Auto-generated method stub
    return null;
  }
}

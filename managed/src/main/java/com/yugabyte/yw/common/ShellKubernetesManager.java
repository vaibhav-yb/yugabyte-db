// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.yugabyte.yw.common.SupportBundleUtil.KubernetesResourceType;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimCondition;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.events.v1.Event;
import io.fabric8.kubernetes.api.model.events.v1.EventList;
import lombok.extern.slf4j.Slf4j;
import play.libs.Json;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

@Singleton
@Slf4j
public class ShellKubernetesManager extends KubernetesManager {

  @Inject ReleaseManager releaseManager;

  @Inject ShellProcessHandler shellProcessHandler;

  @Inject play.Configuration appConfig;

  public static final Logger LOG = LoggerFactory.getLogger(ShellKubernetesManager.class);

  private ShellResponse execCommand(Map<String, String> config, List<String> command) {
    return execCommand(config, command, true /*logCmdOutput*/);
  }

  private ShellResponse execCommand(
      Map<String, String> config, List<String> command, boolean logCmdOutput) {
    String description = String.join(" ", command);
    return shellProcessHandler.run(
        command,
        ShellProcessContext.builder()
            .description(description)
            .extraEnvVars(config)
            .logCmdOutput(logCmdOutput)
            .build());
  }

  private <T> T deserialize(String json, Class<T> type) {
    try {
      return new ObjectMapper().configure(Feature.ALLOW_SINGLE_QUOTES, true).readValue(json, type);
    } catch (Exception e) {
      throw new RuntimeException("Error deserializing response from kubectl command: ", e);
    }
  }

  @Override
  public void createNamespace(Map<String, String> config, String namespace) {
    String namespaceFile = generateNamespaceYaml(namespace);
    List<String> commandList =
        ImmutableList.of("kubectl", "apply", "-f", namespaceFile, "--server-side");
    execCommand(config, commandList).processErrors();
  }

  // TODO(bhavin192): modify the pullSecret on the fly while applying
  // it? Add nodePrefix to the name, add labels which make it easy to
  // find the secret by label selector, and even delete it if
  // required. Something like, -lprovider=gke1 or
  // -luniverse=uni1. Tracked here:
  // https://github.com/yugabyte/yugabyte-db/issues/7012
  @Override
  public void applySecret(Map<String, String> config, String namespace, String pullSecret) {
    // --server-side flag ensures that there is no race condition when
    // multiple kubectl apply try to apply same secret in same
    // namespace.
    List<String> commandList =
        ImmutableList.of(
            "kubectl", "apply", "-f", pullSecret, "--namespace", namespace, "--server-side");
    execCommand(config, commandList).processErrors();
  }

  @Override
  public List<Pod> getPodInfos(
      Map<String, String> config, String universePrefix, String namespace) {
    // Implementation specific helm release name.
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    List<String> commandList =
        ImmutableList.of(
            "kubectl",
            "get",
            "pods",
            "--namespace",
            namespace,
            "-o",
            "json",
            "-l",
            "release=" + helmReleaseName);
    ShellResponse response =
        execCommand(config, commandList, false /*logCmdOutput*/).processErrors();
    return deserialize(response.message, PodList.class).getItems();
  }

  @Override
  public List<Service> getServices(
      Map<String, String> config, String universePrefix, String namespace) {
    // Implementation specific helm release name.
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    List<String> commandList =
        ImmutableList.of(
            "kubectl",
            "get",
            "services",
            "--namespace",
            namespace,
            "-o",
            "json",
            "-l",
            "release=" + helmReleaseName);
    ShellResponse response = execCommand(config, commandList).processErrors();
    return deserialize(response.message, ServiceList.class).getItems();
  }

  @Override
  public Pod getPodObject(Map<String, String> config, String namespace, String podName) {
    List<String> commandList =
        ImmutableList.of("kubectl", "get", "pod", "--namespace", namespace, "-o", "json", podName);
    ShellResponse response =
        execCommand(config, commandList, false /*logCmdOutput*/).processErrors();
    return deserialize(response.message, Pod.class);
  }

  @Override
  public PodStatus getPodStatus(Map<String, String> config, String namespace, String podName) {
    return getPodObject(config, namespace, podName).getStatus();
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
    // TODO(bhavin192): this might need to be changed when we support
    // multi-cluster environments.
    String selector =
        String.format("release=%s,%s=%s,service-type!=headless", universePrefix, appLabel, appName);
    List<String> commandList =
        ImmutableList.of(
            "kubectl", "get", "svc", "--namespace", namespace, "-l", selector, "-o", "json");
    ShellResponse response = execCommand(config, commandList).processErrors();
    List<Service> services = deserialize(response.message, ServiceList.class).getItems();
    if (services.size() != 1) {
      throw new RuntimeException(
          "There must be exactly one Master or TServer endpoint service, got " + services.size());
    }
    return getIp(services.get(0));
  }

  @Override
  public List<Node> getNodeInfos(Map<String, String> config) {
    List<String> commandList = ImmutableList.of("kubectl", "get", "nodes", "-o", "json");
    ShellResponse response =
        execCommand(config, commandList, false /*logCmdOutput*/)
            .processErrors("Unable to get node information");
    return deserialize(response.message, NodeList.class).getItems();
  }

  @Override
  public Secret getSecret(Map<String, String> config, String secretName, String namespace) {
    List<String> commandList = new ArrayList<String>();
    commandList.addAll(ImmutableList.of("kubectl", "get", "secret", secretName, "-o", "json"));
    if (namespace != null) {
      commandList.add("--namespace");
      commandList.add(namespace);
    }
    ShellResponse response =
        execCommand(config, commandList, false /*logCmdOutput*/)
            .processErrors("Unable to get secret");
    return deserialize(response.message, Secret.class);
  }

  @Override
  public void updateNumNodes(
      Map<String, String> config,
      String universePrefix,
      String namespace,
      int numNodes,
      boolean newNamingStyle) {
    String appLabel = newNamingStyle ? "app.kubernetes.io/name" : "app";
    String selector = String.format("release=%s,%s=yb-tserver", universePrefix, appLabel);
    List<String> commandList =
        ImmutableList.of(
            "kubectl",
            "--namespace",
            namespace,
            "scale",
            "statefulset",
            "-l",
            selector,
            "--replicas=" + numNodes);
    execCommand(config, commandList).processErrors();
  }

  @Override
  public void deleteStorage(Map<String, String> config, String universePrefix, String namespace) {
    // Implementation specific helm release name.
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    // Delete Master and TServer Volumes
    List<String> commandList =
        ImmutableList.of(
            "kubectl",
            "delete",
            "pvc",
            "--namespace",
            namespace,
            "-l",
            "release=" + helmReleaseName);
    execCommand(config, commandList);
    // TODO: check the execCommand outputs.
  }

  @Override
  public void deleteNamespace(Map<String, String> config, String namespace) {
    // Delete Namespace
    List<String> masterCommandList = ImmutableList.of("kubectl", "delete", "namespace", namespace);
    execCommand(config, masterCommandList);
    // TODO: process any errors. Don't raise exception in case of not
    // found error, this can happen when same namespace is deleted by
    // multiple invocations concurrently.
  }

  @Override
  public void deletePod(Map<String, String> config, String namespace, String podName) {
    List<String> masterCommandList =
        ImmutableList.of("kubectl", "--namespace", namespace, "delete", "pod", podName);
    execCommand(config, masterCommandList).processErrors("Unable to delete pod");
  }

  @Override
  public List<Event> getEvents(Map<String, String> config, String namespace) {
    List<String> commandList =
        ImmutableList.of("kubectl", "get", "events", "-n", namespace, "-o", "json");
    ShellResponse response =
        execCommand(config, commandList, false /*logCmdOutput*/)
            .processErrors("Unable to list events");
    return deserialize(response.message, EventList.class).getItems();
  }

  // generateNamespaceYaml creates a namespace YAML file for given
  // name. This can be later extended to add other metadata like
  // labels, annotations etc.
  private String generateNamespaceYaml(String name) {
    Map<String, Object> namespace = new HashMap<String, Object>();
    namespace.put("apiVersion", "v1");
    namespace.put("kind", "Namespace");
    namespace.put("metadata", ImmutableMap.of("name", name));
    Yaml yaml = new Yaml();

    try {
      Path tempFile = Files.createTempFile(UUID.randomUUID().toString() + "-namespace", ".yml");
      BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile.toFile()));
      yaml.dump(namespace, bw);
      return tempFile.toAbsolutePath().toString();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw new RuntimeException("Error writing Namespace YAML file.");
    }
  }

  @Override
  public boolean deleteStatefulSet(Map<String, String> config, String namespace, String stsName) {
    List<String> commandList =
        ImmutableList.of(
            "kubectl",
            "--namespace",
            namespace,
            "delete",
            "statefulset",
            stsName,
            "--cascade=orphan");
    ShellResponse response =
        execCommand(config, commandList, false).processErrors("Unable to delete StatefulSet");
    return response.isSuccess();
  }

  @Override
  public boolean expandPVC(
      Map<String, String> config,
      String namespace,
      String universePrefix,
      String appLabel,
      String newDiskSize) {
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    String labelSelector = String.format("app=%s,release=%s", appLabel, helmReleaseName);
    List<String> commandList =
        ImmutableList.of(
            "kubectl", "--namespace", namespace, "get", "pvc", "-l", labelSelector, "-o", "name");
    ShellResponse response =
        execCommand(config, commandList, false).processErrors("Unable to get PVCs");
    log.info("Expanding PVCs: {}", response.getMessage());
    ObjectNode patchObj = Json.newObject();
    patchObj
        .putObject("spec")
        .putObject("resources")
        .putObject("requests")
        .put("storage", newDiskSize);
    String patchStr = patchObj.toString();
    boolean patchSuccess = true;
    for (String pvcName : response.getMessage().split("\n")) {
      commandList =
          ImmutableList.of("kubectl", "--namespace", namespace, "patch", pvcName, "-p", patchStr);
      response = execCommand(config, commandList, false).processErrors("Unable to patch PVC");
      patchSuccess &= response.isSuccess() && waitForPVCExpand(config, namespace, pvcName);
    }
    return patchSuccess;
  }

  // Ref: https://kubernetes.io/blog/2022/05/05/volume-expansion-ga/
  // The PVC status condition is cleared once PVC resize is done.
  private boolean waitForPVCExpand(Map<String, String> config, String namespace, String pvcName) {
    RetryTaskUntilCondition<List<PersistentVolumeClaimCondition>> waitForExpand =
        new RetryTaskUntilCondition<>(
            // task
            () -> {
              List<String> commandList =
                  ImmutableList.of(
                      "kubectl", "--namespace", namespace, "get", pvcName, "-o", "json");
              ShellResponse response =
                  execCommand(config, commandList, false).processErrors("Unable to get PVC");
              List<PersistentVolumeClaimCondition> pvcConditions =
                  deserialize(response.message, PersistentVolumeClaim.class)
                      .getStatus()
                      .getConditions();
              return pvcConditions;
            },
            // until condition
            pvcConditions -> {
              if (!pvcConditions.isEmpty()) {
                log.info(
                    "Waiting for condition to clear from PVC {}/{}: {}",
                    namespace,
                    pvcName,
                    pvcConditions.get(0));
              }
              return pvcConditions.isEmpty();
            },
            // delay between retry of task
            2,
            // timeout for retry in secs
            getTimeoutSecs());
    return waitForExpand.retryUntilCond();
  }

  @Override
  public String getStorageClassName(
      Map<String, String> config, String namespace, String universePrefix, boolean forMaster) {
    String appLabel = forMaster ? "yb-master" : "yb-tserver";
    String helmReleaseName = Util.sanitizeHelmReleaseName(universePrefix);
    String labelSelector = String.format("app=%s,release=%s", appLabel, helmReleaseName);
    List<String> commandList =
        ImmutableList.of(
            "kubectl",
            "--namespace",
            namespace,
            "get",
            "pvc",
            "-l",
            labelSelector,
            "-o",
            "jsonpath='{.items[0].spec.storageClassName}'");
    ShellResponse response =
        execCommand(config, commandList, false)
            .processErrors("Unable to read StorageClass name for yb-tserver PVC");
    return deserialize(response.getMessage(), String.class);
  }

  @Override
  public boolean storageClassAllowsExpansion(Map<String, String> config, String storageClassName) {
    List<String> commandList =
        ImmutableList.of(
            "kubectl", "get", "sc", storageClassName, "-o", "jsonpath='{.allowVolumeExpansion}'");
    ShellResponse response =
        execCommand(config, commandList, false)
            .processErrors("Unable to read StorageClass volume expansion");
    String allowsExpansion = deserialize(response.getMessage(), String.class);
    return allowsExpansion.equalsIgnoreCase("true");
  }

  public void checkAndAddFlagToCommand(List<String> commandList, String flagKey, String flagValue) {
    if (StringUtils.isNotBlank(flagValue)) {
      commandList.add(flagKey);
      commandList.add(flagValue);
    }
  }

  /**
   * Returns the current-context on a kubernetes cluster given its kubeconfig
   *
   * @param config the environment variables to set (KUBECONFIG, OVERRIDES, STORAGE_CLASS, etc.).
   * @return the current-context.
   */
  @Override
  public String getCurrentContext(Map<String, String> config) {
    List<String> commandList = ImmutableList.of("kubectl", "config", "current-context");
    ShellResponse response =
        execCommand(config, commandList).processErrors("Unable to get current-context");
    return StringUtils.defaultString(response.message);
  }

  /**
   * Executes the command passed, and stores the command output in the file given. Rewrites the file
   * if it already exists.
   *
   * @param config the environment variables to set (KUBECONFIG, OVERRIDES, STORAGE_CLASS, etc.).
   * @param commandList the list of strings forming a shell command when joined.
   * @param localFilePath the local file path to store the output of command executed.
   * @return true if command executed and output saved successfully, else false.
   */
  @Override
  public String execCommandProcessErrors(Map<String, String> config, List<String> commandList) {
    ShellResponse response =
        execCommand(config, commandList, false /*logCmdOutput*/)
            .processErrors(
                String.format(
                    "Something went wrong trying to execute command: '%s'. \n",
                    String.join(" ", commandList)));
    return response.message;
  }

  /**
   * Generic implementation to get any kubectl resource output to a file. Example command: {@code
   * kubectl get pods -n <namespace> -o yaml}.
   *
   * @param config the environment variables to set (KUBECONFIG).
   * @param k8sResource the resource to get.
   * @param namespace the namespace in the cluster to run the command.
   * @param outputFormat the format of the kubectl command output like "yaml", "json".
   * @param localFilePath the local file path to store the output of command executed.
   * @return true if command executed and output saved successfully, else false.
   */
  @Override
  public String getK8sResource(
      Map<String, String> config, String k8sResource, String namespace, String outputFormat) {
    List<String> commandList = new ArrayList<String>(Arrays.asList("kubectl", "get", k8sResource));

    checkAndAddFlagToCommand(commandList, "-n", namespace);
    checkAndAddFlagToCommand(commandList, "-o", outputFormat);
    return execCommandProcessErrors(config, commandList);
  }

  /**
   * Gets the kubectl events output to a file. Retrieves custom columns sorted by the creation
   * timestamp.
   *
   * @param config the environment variables to set (KUBECONFIG, OVERRIDES, STORAGE_CLASS, etc.).
   * @param namespace the namespace in the cluster to run the command.
   * @param localFilePath the local file path to store the output of command executed.
   * @return true if command executed and output saved successfully, else false.
   */
  @Override
  public String getEvents(Map<String, String> config, String namespace, String localFilePath) {
    List<String> commandList = new ArrayList<String>(Arrays.asList("kubectl", "get", "events"));
    String outputFormat =
        "custom-columns=Namespace:.involvedObject.namespace,Object:.involvedObject.name,"
            + "First:firstTimestamp,Last:lastTimestamp,Reason:reason,Message:message";
    String sortByFormat = ".metadata.creationTimestamp";

    checkAndAddFlagToCommand(commandList, "-n", namespace);
    commandList.add("-o");
    commandList.add(outputFormat);
    commandList.add("--sort-by=" + sortByFormat);

    return execCommandProcessErrors(config, commandList);
  }

  /**
   * Gets the output of running {@code kubectl version} to a file.
   *
   * @param outputFormat the format of the kubectl command output like "yaml", "json".
   * @param localFilePath the local file path to store the output of command executed.
   * @return true if command executed and output saved successfully, else false.
   */
  @Override
  public String getK8sVersion(Map<String, String> config, String outputFormat) {
    List<String> commandList = new ArrayList<String>(Arrays.asList("kubectl", "version"));

    checkAndAddFlagToCommand(commandList, "-o", outputFormat);
    return execCommandProcessErrors(config, commandList);
  }

  /**
   * Veirfies whether a given namespace actually exists in the cluster or not.
   *
   * @param namespace the namespace to check.
   * @return true if the namespace exists, else false.
   */
  public boolean verifyNamespace(String namespace) {
    List<String> commandList =
        new ArrayList<String>(Arrays.asList("kubectl", "get", "ns", namespace));
    ShellResponse response = execCommand(null, commandList);
    return response.isSuccess();
  }

  /**
   * Retrieves the platform namespace by running {@code hostname -f} to get the FQDN. Splits the
   * FQDN output on "." to get the namespace at index 2.
   *
   * <pre>
   * Example FQDN = {@code "yb-yugaware-0.yb-yugaware.yb-platform.svc.cluster.local"}
   * Example namespace = {@code "yb-platform"}
   * </pre>
   *
   * @return the platform namespace.
   */
  @Override
  public String getPlatformNamespace() {
    List<String> commandList = new ArrayList<String>(Arrays.asList("hostname", "-f"));
    ShellResponse response = execCommand(null, commandList);
    String hostNameFqdn = response.message;
    String[] fqdnParts = hostNameFqdn.split(".");
    if (fqdnParts.length < 3) {
      log.debug(String.format("Output of 'hostname -f' is '%s'.", hostNameFqdn));
      return null;
    }
    String platformNamespace = fqdnParts[2];
    if (!verifyNamespace(platformNamespace)) {
      return null;
    }
    return platformNamespace;
  }

  /**
   * Executes the command {@code helm get values <release_name> -n <namespace> -o yaml} and stores
   * the output in a file.
   *
   * @param config the environment variables to set (KUBECONFIG, OVERRIDES, STORAGE_CLASS, etc.).
   * @param namespace the namespace in the cluster to run the command.
   * @param helmReleaseName the release name.
   * @param outputFormat the format of the kubectl command output like "yaml", "json".
   * @param localFilePath the local file path to store the output of command executed.
   * @return true if command executed and output saved successfully, else false.
   */
  @Override
  public String getHelmValues(
      Map<String, String> config, String namespace, String helmReleaseName, String outputFormat) {
    List<String> commandList =
        new ArrayList<String>(Arrays.asList("helm", "get", "values", helmReleaseName));

    checkAndAddFlagToCommand(commandList, "-n", namespace);
    checkAndAddFlagToCommand(commandList, "-o", outputFormat);
    return execCommandProcessErrors(config, commandList);
  }

  /**
   * Gets all the role data for the given service account name. Executes {@code kubectl get
   * rolebinding,clusterrolebinding --all-namespaces} and filters the output for the given service
   * account name.
   *
   * @param serviceAccountName the name of the service account.
   * @return List of all the role data. Contains {@code roleRef.kind}, {@code roleRef.name}, and
   *     {@code metadata.namespace}.
   */
  @Override
  public List<RoleData> getAllRoleDataForServiceAccountName(
      Map<String, String> config, String serviceAccountName) {
    List<RoleData> roleDataList = new ArrayList<RoleData>();
    String jsonPathFormat =
        "{range .items[?(@.subjects[0].name==\""
            + serviceAccountName
            + "\")]}"
            + "[\"{.roleRef.kind}\",\"{.roleRef.name}\",\"{.metadata.namespace}\"]{\"\\n\"}{end}";
    List<String> commandList =
        new ArrayList<String>(
            Arrays.asList(
                "kubectl",
                "get",
                "rolebinding,clusterrolebinding",
                "--all-namespaces",
                "-o",
                "jsonpath=" + jsonPathFormat));

    ShellResponse response = execCommand(config, commandList);
    for (String rawRoleData : response.message.split("\n")) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        List<String> parsedRoleData =
            mapper.readValue(rawRoleData, new TypeReference<List<String>>() {});
        if (parsedRoleData.size() >= 3) {
          RoleData roleData =
              new RoleData(parsedRoleData.get(0), parsedRoleData.get(1), parsedRoleData.get(2));
          roleDataList.add(roleData);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return roleDataList;
  }

  /**
   * Gets the permissions for a particular roleData and stores the output to a file.
   *
   * @param roleData contains {@code roleRef.kind}, {@code roleRef.name}, and {@code
   *     metadata.namespace}.
   * @param outputFormat the format of the kubectl command output like "yaml", "json"
   * @param localFilePath the local file path to store the output of command executed
   * @return true if command executed and output saved successfully, else false
   */
  @Override
  public String getServiceAccountPermissions(
      Map<String, String> config, RoleData roleData, String outputFormat) {
    List<String> commandList =
        new ArrayList<String>(Arrays.asList("kubectl", "get", roleData.kind, roleData.name));

    checkAndAddFlagToCommand(commandList, "-n", roleData.namespace);
    checkAndAddFlagToCommand(commandList, "-o", outputFormat);
    return execCommandProcessErrors(config, commandList);
  }

  /**
   * Returns the output of {@code kubectl get storageclass <storageClassName> -o <outputformat>} to
   * a given file.
   *
   * @param config the environment variables to set (KUBECONFIG, OVERRIDES, STORAGE_CLASS, etc.).
   * @param storageClassName the name of the storage class
   * @param namespace the namespace in the cluster to run the command.
   * @param outputFormat the format of the kubectl command output like "yaml", "json".
   * @param localFilePath the local file path to store the output of command executed.
   * @return
   */
  @Override
  public String getStorageClass(
      Map<String, String> config, String storageClassName, String namespace, String outputFormat) {
    List<String> commandList =
        new ArrayList<String>(
            Arrays.asList(
                "kubectl",
                "get",
                KubernetesResourceType.STORAGECLASS.toString().toLowerCase(),
                storageClassName));

    checkAndAddFlagToCommand(commandList, "-n", namespace);
    checkAndAddFlagToCommand(commandList, "-o", outputFormat);

    return execCommandProcessErrors(config, commandList);
  }
}

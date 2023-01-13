// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.models;

import static play.mvc.Http.Status.BAD_REQUEST;
import static play.mvc.Http.Status.INTERNAL_SERVER_ERROR;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.yugabyte.yw.common.PlatformServiceException;
import io.ebean.Finder;
import io.ebean.Model;
import io.ebean.Query;
import io.ebean.annotation.DbJson;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiModelProperty.AccessMode;
import java.io.File;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateUtils;
import play.data.validation.Constraints;

@Entity
@ApiModel(
    description =
        "Access key for the cloud provider. This helps to "
            + "authenticate the user and get access to the provider.")
public class AccessKey extends Model {
  public static class MigratedKeyInfoFields {
    // Below fields are moved to provider details
    @ApiModelProperty public String sshUser;
    @ApiModelProperty public Integer sshPort = 22;
    @ApiModelProperty public boolean airGapInstall = false;
    @ApiModelProperty public boolean passwordlessSudoAccess = true;
    @ApiModelProperty public String provisionInstanceScript = "";
    @ApiModelProperty public boolean installNodeExporter = true;
    @ApiModelProperty public Integer nodeExporterPort = 9300;
    @ApiModelProperty public String nodeExporterUser = "prometheus";
    @ApiModelProperty public boolean skipProvisioning = false;
    @ApiModelProperty public boolean setUpChrony = false;
    @ApiModelProperty public List<String> ntpServers = Collections.emptyList();;

    // Indicates whether the provider was created before or after PLAT-3009
    // True if it was created after, else it was created before.
    // Dictates whether or not to show the set up NTP option in the provider UI
    @ApiModelProperty public boolean showSetUpChrony = false;

    public void mergeFrom(MigratedKeyInfoFields keyInfo) {
      sshUser = keyInfo.sshUser;
      sshPort = keyInfo.sshPort;
      airGapInstall = keyInfo.airGapInstall;
      passwordlessSudoAccess = keyInfo.passwordlessSudoAccess;
      provisionInstanceScript = keyInfo.provisionInstanceScript;
      installNodeExporter = keyInfo.installNodeExporter;
      nodeExporterPort = keyInfo.nodeExporterPort;
      nodeExporterUser = keyInfo.nodeExporterUser;
      skipProvisioning = keyInfo.skipProvisioning;
      setUpChrony = keyInfo.setUpChrony;
      showSetUpChrony = keyInfo.showSetUpChrony;
      ntpServers = keyInfo.ntpServers;
    }
  }

  @ApiModel
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class KeyInfo extends MigratedKeyInfoFields {
    @ApiModelProperty public String publicKey;
    @ApiModelProperty public String privateKey;
    @ApiModelProperty public String vaultPasswordFile;
    @ApiModelProperty public String vaultFile;
    @ApiModelProperty public boolean deleteRemote = true;
  }

  public static String getDefaultKeyCode(Provider provider) {
    String sanitizedProviderName = provider.name.replaceAll("\\s+", "-").toLowerCase();
    return String.format(
        "yb-%s-%s_%s-key",
        Customer.get(provider.customerUUID).code, sanitizedProviderName, provider.uuid);
  }

  // scheduled access key rotation task uses this
  // since the granularity for that is days,
  // we can safely use a timestamp with second granularity
  public static String getNewKeyCode(Provider provider) {
    String sanitizedProviderName = provider.name.replaceAll("\\s+", "-").toLowerCase();
    String timestamp = generateKeyCodeTimestamp();
    return String.format(
        "yb-%s-%s-key-%s",
        Customer.get(provider.customerUUID).code, sanitizedProviderName, timestamp);
  }

  public static String generateKeyCodeTimestamp() {
    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    return sdf1.format(new Date());
  }

  @ApiModelProperty(required = false, hidden = true)
  @JsonIgnore
  public String getPublicKeyContent() {
    String pubKeyPath = this.getKeyInfo().publicKey;
    String publicKeyContent = "";
    try {
      File publicKeyFile = new File(pubKeyPath);
      publicKeyContent = FileUtils.readFileToString(publicKeyFile, Charset.defaultCharset());
    } catch (Exception e) {
      String msg = "Reading public key content from " + pubKeyPath + " failed!";
      throw new RuntimeException(msg, e);
    }
    return publicKeyContent;
  }

  @ApiModelProperty(required = true)
  @EmbeddedId
  @Constraints.Required
  public AccessKeyId idKey;

  @ApiModelProperty(required = false, hidden = true)
  @JsonIgnore
  public String getKeyCode() {
    return this.idKey.keyCode;
  }

  @ApiModelProperty(required = false, hidden = true)
  @JsonIgnore
  public UUID getProviderUUID() {
    return this.idKey.providerUUID;
  }

  @Constraints.Required
  @Column(nullable = false, columnDefinition = "TEXT")
  @ApiModelProperty(value = "Cloud provider key information", required = true)
  @DbJson
  private KeyInfo keyInfo;

  public void setKeyInfo(KeyInfo info) {
    this.keyInfo = info;
  }

  public KeyInfo getKeyInfo() {
    try {
      Provider provider = Provider.getOrBadRequest(getProviderUUID());
      if (provider.details != null) {
        keyInfo.mergeFrom(provider.details);
      } else {
        keyInfo.mergeFrom(new ProviderDetails());
      }
    } catch (PlatformServiceException e) {
      // Pass
    }
    return this.keyInfo;
  }

  // Post expiration, keys cannot be rotated into any universe and
  // will be unavailable for new universes as well
  @Column(nullable = true)
  @ApiModelProperty(
      value = "Expiration date of key",
      required = false,
      example = "2022-12-12T13:07:18Z",
      accessMode = AccessMode.READ_WRITE)
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
  @Getter
  public Date expirationDate;

  @JsonIgnore
  public void setExpirationDate(int expirationThresholdDays) {
    this.expirationDate = DateUtils.addDays(this.creationDate, expirationThresholdDays);
  }

  @JsonIgnore
  public void updateExpirationDate(int expirationThresholdDays) {
    this.setExpirationDate(expirationThresholdDays);
    this.save();
  }

  @Column(nullable = false)
  @ApiModelProperty(
      value = "Creation date of key",
      required = false,
      example = "2022-12-12T13:07:18Z",
      accessMode = AccessMode.READ_ONLY)
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
  @Getter
  public Date creationDate;

  public void setCreationDate() {
    this.creationDate = new Date();
  }

  public static AccessKey create(UUID providerUUID, String keyCode, KeyInfo keyInfo) {
    AccessKey accessKey = new AccessKey();
    accessKey.idKey = AccessKeyId.create(providerUUID, keyCode);
    accessKey.setKeyInfo(keyInfo);
    accessKey.setCreationDate();
    accessKey.save();
    return accessKey;
  }

  public void deleteOrThrow() {
    if (!super.delete()) {
      throw new PlatformServiceException(
          INTERNAL_SERVER_ERROR, "Delete unsuccessful for: " + this.idKey);
    }
  }

  private static final Finder<AccessKeyId, AccessKey> find =
      new Finder<AccessKeyId, AccessKey>(AccessKey.class) {};

  public static AccessKey getOrBadRequest(UUID providerUUID, String keyCode) {
    AccessKey accessKey = get(providerUUID, keyCode);
    if (accessKey == null) {
      throw new PlatformServiceException(BAD_REQUEST, "KeyCode not found: " + keyCode);
    }
    return accessKey;
  }

  @Deprecated
  public static AccessKey get(UUID providerUUID, String keyCode) {
    return find.byId(AccessKeyId.create(providerUUID, keyCode));
  }

  public static List<AccessKey> getAll(UUID providerUUID) {
    return find.query().where().eq("provider_uuid", providerUUID).findList();
  }

  public void mergeProviderDetails() {
    Provider provider = Provider.getOrBadRequest(getProviderUUID());
    if (provider.details != null) {
      keyInfo.mergeFrom(provider.details);
    } else {
      keyInfo.mergeFrom(new ProviderDetails());
    }
  }

  public static List<AccessKey> getByProviderUuids(List<UUID> providerUUIDs) {
    return find.query().where().in("provider_uuid", providerUUIDs).findList();
  }

  public static List<AccessKey> getAll() {
    return find.query().findList();
  }

  public static List<AccessKey> getAllActive(UUID providerUUID) {
    Date currentDate = new Date();
    return find.query()
        .where()
        .eq("provider_uuid", providerUUID)
        .gt("expiration_date", currentDate)
        .findList();
  }

  public static List<AccessKey> getAllExpired(UUID providerUUID) {
    Date currentDate = new Date();
    return find.query()
        .where()
        .eq("provider_uuid", providerUUID)
        .lt("expiration_date", currentDate)
        .findList();
  }

  // returns the most recently created access key
  // this can be used to set the params during creating another key
  public static AccessKey getLatestKey(UUID providerUUID) {
    return getLatestAccessKeyQuery(providerUUID).findOne();
  }

  public static Query<AccessKey> getLatestAccessKeyQuery(UUID providerUUID) {
    return find.query()
        .where()
        .eq("provider_uuid", providerUUID)
        .orderBy("creation_date DESC")
        .setMaxRows(1);
  }
}

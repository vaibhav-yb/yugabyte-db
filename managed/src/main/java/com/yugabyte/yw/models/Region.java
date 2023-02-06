// Copyright (c) Yugabyte, Inc.
package com.yugabyte.yw.models;

import static io.ebean.Ebean.beginTransaction;
import static io.ebean.Ebean.commitTransaction;
import static io.ebean.Ebean.endTransaction;
import static io.swagger.annotations.ApiModelProperty.AccessMode.READ_ONLY;
import static io.swagger.annotations.ApiModelProperty.AccessMode.READ_WRITE;
import static play.mvc.Http.Status.BAD_REQUEST;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.yugabyte.yw.cloud.PublicCloudConstants.Architecture;
import com.yugabyte.yw.commissioner.Common.CloudType;
import com.yugabyte.yw.common.PlatformServiceException;
import com.yugabyte.yw.models.helpers.CloudInfoInterface;
import com.yugabyte.yw.models.helpers.ProviderAndRegion;
import com.yugabyte.yw.models.helpers.provider.region.AWSRegionCloudInfo;
import com.yugabyte.yw.models.helpers.provider.region.AzureRegionCloudInfo;
import com.yugabyte.yw.models.helpers.provider.region.GCPRegionCloudInfo;

import io.ebean.Ebean;
import io.ebean.ExpressionList;
import io.ebean.Finder;
import io.ebean.Junction;
import io.ebean.Model;
import io.ebean.Query;
import io.ebean.RawSql;
import io.ebean.RawSqlBuilder;
import io.ebean.SqlUpdate;
import io.ebean.annotation.DbJson;
import io.ebean.annotation.Encrypted;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import org.apache.commons.collections.CollectionUtils;
import play.data.validation.Constraints;
import play.libs.Json;

@Entity
@JsonInclude(JsonInclude.Include.NON_NULL)
@ApiModel(
    description =
        "Region within a given provider. Typically, this maps to a "
            + "single cloud provider region.")
public class Region extends Model {

  @Id
  @ApiModelProperty(value = "Region UUID", accessMode = READ_ONLY)
  public UUID uuid;

  @Column(length = 25, nullable = false)
  @ApiModelProperty(
      value = "Cloud provider region code",
      example = "us-west-2",
      accessMode = READ_WRITE)
  public String code;

  @Column(length = 100, nullable = false)
  @ApiModelProperty(
      value = "Cloud provider region name",
      example = "US West (Oregon)",
      accessMode = READ_ONLY)
  public String name;

  @Deprecated
  @ApiModelProperty(
      hidden = true,
      value = "The AMI to be used in this region.",
      example = "TODO",
      accessMode = READ_WRITE)
  public String ybImage;

  @Column(columnDefinition = "float")
  @ApiModelProperty(value = "The region's longitude", example = "-120.01", accessMode = READ_ONLY)
  @Constraints.Min(-180)
  @Constraints.Max(180)
  public double longitude = -90;

  @Column(columnDefinition = "float")
  @ApiModelProperty(value = "The region's latitude", example = "37.22", accessMode = READ_ONLY)
  @Constraints.Min(-90)
  @Constraints.Max(90)
  public double latitude = -90;

  @Column(nullable = false)
  @ManyToOne
  @JsonBackReference("provider-regions")
  public Provider provider;

  @OneToMany(cascade = CascadeType.ALL)
  @JsonManagedReference("region-zones")
  public List<AvailabilityZone> zones;

  @ApiModelProperty(accessMode = READ_ONLY)
  @Column(nullable = false, columnDefinition = "boolean default true")
  private Boolean active = true;

  public boolean isActive() {
    return active == null || active;
  }

  @JsonIgnore
  public void setActiveFlag(Boolean active) {
    this.active = active;
  }

  @Encrypted
  @DbJson
  @Column(columnDefinition = "TEXT")
  @ApiModelProperty
  public RegionDetails details = new RegionDetails();

  @JsonIgnore
  public long getNodeCount() {
    Set<UUID> azUUIDs = zones.stream().map(az -> az.uuid).collect(Collectors.toSet());
    return Customer.get(provider.customerUUID)
        .getUniversesForProvider(provider.uuid)
        .stream()
        .flatMap(u -> u.getUniverseDetails().nodeDetailsSet.stream())
        .filter(nd -> azUUIDs.contains(nd.azUuid))
        .count();
  }

  public void setSecurityGroupId(String securityGroupId) {
    Provider p = this.provider;
    if (p.getCloudCode() == CloudType.aws) {
      AWSRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      regionCloudInfo.setSecurityGroupId(securityGroupId);
    } else if (p.getCloudCode() == CloudType.azu) {
      AzureRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      regionCloudInfo.setSecurityGroupId(securityGroupId);
    }
  }

  @JsonIgnore
  public String getSecurityGroupId() {
    Map<String, String> envVars = CloudInfoInterface.fetchEnvVars(this);
    String sgNode = "";
    if (envVars.containsKey("securityGroupId")) {
      sgNode = envVars.getOrDefault("securityGroupId", null);
    }
    return sgNode == null || sgNode.isEmpty() ? null : sgNode;
  }

  public void setVnetName(String vnetName) {
    Provider p = this.provider;
    if (p.getCloudCode() == CloudType.aws) {
      AWSRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      regionCloudInfo.setVnet(vnetName);
    } else if (p.getCloudCode() == CloudType.azu) {
      AzureRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      regionCloudInfo.setVnet(vnetName);
    }
  }

  @JsonIgnore
  public String getVnetName() {
    Map<String, String> envVars = CloudInfoInterface.fetchEnvVars(this);
    String vnetNode = "";
    if (envVars.containsKey("vnet")) {
      vnetNode = envVars.getOrDefault("vnet", null);
    }
    return vnetNode == null || vnetNode.isEmpty() ? null : vnetNode;
  }

  public void setArchitecture(Architecture arch) {
    Provider p = this.provider;
    if (p.getCloudCode() == CloudType.aws) {
      AWSRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      regionCloudInfo.setArch(arch);
    }
  }

  @JsonIgnore
  public Architecture getArchitecture() {
    Provider p = this.provider;
    if (p.getCloudCode() == CloudType.aws) {
      AWSRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      return regionCloudInfo.getArch();
    }
    return null;
  }

  @JsonIgnore
  public String getYbImage() {
    Map<String, String> envVars = CloudInfoInterface.fetchEnvVars(this);
    if (envVars.containsKey("ybImage")) {
      return envVars.getOrDefault("ybImage", null);
    }
    return null;
  }

  public void setYbImage(String ybImage) {
    Provider p = this.provider;
    if (p.getCloudCode().equals(CloudType.aws)) {
      AWSRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      regionCloudInfo.setYbImage(ybImage);
    } else if (p.getCloudCode().equals(CloudType.gcp)) {
      GCPRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      regionCloudInfo.setYbImage(ybImage);
    } else if (p.getCloudCode().equals(CloudType.azu)) {
      AzureRegionCloudInfo regionCloudInfo = CloudInfoInterface.get(this);
      regionCloudInfo.setYbImage(ybImage);
    }
  }

  @Deprecated
  @DbJson
  @Column(columnDefinition = "TEXT")
  public Map<String, String> config;

  @Deprecated
  @JsonProperty("config")
  public void setConfig(Map<String, String> configMap) {
    CloudInfoInterface.setCloudProviderInfoFromConfig(this, configMap);
  }

  @JsonProperty("details")
  public void setRegionDetails(RegionDetails regionDetails) {
    this.details = regionDetails;
  }

  @JsonProperty("details")
  public RegionDetails getMaskRegionDetails() {
    CloudInfoInterface.maskRegionDetails(this);
    return details;
  }

  @JsonIgnore
  public RegionDetails getRegionDetails() {
    if (details == null) {
      details = new RegionDetails();
    }
    return details;
  }

  /** Query Helper for PlacementRegion with region code */
  public static final Finder<UUID, Region> find = new Finder<UUID, Region>(Region.class) {};

  /**
   * Create new instance of PlacementRegion
   *
   * @param provider Cloud Provider
   * @param code Unique PlacementRegion Code
   * @param name User Friendly PlacementRegion Name
   * @param ybImage The YB image ID that we need to use for provisioning in this region
   * @return instance of PlacementRegion
   */
  public static Region create(Provider provider, String code, String name, String ybImage) {
    return create(provider, code, name, ybImage, 0.0, 0.0);
  }

  // Overload create function with lat, long values for OnPrem case
  public static Region create(
      Provider provider,
      String code,
      String name,
      String ybImage,
      double latitude,
      double longitude) {
    return create(provider, code, name, ybImage, latitude, longitude, new RegionDetails());
  }

  public static Region create(
      Provider provider,
      String code,
      String name,
      String ybImage,
      double latitude,
      double longitude,
      RegionDetails details) {
    Region region = new Region();
    region.provider = provider;
    region.code = code;
    region.name = name;
    region.setYbImage(ybImage);
    region.latitude = latitude;
    region.longitude = longitude;
    region.setRegionDetails(details);
    region.save();
    return region;
  }

  public static Region create(
      Provider provider,
      String code,
      String name,
      String ybImage,
      double latitude,
      double longitude,
      Map<String, String> config) {
    Region region = new Region();
    region.provider = provider;
    region.code = code;
    region.name = name;
    region.setYbImage(ybImage);
    region.latitude = latitude;
    region.longitude = longitude;
    region.setConfig(config);
    region.save();
    return region;
  }

  public static Region createWithMetadata(Provider provider, String code, JsonNode metadata) {
    Region region = Json.fromJson(metadata, Region.class);
    region.provider = provider;
    region.code = code;
    region.details = new RegionDetails();
    if (metadata.has("ybImage")) {
      region.setYbImage(metadata.get("ybImage").textValue());
    }
    region.save();
    return region;
  }

  /** DEPRECATED: use {@link #getOrBadRequest(UUID, UUID, UUID)} */
  @Deprecated()
  public static Region get(UUID regionUUID) {
    return find.query().fetch("provider").where().idEq(regionUUID).findOne();
  }

  public static List<Region> findByUuids(Collection<UUID> uuids) {
    return Region.find.query().where().idIn(uuids).findList();
  }

  public static Region getByCode(Provider provider, String code) {
    return find.query().where().eq("provider_UUID", provider.uuid).eq("code", code).findOne();
  }

  public static List<Region> getByProvider(UUID providerUUID) {
    return find.query().where().eq("provider_UUID", providerUUID).findList();
  }

  public static Region getOrBadRequest(UUID customerUUID, UUID providerUUID, UUID regionUUID) {
    Region region = get(customerUUID, providerUUID, regionUUID);
    if (region == null) {
      throw new PlatformServiceException(BAD_REQUEST, "Invalid Provider/Region UUID");
    }
    return region;
  }

  public static Region getOrBadRequest(UUID regionUUID) {
    Region region = get(regionUUID);
    if (region == null) {
      throw new PlatformServiceException(BAD_REQUEST, "Invalid Region UUID");
    }
    return region;
  }

  public static List<Region> findByKeys(Collection<ProviderAndRegion> keys) {
    if (CollectionUtils.isEmpty(keys)) {
      return Collections.emptyList();
    }
    Set<ProviderAndRegion> uniqueKeys = new HashSet<>(keys);
    ExpressionList<Region> query = find.query().where();
    Junction<Region> orExpr = query.or();
    for (ProviderAndRegion key : uniqueKeys) {
      Junction<Region> andExpr = orExpr.and();
      andExpr.eq("provider_UUID", key.getProviderUuid());
      andExpr.eq("code", key.getRegionCode());
      orExpr.endAnd();
    }
    return query.endOr().findList();
  }

  /** DEPRECATED: use {@link #getOrBadRequest(UUID, UUID, UUID)} */
  @Deprecated
  public static Region get(UUID customerUUID, UUID providerUUID, UUID regionUUID) {
    String regionQuery =
        " select r.uuid, r.code, r.name"
            + "   from region r join provider p on p.uuid = r.provider_uuid "
            + "  where r.uuid = :r_UUID and p.uuid = :p_UUID and p.customer_uuid = :c_UUID";

    RawSql rawSql = RawSqlBuilder.parse(regionQuery).create();
    Query<Region> query = Ebean.find(Region.class);
    query.setRawSql(rawSql);
    query.setParameter("r_UUID", regionUUID);
    query.setParameter("p_UUID", providerUUID);
    query.setParameter("c_UUID", customerUUID);
    return query.findOne();
  }

  /**
   * Fetch Regions with the minimum zone count and having a valid yb server image.
   *
   * @return List of PlacementRegion
   */
  public static List<Region> fetchValidRegions(
      UUID customerUUID, UUID providerUUID, int minZoneCount) {
    String regionQuery =
        " select r.uuid, r.code, r.name"
            + "   from region r join provider p on p.uuid = r.provider_uuid "
            + "   left outer join availability_zone zone on zone.region_uuid = r.uuid "
            + "  where p.uuid = :p_UUID and p.customer_uuid = :c_UUID"
            + "  group by r.uuid "
            + " having count(zone.uuid) >= "
            + minZoneCount;

    RawSql rawSql = RawSqlBuilder.parse(regionQuery).create();
    Query<Region> query = Ebean.find(Region.class);
    query.setRawSql(rawSql);
    query.setParameter("p_UUID", providerUUID);
    query.setParameter("c_UUID", customerUUID);
    return query.findList();
  }

  public void disableRegionAndZones() {
    beginTransaction();
    try {
      setActiveFlag(false);
      update();
      String s =
          "UPDATE availability_zone set active = :active_flag where region_uuid = :region_UUID";
      SqlUpdate updateStmt = Ebean.createSqlUpdate(s);
      updateStmt.setParameter("active_flag", false);
      updateStmt.setParameter("region_UUID", uuid);
      Ebean.execute(updateStmt);
      commitTransaction();
    } catch (Exception e) {
      throw new RuntimeException("Unable to flag Region UUID as deleted: " + uuid);
    } finally {
      endTransaction();
    }
  }

  public String toString() {
    return Json.newObject()
        .put("code", code)
        .put("provider", provider.uuid.toString())
        .put("name", name)
        .put("ybImage", ybImage)
        .put("latitude", latitude)
        .put("longitude", longitude)
        .toString();
  }
}

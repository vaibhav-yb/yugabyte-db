/*
 * Copyright 2022 YugaByte, Inc. and Contributors
 *
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

package com.yugabyte.yw.models.migrations;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.yugabyte.yw.models.AccessKey.MigratedKeyInfoFields;
import io.ebean.Finder;
import io.ebean.Model;
import io.ebean.annotation.DbJson;
import io.ebean.annotation.Encrypted;
import java.util.List;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import play.libs.Json;

/** Snapshot View of ORM entities at the time migration V230_5 was added. */
@Slf4j
public class V230_5 {

  @Entity
  @Table(name = "customer")
  public static class TmpCustomer extends Model {

    @Id public UUID uuid;

    public static final Finder<UUID, TmpCustomer> find =
        new Finder<UUID, TmpCustomer>(TmpCustomer.class) {};
  }

  @Entity
  @Table(name = "provider")
  public static class TmpProvider extends Model {

    @Id public UUID uuid;

    @Column(name = "customer_uuid", nullable = false)
    public UUID customerUUID;

    @Column(nullable = false, columnDefinition = "TEXT")
    @Encrypted
    @DbJson
    public TmpProviderDetails details = new TmpProviderDetails();

    public static final Finder<UUID, TmpProvider> find =
        new Finder<UUID, TmpProvider>(TmpProvider.class) {};

    public static List<TmpProvider> getAll(UUID customerUUID) {
      return find.query().where().eq("customer_uuid", customerUUID).findList();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @ToString
  public static class TmpAccessKeyDto {
    public final MigratedKeyInfoFields keyInfo;

    public TmpAccessKeyDto(JsonNode keyInfoJson) {
      log.debug("AccessKey.KeyInfo:\n" + keyInfoJson.toPrettyString());
      this.keyInfo = Json.fromJson(keyInfoJson, MigratedKeyInfoFields.class);
    }
  }

  public static class TmpProviderDetails extends MigratedKeyInfoFields {}
}

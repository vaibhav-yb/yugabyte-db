/*
 * Copyright 2022 YugaByte, Inc. and Contributors
 *
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * https://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

package com.yugabyte.yw.models;

import io.ebean.annotation.EnumValue;
import java.io.Serializable;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import lombok.Data;
import play.libs.Json;

@Embeddable
@Data
public class FileDataId implements Serializable {

  @Column(name = "file_path")
  public String filePath;

  @Column(name = "extension")
  public String fileExtension;

  public FileDataId(String filePath, String fileExtension) {
    this.filePath = filePath;
    this.fileExtension = fileExtension;
  }
}

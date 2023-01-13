// Copyright (c) Yugabyte, Inc.

package com.yugabyte.yw.forms;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import play.data.validation.Constraints;

/**
 * This class will be used by the API and UI Form Elements to validate constraints for
 * PlacementRegion Data
 */
public class AvailabilityZoneFormData {

  @Constraints.Required()
  @ApiModelProperty(value = "List of availability zones", required = true)
  public List<AvailabilityZoneData> availabilityZones;

  @ApiModel(description = "Model used to edit an availability zone")
  public static class AvailabilityZoneEditData {
    @ApiModelProperty(value = "AZ subnet")
    public String subnet;

    @ApiModelProperty(value = "AZ subnet")
    public String secondarySubnet;
  }

  @ApiModel(
      description =
          "Details of an availability zone, used by the API and UI to validate data against input constraints")
  public static class AvailabilityZoneData extends AvailabilityZoneEditData {
    @Constraints.Required()
    @ApiModelProperty(value = "AZ name", required = true)
    public String name;

    @Constraints.Required()
    @ApiModelProperty(value = "AZ code", required = true)
    public String code;
  }
}

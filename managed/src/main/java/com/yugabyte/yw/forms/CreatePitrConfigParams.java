package com.yugabyte.yw.forms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.UUID;
import lombok.NoArgsConstructor;
import org.yb.CommonTypes.TableType;
import play.data.validation.Constraints;

@ApiModel(description = "Create PITR config parameters")
@NoArgsConstructor
public class CreatePitrConfigParams extends UniverseTaskParams {

  @JsonIgnore public UUID universeUUID;

  @JsonIgnore public UUID customerUUID;

  @ApiModelProperty(value = "PITR config name")
  public String name;

  @JsonIgnore public String keyspaceName;

  @JsonIgnore public TableType tableType;

  @ApiModelProperty(value = "Retention period of a snapshot")
  @Constraints.Required
  public long retentionPeriodInSeconds;

  @ApiModelProperty(value = "Time interval between snapshots")
  @Constraints.Required
  public long intervalInSeconds = 86400L;
}

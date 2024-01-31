package com.yugabyte.troubleshoot.ts.models;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class GraphResponse {
  String name;
  boolean successful;
  String errorMessage;
  GraphLayout layout;
  List<GraphData> data = new ArrayList<>();

  @Data
  @Accessors(chain = true)
  public static class GraphData {
    public String name;
    public String instanceName;
    public String tableName;
    public String tableId;
    public String namespaceName;
    public String namespaceId;
    public String type;
    public List<Long> x = new ArrayList<>();
    public List<String> y = new ArrayList<>();
    public Map<String, String> labels;
  }
}

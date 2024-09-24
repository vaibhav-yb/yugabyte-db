// Copyright (c) YugaByte, Inc.

package com.yugabyte.yw.models.helpers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
/** YBA error for internal operations. */
public class YBAError {
  private Code code;
  private String message;

  @JsonCreator
  public YBAError(@JsonProperty("code") Code code, @JsonProperty("message") String message) {
    this.code = Objects.requireNonNull(code);
    this.message = Objects.requireNonNull(message);
  }

  /** Define all the task error codes here. */
  @JsonDeserialize(using = CodeDeserializer.class)
  public static enum Code {
    UNKNOWN_ERROR,
    INTERNAL_ERROR,
    PLATFORM_SHUTDOWN,
    PLATFORM_RESTARTED,
    INSTALLATION_ERROR,
    SERVICE_START_ERROR,
    CONNECTION_ERROR;

    @JsonValue
    public String serialize() {
      return name();
    }
  }

  @SuppressWarnings("serial")
  /** Deserializer does not fail if there are unknown enum constants. */
  public static class CodeDeserializer extends StdDeserializer<Code> {

    public CodeDeserializer() {
      super(Code.class);
    }

    @Override
    public Code deserialize(JsonParser jsonParser, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      JsonNode node = jsonParser.getCodec().readTree(jsonParser);
      for (Code code : Code.values()) {
        if (code.name().equalsIgnoreCase(node.asText())) {
          return code;
        }
      }
      return Code.UNKNOWN_ERROR;
    }
  }
}

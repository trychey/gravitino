package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.Map;

@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class UserCreateRequest implements RESTRequest {

    @JsonProperty("name")
    private final String name;

    @Nullable
    @JsonProperty("properties")
    private final Map<String, String> properties;

    /** Default constructor for MetalakeCreateRequest. (Used for Jackson deserialization.) */
    public UserCreateRequest() {
        this(null, null);
    }

    public UserCreateRequest(String name, Map<String, String> properties) {
        super();
        this.name = name;
        this.properties = properties;
    }

    /**
     * Validates the {@link UserCreateRequest} request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
        Preconditions.checkArgument(
                StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");

    }
}

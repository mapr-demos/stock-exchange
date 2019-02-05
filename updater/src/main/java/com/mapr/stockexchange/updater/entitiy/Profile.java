package com.mapr.stockexchange.updater.entitiy;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.List;

@Value
public class Profile {
    private String name;
    private String fullName;

    @JsonIgnore
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private List<Stock> stocks;

}

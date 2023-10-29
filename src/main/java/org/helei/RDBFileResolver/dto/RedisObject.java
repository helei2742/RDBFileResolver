package org.helei.RDBFileResolver.dto;

import lombok.Data;

@Data
public class RedisObject {
    private String key;
    private String value;
    private String type;
}

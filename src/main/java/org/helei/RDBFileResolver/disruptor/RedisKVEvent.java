package org.helei.RDBFileResolver.disruptor;

import lombok.Data;
import net.whitbeck.rdbparser.Entry;
import net.whitbeck.rdbparser.KeyValuePair;

@Data
public class RedisKVEvent {
    private Entry entry;
    private String dbName;
    private KeyValuePair keyValuePair;
}

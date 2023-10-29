package org.helei.RDBFileResolver.disruptor;

import net.whitbeck.rdbparser.Entry;

public class RedisKVHelper {
    private static final RedisKVDisruptorClient client = new RedisKVDisruptorClient(10);

    public static void publish(Entry entry) {
        client.getEventProducer().onData(entry);
    }

    public static void shutdown() {
        client.shutdown();
    }
}

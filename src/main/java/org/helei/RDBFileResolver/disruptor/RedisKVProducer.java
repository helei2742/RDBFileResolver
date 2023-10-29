package org.helei.RDBFileResolver.disruptor;

import com.lmax.disruptor.RingBuffer;
import net.whitbeck.rdbparser.Entry;

public class RedisKVProducer {
    private final RingBuffer<RedisKVEvent> ringBuffer;

    public RedisKVProducer(RingBuffer<RedisKVEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(Entry entry) {
        long sequence = ringBuffer.next();
        try {
            RedisKVEvent redisKVEvent = ringBuffer.get(sequence);
            redisKVEvent.setEntry(entry);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}

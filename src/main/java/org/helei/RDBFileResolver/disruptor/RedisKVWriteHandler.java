package org.helei.RDBFileResolver.disruptor;

import com.lmax.disruptor.WorkHandler;

import net.whitbeck.rdbparser.KeyValuePair;

import javax.annotation.PreDestroy;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class RedisKVWriteHandler implements WorkHandler<RedisKVEvent> {
    private static final Map<String, BufferedWriter> streamMap = new HashMap<>();

    @Override
    public void onEvent(RedisKVEvent event) throws Exception {
        if(event.getKeyValuePair() == null) return;
        String name = event.getDbName();

        KeyValuePair kvp = event.getKeyValuePair();


        StringBuilder sb = new StringBuilder();
        for (byte[] val : kvp.getValues()) {
            sb.append(new String(val, StandardCharsets.US_ASCII)).append(",");
        }
        String line = String.format("%-20s %-20s %-20s %s",
                new String(kvp.getKey(), StandardCharsets.US_ASCII),
                kvp.getValueType(),
                kvp.getExpireTime(), sb.toString());
        System.out.println("write line [" + line + "]");

        try {

            if (streamMap.get(name) == null) {
                synchronized (name.intern()) {
                    if (streamMap.get(name) == null) {
                        BufferedWriter bw = new BufferedWriter(new FileWriter(String.format("%s.txt", name)));
                        bw.write(String.format("%-20s %-20s %-20s %s", "key", "type", "expire", "value"));
                        bw.newLine();
                        streamMap.put(name, bw);
                    }
                }
            }
            BufferedWriter bw = streamMap.get(name);

            bw.write(line);
            bw.newLine();
            bw.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void preDestroy() {
        if(streamMap.size() > 0){
            synchronized (streamMap) {
                if(streamMap.size() > 0) {
                    streamMap.forEach((k, v) -> {
                        try {
                            if (v != null)
                                v.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    streamMap.clear();
                }
                System.out.println("clear map down");
            }
        }
    }
}

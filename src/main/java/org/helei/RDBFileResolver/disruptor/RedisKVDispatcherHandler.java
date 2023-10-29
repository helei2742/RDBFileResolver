package org.helei.RDBFileResolver.disruptor;

import com.lmax.disruptor.EventHandler;
import net.whitbeck.rdbparser.Entry;
import net.whitbeck.rdbparser.Eof;
import net.whitbeck.rdbparser.KeyValuePair;
import net.whitbeck.rdbparser.SelectDb;


public class RedisKVDispatcherHandler implements EventHandler<RedisKVEvent> {

    private String dbName;

    @Override
    public void onEvent(RedisKVEvent redisKVEvent, long l, boolean b) throws Exception {
        Entry e = redisKVEvent.getEntry();
        switch (e.getType()) {
            case SELECT_DB:
                System.out.println(l+" dispatcher handler -- Processing DB: " + ((SelectDb)e).getId());
                dbName = String.valueOf(((SelectDb) e).getId());
                redisKVEvent.setDbName("select db "+dbName);
                break;
            case EOF:
                System.out.print(l+" dispatcher handler -- End of file. Checksum: ");
                for (byte bt : ((Eof)e).getChecksum()) {
                    System.out.print(String.format("%02x", bt & 0xff));
                }
                System.out.println("--------------------------------------------");
                break;
            case KEY_VALUE_PAIR:
                System.out.println(l+" dispatcher handler -- dispatch to " + dbName);
                redisKVEvent.setDbName(dbName);
                redisKVEvent.setKeyValuePair((KeyValuePair)e);
                break;
        }
    }
}

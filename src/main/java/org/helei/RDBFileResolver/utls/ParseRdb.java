package org.helei.RDBFileResolver.utls;

import net.whitbeck.rdbparser.*;
import org.helei.RDBFileResolver.disruptor.RedisKVHelper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ParseRdb implements Runnable{
    private File file;

    /**
     * 判断rdb文件是否存在
     * @param f
     */
    public ParseRdb(File f){
        this.file = f;
        if(file.exists()){
            System.out.println("exists");
        }else{
            System.out.println("not exists");
        }
    }
    @Override
    public void run() {
        try{
            RdbParser parser = new RdbParser(file);
            Entry e;
            while ((e = parser.readNext()) != null) {
                RedisKVHelper.publish(e);
            }
            TimeUnit.SECONDS.sleep(5);
            RedisKVHelper.shutdown();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

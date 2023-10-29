package org.helei.RDBFileResolver;

import org.helei.RDBFileResolver.utls.ParseRdb;

import java.io.File;

public class RDBFileResolver {

    public static void main(String[] args) {
        File rebPath = new File("/Users/helei/develop/ideaworkspace/RDFileResolver/dump.rdb");
        new Thread(new ParseRdb(rebPath)).start();
    }
}

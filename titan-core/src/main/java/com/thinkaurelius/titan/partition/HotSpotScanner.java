package com.thinkaurelius.titan.partition;


import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

/**
 * Created by whshev on 15/3/10.
 */
public abstract class HotSpotScanner {

    protected static final String DEFAULT_TABLE_NAME = "titan";
    protected static final String[] DEFAULT_HOST = {"127.0.0.1"};
    protected static final String DEFAULT_HOT_TABLE = "hotspot";
    protected static final String DEFAULT_HOT_CF = "c";

    protected String tableName;
    protected String[] hostName;
    protected String hotTableName;
    protected String hotcf;
    protected IDManager idManager;

    public HotSpotScanner() {
        this.tableName = DEFAULT_TABLE_NAME;
        this.hostName = DEFAULT_HOST;
        this.hotcf = DEFAULT_HOT_CF;
        this.hotTableName = DEFAULT_HOT_TABLE;
    }

    public HotSpotScanner(String[] hostName, String tableName, IDManager idManager) {
        this.hostName = hostName;
        this.tableName = tableName;
        this.idManager = idManager;
        this.hotcf = DEFAULT_HOT_CF;
        this.hotTableName = DEFAULT_HOT_TABLE;
    }

    public boolean isHotSpot(long vid) {
        return false;
    }

    public long getCanonicalHotSpotId(long hotSpotId) {
        return hotSpotId;
    }

    public void close() {
        return;
    }

}

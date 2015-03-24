package com.thinkaurelius.titan.partition;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.diskstorage.util.ReadArrayBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.LongSerializer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Created by whshev on 15/3/10.
 */
public class HBaseHotSpotScanner extends HotSpotScanner{

    private HotSpotThread hotSpotThread;
    private Configuration hbaseConf;
    private ConcurrentMap<Long, Long> hotSpotMap;

    public HBaseHotSpotScanner() {
        super();
        this.initialize();
    }

    public HBaseHotSpotScanner(String[] hostName, String tableName, IDManager idManager) {
        super(hostName, tableName, idManager);
        this.initialize();
    }

    private void initialize() {
        this.hbaseConf = HBaseConfiguration.create();
        this.hbaseConf.set("hbase.zookeeper.quorum", Joiner.on(",").join(this.hostName));
        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(hbaseConf);
            if (!hBaseAdmin.tableExists(hotTableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(hotTableName));
                tableDescriptor.addFamily(new HColumnDescriptor(hotcf));
                hBaseAdmin.createTable(tableDescriptor);
//                System.out.println("Created " + hotTableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (hBaseAdmin != null) {
                    hBaseAdmin.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        hotSpotMap = new ConcurrentHashMap();
        this.hotSpotThread = new HotSpotThread();
        this.hotSpotThread.start();
    }

    @Override
    public boolean isHotSpot(long vid) {
        if (IDManager.VertexIDType.NormalVertex.is(vid)) {
            return this.hotSpotMap.containsKey(idManager.getCount(vid));
        }
        return false;
    }

    @Override
    public long getCanonicalHotSpotId(long hotSpotId) {
        if (isHotSpot(hotSpotId)) {
            return this.hotSpotMap.get(idManager.getCount(hotSpotId));
        }
        return hotSpotId;
    }

    @Override
    public void close() {
        while (hotSpotThread.isAlive()) {
            this.hotSpotThread.interrupt();
        }
    }

    protected class HotSpotThread extends Thread {
        @Override
        public void run() {
            int retrycnt = 0;
            while (true) {
                HConnection hcx = null;
                ResultScanner hotScanner = null;
                HTableInterface hottable = null;
                try {
                    hcx = HConnectionManager.createConnection(hbaseConf);
                    hottable = hcx.getTable(hotTableName);
                    Scan hotscan = new Scan();
                    hotscan.addFamily(hotcf.getBytes());
                    byte[] tablebytes = tableName.getBytes();
                    hotscan.setStartRow(tablebytes);
                    hotscan.setStopRow(Arrays.copyOf(tablebytes, tablebytes.length + 1));
                    hotscan.setBatch(100000);
                    hotScanner = hottable.getScanner(hotscan);
                    for (Result rs : hotScanner) {
                        for (Cell kv : rs.rawCells()) {
                            long vidcnt = LongSerializer.INSTANCE.read(new ReadArrayBuffer(CellUtil.cloneQualifier(kv)));
                            long vidcan = LongSerializer.INSTANCE.read(new ReadArrayBuffer(CellUtil.cloneValue(kv)));
                            hotSpotMap.putIfAbsent(vidcnt, vidcan);
//                        System.out.println(vidcnt + "   " + vidcan);
                        }
                    }
                    Thread.sleep(10000);
                } catch (IOException e) {
                    ++retrycnt;
                    e.printStackTrace();
                    try {
                        if (hotScanner != null) {
                            hotScanner.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    try {
                        if (hottable != null) {
                            hottable.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    try {
                        if (hcx != null) {
                            hcx.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    if (retrycnt > 100) {
                        break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    try {
                        if (hotScanner != null) {
                            hotScanner.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    try {
                        if (hottable != null) {
                            hottable.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    try {
                        if (hcx != null) {
                            hcx.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    break;
                } finally {
                    try {
                        if (hotScanner != null) {
                            hotScanner.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    try {
                        if (hottable != null) {
                            hottable.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    try {
                        if (hcx != null) {
                            hcx.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }


}

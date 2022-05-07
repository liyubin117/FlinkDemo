package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 1.Put List Size
 * HBase的Put支持单条插入，也支持批量插入。
 * 2. AutoFlush
 * AutoFlush指的是在每次调用HBase的Put操作，是否提交到HBase Server。 默认是true,每次会提交。如果此时是单条插入，就会有更多的IO,从而降低性能
 * 3.Write Buffer Size
 * Write Buffer Size在AutoFlush为false的时候起作用，默认是2MB,也就是当插入数据超过2MB,就会自动提交到Server
 * 4.WAL
 * WAL是Write Ahead Log的缩写，指的是HBase在插入操作前是否写Log。默认是打开，关掉会提高性能，但是如果系统出现故障(负责插入的Region Server挂掉)，数据可能会丢失。
 */
public class HBaseBatchInsert {
    private static HBaseConfiguration hbaseConfig;
  
    public static void main(String[] args) throws Exception {  
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "fuxi-luoge-76");
        hbaseConfig = new HBaseConfiguration(HBASE_CONFIG);  
        insert(false,false,1024*1024*10);
//        insert(false,true,0);
//        insert(true,true,0);
    }  
  
    private static void insert(boolean wal,boolean autoFlush,long writeBuffer)  
            throws IOException {
        String tableName="lyb_test";
        HBaseAdmin hAdmin = new HBaseAdmin(hbaseConfig);
        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);
            hAdmin.truncateTable(TableName.valueOf(tableName),false);
            System.out.println("table truncated");
        }else {
            HTableDescriptor t = new HTableDescriptor(tableName);
            t.addFamily(new HColumnDescriptor("f1"));
            t.addFamily(new HColumnDescriptor("f2"));
            t.addFamily(new HColumnDescriptor("f3"));
            t.addFamily(new HColumnDescriptor("f4"));
            hAdmin.createTable(t);
            System.out.println("table created");
        }
        HTable table = new HTable(hbaseConfig, tableName);
        table.setAutoFlushTo(autoFlush);
        if(writeBuffer!=0){  
            table.setWriteBufferSize(writeBuffer);  
        }  
        List<Put> lp = new ArrayList<>();
        long all = System.currentTimeMillis();  
        int count = 500;
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        Random r = new Random();
        for (int i = 1; i <= count; ++i) {  
            Put p = new Put(String.format("row %d",i).getBytes());
            p.add("f1".getBytes(), null, sdf.format(new Date()).getBytes());
            p.add("f2".getBytes(), null, "v2".getBytes());
            p.add("f3".getBytes(), null, "v3".getBytes());
            p.add("f4".getBytes(), null, "v4".getBytes());
            if(!wal){
                p.setDurability(Durability.SKIP_WAL); //不使用wal
            }else{
                p.setDurability(Durability.ASYNC_WAL); //USER_DEFAULT是同步写入，ASYNC_WAL是异步写入
            }
            lp.add(p);
            //每100条则提交一次并清空list
            if(i%100==0){
                System.out.println("-------");
                table.put(lp);  
                lp.clear();  
            }  
        }
        //必须有close，否则未达到buffersize大小会直接丢弃而不提交
        table.close();
        System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer);  
        System.out.println("insert complete"+",costs:"+(System.currentTimeMillis()-all)*1.0/count+"ms");  
    }  
}  
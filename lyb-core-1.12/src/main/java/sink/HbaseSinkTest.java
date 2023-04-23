package sink;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;

public class HbaseSinkTest {

    private static String hbaseZookeeperQuorum = "my";
    private static String hbaseZookeeperClientPort = "2188";
    private static TableName tableName = TableName.valueOf("tflink");
    private static final String columnFamily = "cf1";

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> socketStream = env.socketTextStream("my",9888);

        socketStream.map(new MapFunction<String, Object>() {
           public String map(String value)throws IOException {
               System.out.println("map开始");
               writeIntoHBase(value);
               return value;
           }

        }).print();
        //transction.writeAsText("/home/admin/log2");
        // transction.addSink(new HBaseOutputFormat();
        try {
            env.execute();
        } catch (Exception ex) {
            Logger.getLogger(HbaseSinkTest.class.getName()).error(ex);
            ex.printStackTrace();
        }
    }

    public static void writeIntoHBase(String m)throws IOException
    {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        config.set("hbase.master", "my:60000");
        config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClientPort);
        config.setInt("hbase.rpc.timeout", 20000);
        config.setInt("hbase.client.operation.timeout", 30000);
        config.setInt("hbase.client.scanner.timeout.period", 200000);

        //config.set(TableOutputFormat.OUTPUT_TABLE, hbasetable);

        Connection c = ConnectionFactory.createConnection(config);

        Admin admin = c.getAdmin();
        if(!admin.tableExists(tableName)){
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily)));
        }
        Table t = c.getTable(tableName);

        TimeStamp ts = new TimeStamp(new Date());

        Date date = ts.getDate();

        Put put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(date.toString()));

        put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes("f1"), org.apache.hadoop.hbase.util.Bytes.toBytes("c1"),org.apache.hadoop.hbase.util.Bytes.toBytes(m));
        put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes("f2"), org.apache.hadoop.hbase.util.Bytes.toBytes("c2"),org.apache.hadoop.hbase.util.Bytes.toBytes(m));

        t.put(put);

        t.close();
        c.close();
    }
}
package sink;

import java.io.IOException;
import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HBaseSink<T> extends RichSinkFunction<T> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private Connection conn;
    private String tableName;
    private Properties p;

    public HBaseSink(String tableName, Properties p) {
        this.tableName = tableName;
        this.p = p;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getConnection(p);
        log.info("Flink-hbase open connection");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeResource(conn.getTable(TableName.valueOf(tableName)));
        super.close();
        log.info("Flink-hbase close connection");
    }

    public Table getTable() throws IOException {
        return conn.getTable(TableName.valueOf(tableName));
    }

    public Logger getLogger() {
        return this.log;
    }

    private static class HBaseUtil {
        private static Connection conn;

        static Connection getConnection(Properties p) {
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            for (Object key : p.keySet()) {
                config.set(key.toString(), p.getProperty(key.toString()));
            }
            try {
                conn = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return conn;
        }

        static void closeResource(Table table) {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 等待垃圾回收
            table = null;
            if (conn != null) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 等待垃圾回收
            conn = null;
        }
    }
}

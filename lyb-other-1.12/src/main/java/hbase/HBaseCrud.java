package hbase;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** http://hbase.apache.org/1.2/apidocs/index.html */
public class HBaseCrud {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private Connection connection;
    private TableName tableName;

    public static class HBaseConnectionUtils {

        public static Connection getConnection() {
            Connection connection = null;
            try {
                connection = ConnectionFactory.createConnection(getConfiguration());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return connection;
        }

        private static Configuration getConfiguration() {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.property.clientPort", "2188");
            config.set("hbase.zookeeper.quorum", "my");
            return config;
        }
    }

    @Before
    public void init() {
        connection = HBaseConnectionUtils.getConnection();
        tableName = TableName.valueOf("tableByJava");
    }

    @After
    public void close() {
        try {
            connection.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    @Test
    public void testCreate() {
        // 创建HBase表
        createTable(connection, tableName, "f1", "f2");
    }

    @Test
    public void testPut() {
        // put
        String rowKey = "u12000";
        put(connection, tableName, rowKey, "f1", "name", "ricky");
        put(connection, tableName, rowKey, "f1", "password", "root");
        put(connection, tableName, rowKey, "f2", "age", "28");
    }

    @Test
    public void testGet() {
        // get
        String rowKey = "u12000";
        get(connection, tableName, rowKey);
    }

    @Test
    public void testScan() {
        // scan
        scan(connection, tableName);
    }

    @Test
    public void testDelete() {
        // delete
        deleteTable(connection, tableName);
    }

    public void scan(Connection connection, TableName tableName) {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            ResultScanner rs = null;
            try {
                // Scan scan = new Scan(Bytes.toBytes("u120000"), Bytes.toBytes("u200000"));
                rs = table.getScanner(new Scan());
                for (Result r : rs) {
                    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
                            navigableMap = r.getMap();
                    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry :
                            navigableMap.entrySet()) {
                        logger.info(
                                "row:{} key:{}",
                                Bytes.toString(r.getRow()),
                                Bytes.toString(entry.getKey()));
                        NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
                        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> en : map.entrySet()) {
                            System.out.print(Bytes.toString(en.getKey()) + "##");
                            NavigableMap<Long, byte[]> ma = en.getValue();
                            for (Map.Entry<Long, byte[]> e : ma.entrySet()) {
                                System.out.print(e.getKey() + "###");
                                System.out.println(Bytes.toString(e.getValue()));
                            }
                        }
                    }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 根据row key获取表中的该行数据
    public void get(Connection connection, TableName tableName, String rowKey) {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap =
                    result.getMap();
            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry :
                    navigableMap.entrySet()) {

                logger.info("columnFamily:{}", Bytes.toString(entry.getKey()));
                NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> en : map.entrySet()) {
                    System.out.print(Bytes.toString(en.getKey()) + "##");
                    NavigableMap<Long, byte[]> nm = en.getValue();
                    for (Map.Entry<Long, byte[]> me : nm.entrySet()) {
                        logger.info("column key:{}, value:{}", me.getKey(), me.getValue());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /** 批量插入可以使用 Table.put(List<Put> list)* */
    public void put(
            Connection connection,
            TableName tableName,
            String rowKey,
            String columnFamily,
            String column,
            String data) {

        Table table = null;
        try {
            table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void createTable(Connection connection, TableName tableName, String... columnFamilies) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                logger.warn("table:{} exists!", tableName.getName());
            } else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for (String columnFamily : columnFamilies) {
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                }
                admin.createTable(tableDescriptor);
                logger.info("create table:{} success!", tableName.getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 删除表中的数据
    public void deleteTable(Connection connection, TableName tableName) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                // 必须先disable, 再delete
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void disableTable(Connection connection, TableName tableName) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }
}

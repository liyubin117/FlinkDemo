package org.lyb;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkOfficialTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
//        tableEnvironment.executeSql(
//                "create table t2(id int comment 'hello') comment 'c123 ''c12''' with ('connector'='print')");
//        tableEnvironment.executeSql("insert into t2 select 1 as id");
//        TableResult result = tableEnvironment.executeSql("desc t2");
//        for (CloseableIterator<Row> it = result.collect(); it.hasNext(); ) {
//            Row row = it.next();
//            System.out.println(row);
//        }
//        tableEnvironment.executeSql("show create table t2").print();
        tableEnvironment.executeSql("CREATE TABLE kafka (\n"
                + "  `id` int,\n"
                + "  `name` string,\n"
                + "  `time1` time(9)\n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'test_topic123456',\n"
                + "  'properties.bootstrap.servers' = 'datassert-kafka1.dg.163.org:9092,datassert-kafka2.dg.163.org:9092',\n"
                + "  'properties.group.id' = 'a',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'format' = 'json'\n"
                + ")");
        tableEnvironment.executeSql("create table t1 (id int,name string, time1 time(9)) with ('connector' = 'print')");
        tableEnvironment.executeSql("create table t2 (id int,name string, time1 time(9)) with ('connector' = 'datagen', 'rows-per-second' = '1')");
//        tableEnvironment.executeSql("insert into t1 select * from t2");
        tableEnvironment.executeSql("insert into kafka select * from t2");
        tableEnvironment.executeSql("insert into kafka select 2 as id, 'a' as name, TIME '12:12:12.123456' as time1");
//        tableEnvironment.executeSql("show columns in orders3 like 'p%'").print();
    }
}

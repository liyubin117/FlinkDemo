/**
 {"userid":123, "item_id":200, "name":"testname"}
 CREATE TABLE kafka (
 `userid` BIGINT,
 `item_id` BIGINT,
 `name` STRING,
 `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
 ) WITH (
 'connector' = 'kafka',
 'topic' = 'test',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup2',
 'scan.startup.mode' = 'latest-offset',
 'format' = 'json'
 )
 create table fs_parquet
 (userid bigint, name string)
 with(
 'connector' = 'filesystem',
 'path' = '/home/rick/data/fs_parquet',
 'format' = 'parquet',
 'sink.rolling-policy.file-size' = '5KB',
 'sink.rolling-policy.rollover-interval' = '20s',
 'sink.rolling-policy.check-interval' = '5s'
 )
 */
public class Kafka2File {
    public static void main(String[] args) {
        String kafka = "CREATE TABLE kafka (\n" +
                "  `userid` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `name` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup2',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
        Env.tableEnv.executeSql(kafka);
        Env.tableEnv.from("kafka").printSchema();
        String file = "create table fs_parquet\n" +
                "(userid bigint, name string)\n" +
                "with(\n" +
                "'connector' = 'filesystem',\n" +
                "'path' = '/home/rick/data/fs_parquet',\n" +
                "'format' = 'parquet',\n" +
                "'sink.rolling-policy.file-size' = '5KB',\n" +
                "'sink.rolling-policy.rollover-interval' = '20s',\n" +
                "'sink.rolling-policy.check-interval' = '5s'\n" +
                ")";
        String dml = "insert into fs_parquet select userid, name from kafka";
        Env.tableEnv.executeSql(file);
        Env.tableEnv.executeSql(dml);
    }
}

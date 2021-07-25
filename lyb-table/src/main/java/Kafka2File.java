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
                "'sink.rolling-policy.file-size' = '500b',\n" +
                "'sink.rolling-policy.rollover-interval' = '800s',\n" +
                "'sink.rolling-policy.check-interval' = '60s'\n" +
                ")";
        String dml = "insert into fs_parquet select userid, name from kafka";
        Env.tableEnv.executeSql(file);
        Env.tableEnv.executeSql(dml);
    }
}

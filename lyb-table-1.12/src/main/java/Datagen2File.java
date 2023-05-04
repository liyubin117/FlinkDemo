public class Datagen2File {
    public static void main(String[] args) {
        String datagen =
                "CREATE TABLE datagen (\n"
                        + " name STRING , \n"
                        + " userid INT \n"
                        + ") WITH (\n"
                        + " 'connector' = 'datagen' ,\n"
                        + " 'fields.name.length'='10'\n"
                        + ")";
        ;
        Env.tableEnv.executeSql(datagen);
        Env.tableEnv.from("datagen").printSchema();
        String file =
                "create table fs_parquet\n"
                        + "(userid bigint, name string)\n"
                        + "with(\n"
                        + "'connector' = 'filesystem',\n"
                        + "'path' = 'file:///home/rick/data/fs_parquet2',\n"
                        + "'format' = 'parquet',\n"
                        + "'parquet.compression' = 'snappy',\n"
                        + "'auto-compaction' = 'true',\n"
                        + "'compaction.file-size' = '3000b',\n"
                        + "'sink.rolling-policy.file-size' = '500b',\n"
                        + "'sink.rolling-policy.rollover-interval' = '800s',\n"
                        + "'sink.rolling-policy.check-interval' = '60s'\n"
                        + ")";
        //        String dml = "insert into fs_parquet select userid, name from datagen";
        String dml = "insert into fs_parquet values (1,'test')";

        Env.tableEnv.executeSql(file);
        Env.tableEnv.executeSql(dml);
    }
}

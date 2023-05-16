public class Datagen2File {
    public static void main(String[] args) {
        Env.tableEnv.executeSql("create database `T$#.1`");
        String datagen =
                "CREATE TABLE default_catalog.`T$#.1`.datagen (\n"
                        + " userid BIGINT, \n"
                        + " name STRING \n"
                        + ") WITH (\n"
                        + " 'connector' = 'datagen' ,\n"
                        + " 'fields.name.length'='10',\n"
                        + " 'number-of-rows'='10'\n"
                        + ")";
        ;
        Env.tableEnv.executeSql(datagen);
        Env.tableEnv.from("default_catalog.`T$#.1`.datagen").printSchema();
        String file =
                "create table `T$#.1`.fs_parquet\n"
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
        String dml =
                "insert into default_catalog.`T$#.1`.fs_parquet select userid, name from default_catalog.`T$#.1`.datagen";
        //        String dml = "insert into default_catalog.`T$#.1`.fs_parquet values (1,'test')";

        Env.tableEnv.executeSql(file);
        Env.tableEnv.executeSql(dml);
    }
}

/*
create table test.flink_test (name varchar(200), userid int);
 */

public class Datagen2Jdbc {
    public static void main(String[] args) {
        String datagen = "CREATE TABLE datagen (\n" +
                " name STRING , \n" +
                " userid INT \n" +
                ") WITH (\n" +
                " 'connector' = 'datagen' ,\n" +
                " 'fields.name.length'='10',\n" +
                "'number-of-rows' = '10'" +
                ")";;
        Env.tableEnv.executeSql(datagen);
        Env.tableEnv.from("datagen").printSchema();

        String jdbc = "create table jdbc (name string, userid int) " +
                "with ('connector'='jdbc','url' = 'jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai','table-name'='flink_test','username'='root','password'='passwd123')";
//        String dml = "insert into fs_parquet select userid, name from datagen";
        String dml = "insert into jdbc select name, userid from datagen";

        Env.tableEnv.executeSql(jdbc);
        Env.tableEnv.executeSql(dml);
    }
}

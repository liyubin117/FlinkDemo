package org.lyb;

import static org.apache.flink.table.api.Expressions.row;

import java.util.concurrent.ExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

public class SqlSyntaxTest {
    private TableEnvironment tableEnv;
    private Table source;
    private StreamExecutionEnvironment env;

    @Before
    public void init() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.tableEnv = StreamTableEnvironment.create(env, settings);
        this.source =
                tableEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2), "col id"),
                                DataTypes.FIELD("name", DataTypes.STRING(), "col name")),
                        row(1, "ABC"),
                        row(2L, "ABCDE"));
    }

    @Test
    public void testColumnComment() throws ExecutionException, InterruptedException {
        tableEnv.executeSql(
                "create table t2(id int comment 'hello') comment 'c123 ''c12''' with ('connector'='print')");
        tableEnv.executeSql("insert into t2 select 1 as id").await();
        tableEnv.executeSql("desc t2").print();
        tableEnv.executeSql("show columns from t2").print();
        tableEnv.executeSql("show create table t2").print(); // 1.18支持显示列注释
    }
}

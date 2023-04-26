package org.lyb.hive.dml._04_tumble_window;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.CoreModule;
import org.lyb.hive.HiveModuleV2;

/**
 * hadoop 启动：/usr/local/Cellar/hadoop/3.2.1/sbin/start-all.sh http://localhost:9870/
 * http://localhost:8088/cluster
 *
 * <p>hive 启动：$HIVE_HOME/bin/hive --service metastore & hive cli：$HIVE_HOME/bin/hive
 */
public class Test1 {

    // CREATE TABLE `hive_tumble_window_table`(
    //  `user_id` string,
    //  `order_amount` double,
    //  `server_timestamp` timestamp
    //
    //  )
    // PARTITIONED BY (
    //  `p_date` string)
    //
    //
    // insert into hive_tumble_window_table values ('yyc', 300, '2021-09-30 11:22:57.0',
    // '20210920'), ('yyc', 300,
    // '2021-09-30 11:22:58.0', '20210920'), ('yyc', 300, '2021-09-30 11:23:57.0', '20210920'),
    // ('yyc', 300,
    // '2021-09-30 11:24:57.0', '20210920'), ('yyc', 300, '2021-09-30 11:25:57.0', '20210920'),
    // ('yyc', 300,
    // '2021-09-30 11:25:58.0', '20210920')

    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        6,
                        org.apache.flink.api.common.time.Time.of(10L, TimeUnit.MINUTES),
                        org.apache.flink.api.common.time.Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);

        // ck 设置
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.enableCheckpointing(30 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3L);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig()
                .getConfiguration()
                .setString("pipeline.name", "1.13.5 Interval Outer Join 事件时间案例");

        String defaultDatabase = "default";
        String hiveConfDir = "/usr/local/Cellar/hive/3.1.2/libexec/conf";

        HiveCatalog hive = new HiveCatalog("default", defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("default", hive);

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        // set the HiveCatalog as the current catalog of the session
        tEnv.useCatalog("default");

        String version = "3.1.2";
        tEnv.unloadModule("core");

        HiveModuleV2 hiveModuleV2 = new HiveModuleV2(version);

        tEnv.loadModule("default", hiveModuleV2);
        tEnv.loadModule("core", CoreModule.INSTANCE);

        String sql3 =
                ""
                        + "with tmp as (\n"
                        + "select cast(server_timestamp as timestamp(3)) as ti, order_amount as order_amount from hive_tumble_window_table\n"
                        + ")\n"
                        + "\n"
                        + "select  window_start, window_end, count(1) as part_pv\n"
                        + "     , max(order_amount) as part_max\n"
                        + "     , min(order_amount) as part_min\n"
                        + "from TABLE(\n"
                        + "    TUMBLE(TABLE tmp, DESCRIPTOR(ti), INTERVAL '1' MINUTES))\n"
                        //                                          + "from tmp\n";
                        //                + "where p_date = '20210920'\n"
                        + "group by window_start, window_end\n";

        tEnv.executeSql(sql3).print();
    }
}

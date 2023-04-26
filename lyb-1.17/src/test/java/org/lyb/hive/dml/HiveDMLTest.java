package org.lyb.hive.dml;

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
import org.apache.flink.table.module.hive.HiveModule;

import java.util.concurrent.TimeUnit;


/**
 * hive 启动：$HIVE_HOME/bin/hive --service metastore &
 * hive cli：$HIVE_HOME/bin/hive
 * hadoop 启动：/usr/local/Cellar/hadoop/3.2.1/sbin/start-all.sh
 * http://localhost:9870/
 * http://localhost:8088/cluster
 */
public class HiveDMLTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(6, org.apache.flink.api.common.time.Time
                .of(10L, TimeUnit.MINUTES), org.apache.flink.api.common.time.Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(10);

        // ck 设置
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.enableCheckpointing(30 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3L);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig().getConfiguration().setString("pipeline.name", "1.13.5 Interval Outer Join 事件时间案例");


        String defaultDatabase = "default";
        String hiveConfDir = "/usr/local/Cellar/hive/3.1.2/libexec/conf";

        HiveCatalog hive = new HiveCatalog("default", defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("myhive", hive);

        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        // set the HiveCatalog as the current catalog of the session
        tEnv.useCatalog("myhive");

        String version = "3.1.2";
        tEnv.loadModule("myhive", new HiveModule(version));

        tEnv.executeSql("select count(1) as uv\n"
                + "     , sum(part_pv) as pv\n"
                + "     , max(part_max) as max_no\n"
                + "     , nvl(min(part_min), 1) as min_no\n"
                + "from (\n"
                + "    select user_id\n"
                + "         , count(1) as part_pv\n"
                + "         , max(order_amount) as part_max\n"
                + "         , min(order_amount) as part_min\n"
                + "    from hive_table\n"
                + "    where p_date = '20210920'\n"
                + "    group by user_id\n"
                + ")")
                .print();

    }

}

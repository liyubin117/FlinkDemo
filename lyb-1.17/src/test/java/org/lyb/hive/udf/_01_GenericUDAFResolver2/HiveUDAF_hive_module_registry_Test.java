package org.lyb.hive._01_GenericUDAFResolver2;

import org.lyb.utils.FlinkEnvUtils;

import java.io.IOException;


/**
 * hadoop 启动：/usr/local/Cellar/hadoop/3.2.1/sbin/start-all.sh
 * http://localhost:9870/
 * http://localhost:8088/cluster
 *
 * hive 启动：$HIVE_HOME/bin/hive --service metastore &
 * hive cli：$HIVE_HOME/bin/hive
 */
public class HiveUDAF_hive_module_registry_Test {

    public static void main(String[] args) throws IOException {

        FlinkEnvUtils.FlinkEnv flinkEnv = FlinkEnvUtils.getBatchTableEnv(args);

        // TODO 可以成功执行没有任何问题
        flinkEnv.hiveModuleV2().registryHiveUDF("test_hive_udaf", TestHiveUDAF.class.getName());

        String sql3 = "select test_hive_udaf(user_id)\n"
                + "         , count(1) as part_pv\n"
                + "         , max(order_amount) as part_max\n"
                + "         , min(order_amount) as part_min\n"
                + "    from hive_table\n"
                + "    where p_date between '20210920' and '20210920'\n"
                + "    group by 0";

        flinkEnv.batchTEnv()
                .executeSql(sql3)
                .print();
    }

}

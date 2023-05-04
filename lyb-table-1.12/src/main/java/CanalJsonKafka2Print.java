/**
 * @author Yubin Li
 * @date 2022/2/28 15:04
 *     {"data":[{"id":"1","name":"scooter","description":"desc","weight":"5.18"}],"type":"INSERT"}
 *     {"data":[{"id":"2","name":"TWO","description":"desc2","weight":"5.18"}],"type":"INSERT"}
 *     {"data":[{"id":"1","name":"one","description":"desc3","weight":"5.18"}],"type":"UPDATE","old":[{"id":"1","name":"scooter","description":"desc","weight":"5.18"}]}
 *     {"data":[{"id":"2","name":"new","description":"desc4","weight":"5.18"}],"type":"UPDATE","old":[{"id":"2","name":"TWO","description":"desc2","weight":"5.18"}]}
 */
public class CanalJsonKafka2Print {
    public static void main(String[] args) {
        // DataGen
        String sourceDDL =
                "CREATE TABLE topic_products (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  description STRING,\n"
                        + "  weight DECIMAL(10, 2)\n"
                        + ") WITH (\n"
                        + " 'connector' = 'kafka',\n"
                        + " 'topic' = 'lyb-topic',\n"
                        + " 'properties.bootstrap.servers' = 'sloth-test2.dg.163.org:9092',\n"
                        + " 'properties.group.id' = 'testGroup',\n"
                        + " 'scan.startup.mode' = 'latest-offset',\n"
                        + " 'format' = 'canal-json'\n"
                        + ")";

        // Print
        String sinkDDL =
                "CREATE TABLE rst (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  description STRING,\n"
                        + "  weight DECIMAL(10, 2)\n"
                        + ") WITH (\n"
                        + " 'connector' = 'print'\n"
                        + ")";

        // 注册source和sink
        Env.tableEnv.executeSql(sourceDDL);
        Env.tableEnv.executeSql(sinkDDL);

        String sql = "INSERT INTO rst SELECT * from topic_products";

        Env.tableEnv.executeSql(sql);
    }
}

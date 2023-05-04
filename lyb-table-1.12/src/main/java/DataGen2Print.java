/** 功能描述: 从Kafka读取消息数据，并在控制台进行打印。 操作步骤: 1. 直接执行作业，看到输出 2. 调试查看sql的plan 3. 断点，查看codegen 代码 */
public class DataGen2Print {
    public static void main(String[] args) throws Exception {
        // DataGen
        String sourceDDL =
                "CREATE TABLE stu (\n"
                        + " name STRING , \n"
                        + " age INT \n"
                        + ") WITH (\n"
                        + " 'connector' = 'datagen' ,\n"
                        + " 'fields.name.length'='10'\n"
                        + ")";

        // Print
        String sinkDDL =
                "CREATE TABLE rst (\n"
                        + " name STRING , \n"
                        + " age INT , \n"
                        + " weight INT  \n"
                        + ") WITH (\n"
                        + " 'connector' = 'print'\n"
                        + ")";

        // 注册source和sink
        Env.tableEnv.executeSql(sourceDDL);
        Env.tableEnv.executeSql(sinkDDL);

        String sql = "INSERT INTO rst SELECT stu.name, stu.age-2, 35+2 FROM stu";

        Env.tableEnv.executeSql(sql);
    }
}

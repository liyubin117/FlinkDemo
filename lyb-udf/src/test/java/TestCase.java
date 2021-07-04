import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.row;

/**
 * @author Yubin Li
 * @date 2021/7/4 11:12
 **/
public class TestCase {
    private StreamTableEnvironment tableEnv;
    private Table source;
    @Before
    public void init(){
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.tableEnv = StreamTableEnvironment.create(env, settings);
        this.source = tableEnv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "ABC"),
                row(2L, "ABCDE"));
    }

    @Test
    public void testScalar() throws Exception {
        tableEnv.createTemporaryView("source", source);
        tableEnv.sqlQuery("select * from source");
//        tableEnv.execute("scalar function");
    }
}

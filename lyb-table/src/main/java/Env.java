import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

public class Env {
    public static EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    public static StreamTableEnvironment tableEnv;
    static {
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(20000);
        tableEnv = StreamTableEnvironment.create(env, settings);
    }
}

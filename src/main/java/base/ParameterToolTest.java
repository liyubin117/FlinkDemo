package base;

import org.apache.flink.api.java.utils.ParameterTool;

public class ParameterToolTest {
    public static void main(String[] args) {
        ParameterTool pt = ParameterTool.fromArgs(args);
        if (pt.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic> --output-topic <topic> ");
        }
        System.out.println(pt.get("input-topic"));
        System.out.println(pt.get("output-topic"));
        System.out.println(pt.get("other"));
    }
}

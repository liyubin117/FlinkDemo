package Table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class JavaSetTable{
    public static void main(String[] args) throws Exception {
        //配置环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        //指定输入
        DataSet<String> input = env.fromElements(
                "1,liyubin,26",
                "2,li,27",
                "3,yu,25",
                "4,bin,26",
                "4,bin,26"
        );
        //生成DataSet
        DataSet<Person> personSet = input.map(new MapFunction<String, Person>() {
            public Person map(String value) throws Exception {
                String[] x = value.split(",");
                return new Person(Integer.valueOf(x[0]), x[1], Integer.valueOf(x[2]));
            }
        });
        tableEnv.registerDataSet("person", personSet,"id, name, age");
        //指定表程序
        Table counts = tableEnv.scan("person").filter("age>=26").groupBy("age").select("age,id.count() as cnt");
//        Table counts = tableEnv.sqlQuery("select age,count(1) from person where age>=26 group by age");

//        //结果转化为DataSet
        DataSet<Result> result = tableEnv.toDataSet(counts, Result.class);
        result.print();
    }

}


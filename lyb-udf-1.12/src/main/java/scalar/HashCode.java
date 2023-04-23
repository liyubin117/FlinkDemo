package scalar;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author Yubin Li
 * @date 2021/7/4 10:53
 **/
public class HashCode extends ScalarFunction {
    public HashCode() {}
    private int factor=10;
    public HashCode(int factor){
        this.factor = factor;
    }
    public Integer eval(String input){
        return input.hashCode() * this.factor;
    }
}

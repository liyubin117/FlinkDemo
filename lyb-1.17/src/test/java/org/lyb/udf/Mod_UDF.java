package org.lyb.udf;

import org.apache.flink.table.functions.ScalarFunction;


public class Mod_UDF extends ScalarFunction {

    public int eval(long id, int remainder) {
        return (int) (id % remainder);
    }

}

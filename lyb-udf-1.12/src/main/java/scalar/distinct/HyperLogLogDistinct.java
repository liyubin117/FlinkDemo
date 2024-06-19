package scalar.distinct;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.AggregateFunction;
import net.agkn.hll.HLL;

public class HyperLogLogDistinct implements AggregateFunction<Tuple2<String,Long>,HLL,Long> {
    @Override
    public HLL createAccumulator() {
        return new HLL(14, 5);
    }
    @Override
    public HLL add(Tuple2<String, Long> value, HLL accumulator) {
        //value 为访问记录 <商品sku, 用户id>
        accumulator.addRaw(value.f1);
        return accumulator;
    }
    @Override
    public Long getResult(HLL accumulator) {
        long cardinality = accumulator.cardinality();
        return cardinality;
    }
    @Override
    public HLL merge(HLL a, HLL b) {
        a.union(b);
        return a;
    }
}
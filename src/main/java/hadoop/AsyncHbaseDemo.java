package hadoop;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;

/**
 * @author liyubin
 * @version 1.0
 * @description Deferred类似于JDK中的异步线程返回对象Future，这里加了个Callback对象是当Deferred为available时且是一个异常，就会执行加入的Callback对象。
 * 异步hbase大大提供了大数据量下hbase的写入性能，是一个不错的优化点
 * create 'lyb_testasync','f1','f2'
 */
public class AsyncHbaseDemo {
    public static void main(String[] args) {
        HBaseClient client = new HBaseClient("fuxi-luoge-76");

        PutRequest request = new PutRequest("lyb_testasync", "rk1", "f1", "c1", "v1");
        PutRequest request2 = new PutRequest("lyb_testasync", "rk1", "f2", "", "v2");


        //异步写入
        Deferred<Object> d;
        d = client.put(request2);
        d.addCallback(arg -> {
            return "put successfully!";
        });
        d.addErrback(new Callback<Object, Exception>() {
            @Override
            public Object call(final Exception arg) {
                if (arg instanceof PleaseThrottleException) {
                    final PleaseThrottleException e = (PleaseThrottleException) arg;
                    final HBaseRpc rpc = e.getFailedRpc();
                    if (rpc instanceof PutRequest) {
                        client.put((PutRequest) rpc); // Don't lose edits.
                    }
                    return null;
                }
                return arg;
            }

            @Override
            public String toString() {
                return "importFile errback";
            }
        });
        System.out.println(d);
    }

}

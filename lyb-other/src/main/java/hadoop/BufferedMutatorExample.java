package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * hbase-client 异步提交
 * An example of using the {@link BufferedMutator} interface.
 */
public class BufferedMutatorExample extends Configured implements Tool {
    private static final int POOL_SIZE = 10;
    private static final int TASK_COUNT = 100;
    private static final TableName TABLE = TableName.valueOf("lyb_asynctest");
    private static final byte[] FAMILY = Bytes.toBytes("info");
    private static Configuration hbaseconfig;
    static{
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","fuxi-luoge-76");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        hbaseconfig = HBaseConfiguration.create(conf);
    }
    @Override
    public int run(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
 
        /** a callback invoked when an asynchronous write fails. */
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    System.out.println("Failed to sent put " + i + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);
 
        //  
        // step 1: create a single Connection and a BufferedMutator, shared by all worker threads.  
        //  
        try {
            final Connection connection = ConnectionFactory.createConnection(hbaseconfig);
            final BufferedMutator mutator = connection.getBufferedMutator(params);
            final Table t = connection.getTable(TableName.valueOf(TABLE.toString()));
            /** worker pool that operates on BufferedTable instances */
            final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
            List<Future<Void>> futures = new ArrayList<Future<Void>>(TASK_COUNT);
 
            for (int i = 0; i < TASK_COUNT; i++) {
                futures.add(workerPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        //  
                        // step 2: each worker sends edits to the shared BufferedMutator instance. They all use  
                        // the same backing buffer, call-back "listener", and RPC executor pool.  
                        //  
//                        Put p = new Put(Bytes.toBytes("someRow"));
                        Put p = new Put(Bytes.toBytes(new Random().nextInt()));
                        p.addColumn(FAMILY, Bytes.toBytes("someQualifier"), Bytes.toBytes("some value"));
                        mutator.mutate(p);
                        mutator.flush();
                        // do work... maybe you want to call mutator.flush() after many edits to ensure any of  
                        // this worker's edits are sent before exiting the Callable  
                        System.out.println("====put=====");
                        Get get=new Get(Bytes.toBytes("someRow"));
                        Result result=t.get(get);
                        for(Cell cell:result.rawCells()){
                            System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
                            System.out.print("\t列簇: "+new String(CellUtil.cloneFamily(cell)));
                            System.out.print("\t列: "+new String(CellUtil.cloneQualifier(cell)));
                            System.out.print("\t值: "+new String(CellUtil.cloneValue(cell)));
                            System.out.println("\t时间戳: "+cell.getTimestamp());
                        }
                        System.out.print(">>>>end");
                        return null;
                    }
                }));
            }
 
            //  
            // step 3: clean up the worker pool, shut down.  
            //  
            for (Future<Void> f : futures) {
                f.get(5, TimeUnit.SECONDS);
            }
            Thread.sleep(5000*10000);
            workerPool.shutdown();
            System.out.println("=====================OK=====================");
        } catch (IOException e) {
            // exception while creating/destroying Connection or BufferedMutator  
            e.printStackTrace();
        } // BufferedMutator.close() ensures all work is flushed. Could be the custom listener is  
        // invoked from here.  
        return 0;
    }
 
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BufferedMutatorExample(), args);
    }
}
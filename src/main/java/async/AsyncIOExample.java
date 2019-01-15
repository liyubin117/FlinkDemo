//package async;
//
//import com.google.common.base.Stopwatch;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.async.AsyncFunction;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
//import org.apache.flink.util.Collector;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Collections;
//import java.util.Random;
//import java.util.concurrent.*;
//import java.util.function.Supplier;
//
//import static java.lang.Thread.sleep;
//
///**
// * Example to illustrates how to use {@link AsyncFunction}
// */
//public class AsyncIOExample {
//
//	private static final Logger LOG = LoggerFactory.getLogger(AsyncIOExample.class);
//
//	private static final String EXACTLY_ONCE_MODE = "exactly_once";
//	private static final String EVENT_TIME = "EventTime";
//	private static final String INGESTION_TIME = "IngestionTime";
//	private static final String ORDERED = "ordered";
//
//
//	/**
//	 * An sample of {@link AsyncFunction} using a thread pool and executing working threads
//	 * to simulate multiple async operations.
//	 * <p>
//	 * For the real use case in production environment, the thread pool may stay in the
//	 * async client.
//	 */
//	private static class SampleAsyncFunction extends RichAsyncFunction<String, String> {
//		private static final long serialVersionUID = 2098635244857937717L;
//
//		private static ExecutorService executorService;
//		private static Random random;
//
//		private int counter;
//
//		/**
//		 * The result of multiplying sleepFactor with a random float is used to pause
//		 * the working thread in the thread pool, simulating a time consuming async operation.
//		 */
//		private final long sleepFactor;
//
//		/**
//		 * The ratio to generate an exception to simulate an async error. For example, the error
//		 * may be a TimeoutException while visiting HBase.
//		 */
//		private final float failRatio;
//
//		private final long shutdownWaitTS;
//
//		SampleAsyncFunction(long sleepFactor, float failRatio, long shutdownWaitTS) {
//			this.sleepFactor = sleepFactor;
//			this.failRatio = failRatio;
//			this.shutdownWaitTS = shutdownWaitTS;
//		}
//
//
//		@Override
//		public void open(Configuration parameters) throws Exception {
//			super.open(parameters);
//
//			synchronized (SampleAsyncFunction.class) {
//				if (counter == 0) {
//					executorService = Executors.newFixedThreadPool(30);
//
//					random = new Random();
//				}
//
//				++counter;
//			}
//		}
//
//		@Override
//		public void close() throws Exception {
//			super.close();
//
//			synchronized (SampleAsyncFunction.class) {
//				--counter;
//
//				if (counter == 0) {
//					executorService.shutdown();
//
//					try {
//						if (!executorService.awaitTermination(shutdownWaitTS, TimeUnit.MILLISECONDS)) {
//							executorService.shutdownNow();
//						}
//					} catch (InterruptedException e) {
//						executorService.shutdownNow();
//					}
//				}
//			}
//		}
//
////		@Override
////		//AsyncCollector在1.3有，1.8没有
////		public void asyncInvoke(final String input, final AsyncCollector<String> collector) throws Exception {
////			this.executorService.submit(new Runnable() {
////				@Override
////				public void run() {
////					// wait for while to simulate async operation here
////					//long sleep = (long) (random.nextFloat() * sleepFactor);
////					try {
////						sleep(1); 				// simulate IO operation
////					} catch (InterruptedException e) {
////						e.printStackTrace();
////					}
////					collector.collect(
////						Collections.singletonList("key-" + input));
////				}
////			});
////		}
//
//        @Override
//        public void asyncInvoke(final String input, ResultFuture<String> resultFuture) throws Exception {
//            Future<String> future = this.executorService.submit(new Runnable() {
//                @Override
//                public void run() {
//                    // wait for while to simulate async operation here
//                    //long sleep = (long) (random.nextFloat() * sleepFactor);
//                    try {
//                        sleep(1); 				// simulate IO operation
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            });
//            CompletableFuture.supplyAsync(new Supplier<String>() {
//
//                @Override
//                public String get() {
//                    try {
//                        return future.get();
//                    } catch (InterruptedException | ExecutionException e) {
//                        // Normally handled explicitly.
//                        return null;
//                    }
//                }
//            }).thenAccept( (String dbResult) -> {
//                resultFuture.complete(Collections.singleton("completed!"));
//            });
//        }
//	}
//
//	private static void printUsage() {
//		System.out.println("To customize example, use: AsyncIOExample [--fsStatePath <path to fs state>] " +
//				"[--checkpointMode <exactly_once or at_least_once>] " +
//				"[--maxCount <max number of input from source, -1 for infinite input>] " +
//				"[--sleepFactor <interval to sleep for each stream element>] [--failRatio <possibility to throw exception>] " +
//				"[--waitMode <ordered or unordered>] [--waitOperatorParallelism <parallelism for async wait operator>] " +
//				"[--eventType <EventTime or IngestionTime>] [--shutdownWaitTS <milli sec to wait for thread pool>]" +
//				"[--timeout <Timeout for the asynchronous operations>]");
//	}
//
//	public static void main(String[] args) throws Exception {
//
//		// obtain execution environment
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		// parse parameters
//		final ParameterTool params = ParameterTool.fromArgs(args);
//
//		final String statePath;
//		final String cpMode;
//		final int maxCount;
//		final long sleepFactor;
//		final float failRatio;
//		final String mode;
//		final int taskNum;
//		final String timeType;
//		final long shutdownWaitTS;
//		final long timeout;
//
//		try {
//			// check the configuration for the job
//			statePath = params.get("fsStatePath", null);
//			cpMode = params.get("checkpointMode", "exactly_once");
//			maxCount = params.getInt("maxCount", 100000);
//			sleepFactor = params.getLong("sleepFactor", 100);
//			failRatio = params.getFloat("failRatio", 0.001f);
//			mode = params.get("waitMode", "ordered");
//			taskNum = params.getInt("waitOperatorParallelism", 1);
//			timeType = params.get("eventType", "EventTime");
//			shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
//			timeout = params.getLong("timeout", 10000L);
//		} catch (Exception e) {
//			printUsage();
//
//			throw e;
//		}
//
//		StringBuilder configStringBuilder = new StringBuilder();
//
//		final String lineSeparator = System.getProperty("line.separator");
//
//		configStringBuilder
//			.append("Job configuration").append(lineSeparator)
//			.append("FS state path=").append(statePath).append(lineSeparator)
//			.append("Checkpoint mode=").append(cpMode).append(lineSeparator)
//			.append("Max count of input from source=").append(maxCount).append(lineSeparator)
//			.append("Sleep factor=").append(sleepFactor).append(lineSeparator)
//			.append("Fail ratio=").append(failRatio).append(lineSeparator)
//			.append("Waiting mode=").append(mode).append(lineSeparator)
//			.append("Parallelism for async wait operator=").append(taskNum).append(lineSeparator)
//			.append("Event type=").append(timeType).append(lineSeparator)
//			.append("Shutdown wait timestamp=").append(shutdownWaitTS);
//
//		LOG.info(configStringBuilder.toString());
//
//		if (statePath != null) {
//			// setup state and checkpoint mode
//			env.setStateBackend(new FsStateBackend(statePath));
//		}
//
//		// enable watermark or not
//		if (EVENT_TIME.equals(timeType)) {
//			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//		}
//		else if (INGESTION_TIME.equals(timeType)) {
//			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
//		}
//
////		final Stopwatch stopwatch = Stopwatch.createStarted();
//
//		// create input stream of an single integer
//		// DataStream<String> inputStream = env.fromElements(WordCountData.WORDS).flatMap(new Tokenizer());
//		DataStream<String> inputStream = env.readTextFile("big.txt").flatMap(new Tokenizer());
//
//		DataStream<String> result;
//
//		// Benchmark 1: tracitional I/O
//		// result = inputStream.map(new LegacyRedisWriter());
//
//		// Benchmark 2: Async I/O
//		result = asyncIOAproach(sleepFactor, failRatio, mode, taskNum, shutdownWaitTS, timeout, inputStream);
//
//
//		// add a reduce to get the sum of each keys.
//		result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//			private static final long serialVersionUID = -938116068682344455L;
//
//			@Override
//			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//				out.collect(new Tuple2<>(value, 1));
//			}
//		}).keyBy(0).sum(1).print();
//
//		// execute the program
//		env.execute("Async IO Example");
//
////		stopwatch.stop(); //optional
////
////		System.out.println("Elapsed time ==> " + stopwatch);
//	}
//
//	private static DataStream<String> asyncIOAproach(long sleepFactor, float failRatio, String mode, int taskNum, long shutdownWaitTS, long timeout, DataStream<String> inputStream) {
//		// create async function, which will *wait* for a while to simulate the process of async i/o
//		AsyncFunction<String, String> function =
//				new SampleAsyncFunction(sleepFactor, failRatio, shutdownWaitTS);
//
//		// add async operator to streaming job
//		DataStream<String> result;
//		if (ORDERED.equals(mode)) {
//			result = AsyncDataStream.orderedWait(
//				inputStream,
//				function,
//				timeout,
//				TimeUnit.MILLISECONDS,
//				20).setParallelism(taskNum);
//		}
//		else {
//			result = AsyncDataStream.unorderedWait(
//				inputStream,
//				function,
//				timeout,
//				TimeUnit.MILLISECONDS,
//				20).setParallelism(taskNum);
//		}
//		return result;
//	}
//
//	private static class Tokenizer implements FlatMapFunction<String,String> {
//		@Override
//		public void flatMap(String value, Collector<String> out) throws Exception {
//			String[] tokens = value.toLowerCase().split("\\W+");
//
//			// emit the pairs
//			for (String token : tokens) {
//				if (token.length() > 0) {
//					out.collect(token);
//				}
//			}
//		}
//	}
//
//	private static class LegacyRedisWriter implements org.apache.flink.api.common.functions.MapFunction<String,String> {
//		@Override
//		public String map(String input) throws Exception {
//			String key = "key-" + input;
//			sleep(1); // simulate IO operation
//			/*Jedis jedis = new Jedis();
//			jedis.set(key, String.valueOf(System.currentTimeMillis() / 1000L));
//			jedis.close();
//			*/
//			return key;
//		}
//	}
//}
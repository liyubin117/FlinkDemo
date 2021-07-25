package sink;

import org.apache.avro.Schema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.filesystem.FileSystemTableSink;

import java.util.Arrays;

public class StreamingFileSinkTest {
    public static void main(String[] args) {
        final StreamingFileSink sink = StreamingFileSink
                .forBulkFormat(
                        new Path("/home/rick/data/StreamingFileSinkTest"),
                        ParquetAvroWriters.forReflectRecord(String.class))
//                .withRollingPolicy(new FileSystemTableSink.TableRollingPolicy(true, 500, 60000))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
    }
}

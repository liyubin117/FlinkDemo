package hadoop.format.parquet;

import static hadoop.format.parquet.MessageTypeGenerationType.CODE;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.JulianFields;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Yubin Li */
public class Utils {
    public static final String DELIMITER = "------";
    public static Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static MessageType getMessageType(MessageTypeGenerationType type, String inPath)
            throws IOException {
        MessageType result;
        switch (type) {
            case CODE:
                result =
                        Types.buildMessage()
                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(OriginalType.UTF8)
                                .named("bookName")
                                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                                .named("market")
                                .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                                .named("price")
                                .requiredGroup()
                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(OriginalType.UTF8)
                                .named("name")
                                .required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("age")
                                .required(PrimitiveType.PrimitiveTypeName.INT96)
                                .named("cash")
                                .named("author")
                                .named("Book");
                break;
            case STRING:
                String schemaString =
                        "message Book {\n"
                                + "  required binary bookName (UTF8);\n"
                                + "  required boolean market;\n"
                                + "  required double price;\n"
                                + "  repeated group author {\n"
                                + "    required binary name (UTF8);\n"
                                + "    required int32 age;\n"
                                + "    required int96 cash;\n"
                                + "  }\n"
                                + "}";
                result = MessageTypeParser.parseMessageType(schemaString);
                break;
            case FILE:
                HadoopInputFile hadoopInputFile =
                        HadoopInputFile.fromPath(new Path(inPath), new Configuration());
                ParquetFileReader parquetFileReader =
                        ParquetFileReader.open(
                                hadoopInputFile, ParquetReadOptions.builder().build());
                ParquetMetadata metaData = parquetFileReader.getFooter();
                result = metaData.getFileMetaData().getSchema();
                // 记得关闭
                parquetFileReader.close();
                break;
            default:
                throw new RuntimeException("insufficient type");
        }
        return result;
    }

    public static void parquetRead(String inPath) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader.Builder<Group> builder = ParquetReader.builder(readSupport, new Path(inPath));
        ParquetReader<Group> parquetReader = builder.build();
        Group line;
        while ((line = parquetReader.read()) != null) {
            Group time = line.getGroup("author", 0);
            // 通过下标和字段名称都可以获取
            /*System.out.println(line.getString(0, 0)+"\t"+
            line.getString(1, 0)+"\t"+
            time.getInteger(0, 0)+"\t"+
            time.getString(1, 0)+"\t");*/

            System.out.println(
                    line.getString("bookName", 0)
                            + "\t"
                            + line.getDouble("price", 0)
                            + "\t"
                            + time.getInteger("age", 0)
                            + "\t"
                            + time.getString("name", 0)
                            + "\t");
        }
        System.out.println("读取结束");
    }

    @Deprecated
    public static void parquetMetadataReadDeprecate(String inPath) throws IOException {
        System.out.println("读footer，获取元数据");
        Configuration conf = new Configuration(true);
        ParquetMetadata pd = ParquetFileReader.readFooter(conf, new Path(inPath));
        FileMetaData fm = pd.getFileMetaData();
        MessageType schema = fm.getSchema();
        List<ColumnDescriptor> columns = schema.getColumns();
        System.out.println("字段数：" + columns.size());
        System.out.println(columns);
        System.out.println("第4个字段：" + columns.get(3));
        ColumnDescriptor descriptor = columns.get(3);
        for (String str : descriptor.getPath()) System.out.println("字段：" + str);
        System.out.println(descriptor.getType());
    }

    public static void parquetMetadataRead(String inPath) throws IOException {
        System.out.println("read footer, get metadata");
        HadoopInputFile hadoopInputFile =
                HadoopInputFile.fromPath(new Path(inPath), new Configuration());
        ParquetFileReader parquetFileReader =
                ParquetFileReader.open(hadoopInputFile, ParquetReadOptions.builder().build());
        ParquetMetadata pm = parquetFileReader.getFooter();

        System.out.println("----block level metadata----");
        List<BlockMetaData> bml = pm.getBlocks();
        Map<ColumnPath, Statistics> sumStats = Maps.newHashMap();
        int index = 0;
        for (BlockMetaData bm : bml) {
            System.out.println(
                    "rowCount: "
                            + bm.getRowCount()
                            + "\t"
                            + "totalByteSize: "
                            + bm.getTotalByteSize());
            List<ColumnChunkMetaData> cml = bm.getColumns();
            for (ColumnChunkMetaData cm : cml) {
                Statistics stats = cm.getStatistics();
                ColumnPath path = cm.getPath();
                PrimitiveType type = cm.getPrimitiveType();
                LOG.info(
                        String.valueOf(
                                stats.type()
                                        .equals(type))); // note that column stat get the same type
                // as column
                Statistics mid;
                if (!sumStats.containsKey(path)) {
                    mid = stats;
                } else {
                    mid = sumStats.get(path);
                    mid.mergeStatistics(stats);
                }
                sumStats.put(path, mid);
                System.out.println(
                        path
                                + "\t"
                                + path.toDotString()
                                + "\t"
                                + type.getPrimitiveTypeName()
                                + "\t"
                                + type.getName()
                                + "\t"
                                + stats
                                + "\t"
                                + stats.hasNonNullValue()
                                + "\t"
                                + type.getDecimalMetadata());
            }
            System.out.println("--per block " + index++ + " --");
        }
        sumStats.forEach((k, v) -> System.out.println(k + "\t" + v));

        System.out.println("----file level metadata----");
        FileMetaData fm = pm.getFileMetaData();
        System.out.println(fm.getCreatedBy());
        fm.getKeyValueMetaData().forEach((x, y) -> System.out.println(x + "=" + y));
        MessageType schema = fm.getSchema();
        List<ColumnDescriptor> columns = schema.getColumns();
        System.out.println("fields num: " + columns.size());
        System.out.println(columns);
        System.out.println("forth field: " + columns.get(3));
        ColumnDescriptor descriptor = columns.get(3);
        for (String str : descriptor.getPath()) System.out.println("field: " + str);
        System.out.println(descriptor.getType());

        parquetFileReader.close();
    }

    @Deprecated
    // 新版本中new ParquetReader()所有构造方法好像都弃用了,用上面的builder去构造对象
    public static void parquetReadDeprecate(String inPath) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<Group>(new Path(inPath), readSupport);
        Group line = null;
        while ((line = reader.read()) != null) {
            System.out.println(line.toString());
        }
        System.out.println("读取结束");
    }

    /**
     * @param outPath 　　输出Parquet格式
     * @throws IOException
     */
    public static void parquetWrite(String outPath, String content)
            throws IOException, ParseException {
        File file = new File(outPath);
        if (file.exists()) {
            file.delete();
        }
        MessageType schema = getMessageType(CODE, null);
        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = new Path(outPath);
        CompressionCodecName compressionCodec = CompressionCodecName.UNCOMPRESSED;
        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, configuration);
        writeSupport.init(configuration);
        ParquetWriter<Group> writer =
                new ParquetWriter<Group>(path, writeSupport, compressionCodec, 9999, 999);

        String[] contentArr = content.split("\n");

        for (String line : contentArr) {
            String[] strs = line.split(DELIMITER);
            if (strs.length == 5) {
                Group group =
                        factory.newGroup()
                                .append("bookName", strs[0])
                                .append("price", Double.valueOf(strs[1]));
                Group author = group.addGroup("author");
                author.append("age", Integer.valueOf(strs[2]));
                author.append("name", strs[3]);
                author.append("cash", convertTimestampToParquetType(strs[4]));
                writer.write(group);
            }
        }
        System.out.println("write end");
        writer.close();
    }

    public static Binary convertTimestampToParquetType(String timestamp) throws ParseException {
        String value = timestamp;

        final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
        final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
        final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

        // Parse date
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(parser.parse(value));

        // Calculate Julian days and nanoseconds in the day
        LocalDate dt =
                LocalDate.of(
                        cal.get(Calendar.YEAR),
                        cal.get(Calendar.MONTH) + 1,
                        cal.get(Calendar.DAY_OF_MONTH));
        int julianDays = (int) JulianFields.JULIAN_DAY.getFrom(dt);
        long nanos =
                (cal.get(Calendar.HOUR_OF_DAY) * NANOS_PER_HOUR)
                        + (cal.get(Calendar.MINUTE) * NANOS_PER_MINUTE)
                        + (cal.get(Calendar.SECOND) * NANOS_PER_SECOND);

        // Write INT96 timestamp
        byte[] timestampBuffer = new byte[12];
        ByteBuffer buf = ByteBuffer.wrap(timestampBuffer);
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(julianDays);

        // This is the properly encoded INT96 timestamp
        Binary tsValue = Binary.fromReusedByteArray(timestampBuffer);
        return tsValue;
    }
}

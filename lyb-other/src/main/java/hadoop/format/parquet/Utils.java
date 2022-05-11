package hadoop.format.parquet;

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
import org.apache.parquet.schema.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import static hadoop.format.parquet.MessageTypeGenerationType.CODE;

/**
 * @author Yubin Li
 */
public class Utils {
    public static MessageType getMessageType(MessageTypeGenerationType type, String inPath) throws IOException {
        MessageType result;
        switch (type) {
            case CODE:
                result = Types.buildMessage()
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("bookName")
                        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("market")
                        .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("price")
                        .requiredGroup()
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
                        .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
                        .named("author")
                        .named("Book");
                break;
            case STRING:
                String schemaString = "message Book {\n" +
                        "  required binary bookName (UTF8);\n" +
                        "  required boolean market;\n" +
                        "  required double price;\n" +
                        "  repeated group author {\n" +
                        "    required binary name (UTF8);\n" +
                        "    required int32 age;\n" +
                        "  }\n" +
                        "}";
                result = MessageTypeParser.parseMessageType(schemaString);
                break;
            case FILE:
                HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(new Path(inPath), new Configuration());
                ParquetFileReader parquetFileReader = ParquetFileReader.open(hadoopInputFile, ParquetReadOptions.builder().build());
                ParquetMetadata metaData = parquetFileReader.getFooter();
                result = metaData.getFileMetaData().getSchema();
                //记得关闭
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
            //通过下标和字段名称都可以获取
            /*System.out.println(line.getString(0, 0)+"\t"+
            　　　　　　　　line.getString(1, 0)+"\t"+
            　　　　　　　　time.getInteger(0, 0)+"\t"+
            　　　　　　　　time.getString(1, 0)+"\t");*/

            System.out.println(line.getString("bookName", 0) + "\t" +
                    line.getDouble("price", 0) + "\t" +
                    time.getInteger("age", 0) + "\t" +
                    time.getString("name", 0) + "\t");

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
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(new Path(inPath), new Configuration());
        ParquetFileReader parquetFileReader = ParquetFileReader.open(hadoopInputFile, ParquetReadOptions.builder().build());
        ParquetMetadata pm = parquetFileReader.getFooter();

        System.out.println("----block level metadata----");
        List<BlockMetaData> bml = pm.getBlocks();
        for(BlockMetaData bm : bml) {
            System.out.println(bm.getRowCount() + "\t" + bm.getTotalByteSize());
            List<ColumnChunkMetaData> cml = bm.getColumns();
            for (ColumnChunkMetaData cm : cml) {
                Statistics stats = cm.getStatistics();
                System.out.println(cm.getPath() + "\t" + stats);
            }
            System.out.println("--per block--");
        }

        System.out.println("----file level metadata----");
        FileMetaData fm = pm.getFileMetaData();
        System.out.println(fm.getCreatedBy());
        fm.getKeyValueMetaData().forEach((x,y) -> System.out.println(x + "=" + y));
        MessageType schema = fm.getSchema();
        List<ColumnDescriptor> columns = schema.getColumns();
        System.out.println("fields num: " + columns.size());
        System.out.println(columns);
        System.out.println("forth field: " + columns.get(3));
        ColumnDescriptor descriptor = columns.get(3);
        for (String str : descriptor.getPath()) System.out.println("field: " + str);
        System.out.println(descriptor.getType());

        //记得关闭
        parquetFileReader.close();
    }

    @Deprecated
    //新版本中new ParquetReader()所有构造方法好像都弃用了,用上面的builder去构造对象
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
    public static void parquetWrite(String outPath, String content) throws IOException {
        File file = new File(outPath);
        if (file.exists()) {
            file.delete();
        }
        MessageType schema = getMessageType(CODE, null);
        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = new Path(outPath);
        CompressionCodecName compressionCodec = CompressionCodecName.SNAPPY;
        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema, configuration);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);

        Random r = new Random();
        String[] contentArr = content.split("\n");
        for (String line : contentArr) {
            String[] strs = line.split("\\s+");
            if (strs.length == 2) {
                Group group = factory.newGroup()
                        .append("bookName", strs[0])
                        .append("price", Double.valueOf(strs[1]));
                Group author = group.addGroup("author");
                author.append("age", r.nextInt(9) + 1);
                author.append("name", r.nextInt(9) + "_a");
                writer.write(group);
            }
        }
        System.out.println("write end");
        writer.close();
    }
}
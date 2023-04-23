package hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class FileStatusTest {
	private static Configuration conf=new Configuration();
	private static FileSystem fs=null;
	private static String path="hdfs://my:9000/";
	private static String isDir=null;
	
	public static void main(String[] args) throws IOException{
	    String uri = path.substring(0,path.indexOf("/",3));
        System.out.println(uri);
//		fs=FileSystem.get(URI.create(uri), conf);
//        System.out.println(fs.exists(new Path("/checkpoint/")));
//		FileStatus stat=fs.getFileStatus(new Path("/user/hadoop/"));
//		if(stat.isDir())
//			isDir="d";
//		else
//			isDir="-";
//		System.out.println(stat.getGroup()+"\t"+stat.getOwner()+"\t"+isDir+stat.getPermission().toString()+"\t");
//		FileStatus[] status=fs.listStatus(new Path("/user/hadoop"));
//		for(FileStatus s:status){
//			System.out.println(s.getPath()+"\t"+s.getGroup()+"\t"+s.getOwner()+"\t"+s.getReplication()+"\t"+(s.isDir()?"d":"f"));
//		}
	}
}

package hadoop.krb;

import hadoop.fs.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

import static hadoop.fs.HDFSUtils.getCurrentClassPath;
import static hadoop.fs.HDFSUtils.getResourcePath;
import static java.security.Security.getProperty;

/**
 * 同时访问 krb、simple 集群
 */
public class AuthNoauthKrb {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //初始化认证集群配置文件
        String krbDir = System.getProperty("user.dir") + File.separator + "krb";
        System.out.println(krbDir);
        Configuration configuration = HDFSUtils.initConfiguration(krbDir);
        System.out.println(configuration);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("hadoop.security.authentication", "kerberos");
        //该线程循环访问认证集群
        new Thread(() -> {
            try {
                while (true) {
                    synchronized ("A") {
                        //配置kerberos认证的配置文件
                        System.setProperty("java.security.krb5.conf", krbDir + File.separator + "krb5.conf");
                        System.setProperty("sun.security.krb5.debug", "true");
                        //进行身份认证
                        UserGroupInformation.setConfiguration(configuration);
                        UserGroupInformation.loginUserFromKeytab("sloth/dev@BDMS.163.COM",
                                krbDir + File.separator + "sloth.keytab");
                        System.out.println("当前用户是：" +
                                UserGroupInformation.getCurrentUser());
                        //列出根目录下所有文件
                        FileSystem fileSystem = FileSystem.get(configuration);
                        FileStatus[] files = fileSystem.listStatus(new Path("hdfs://bdms-test/user/sloth/lyb/hdfs_meta1109"));
                        for (FileStatus file : files) {
                            System.out.println("KRB:" + file.getPath());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();


        //初始化非认证配置文件
//     Configuration noAuthconfiguration = HDFSUtils.initConfiguration(File.separator + "root"
//           + File.separator + "conf");
//     noAuthconfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//
//     //该线程循环访问非认证集群
//     new Thread(() -> {
//        try {
//          while (true) {
//            synchronized ("A") {
//              //重置认证信息
//              UserGroupInformation.reset();
//              System.out.println("当前用户是：" + UserGroupInformation.getCurrentUser());
//              UserGroupInformation.setConfiguration(noAuthconfiguration);
//              //列出根目录下所有文件
//              FileSystem fileSystem = FileSystem.get(noAuthconfiguration);
//              FileStatus[] files = fileSystem.listStatus(new Path("hdfs://nameservice1/"));
//              for (FileStatus file : files) {
//                  System.out.println("NONEKRB:" + file.getPath());
//              }
//             }
//           }
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//        }).start();
    }
}
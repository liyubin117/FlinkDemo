package hadoop.krb;

import hadoop.fs.HDFSUtils;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/** 同时访问 krb、simple 集群 */
public class AuthNoauthKrb {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 初始化认证集群配置文件
        String krbDir =
                System.getProperty("user.dir") + File.separator + "file" + File.separator + "krb";
        System.out.println(krbDir);
        Configuration authConf = HDFSUtils.initConfiguration(krbDir);
        System.out.println(authConf.get("hadoop.security.authentication"));
        authConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //        configuration.set("hadoop.security.authentication", "kerberos");
        // 该线程循环访问认证集群
        new Thread(
                        () -> {
                            try {
                                while (true) {
                                    synchronized ("A") {
                                        // 配置kerberos认证的配置文件
                                        System.setProperty(
                                                "java.security.krb5.conf",
                                                krbDir + File.separator + "krb5.conf");
                                        //
                                        // System.setProperty("sun.security.krb5.debug", "true");
                                        // 进行身份认证
                                        UserGroupInformation.setConfiguration(authConf);
                                        UserGroupInformation.loginUserFromKeytab(
                                                "sloth/dev@BDMS.163.COM",
                                                krbDir + File.separator + "sloth.keytab");
                                        System.out.println(
                                                "当前用户是：" + UserGroupInformation.getCurrentUser());
                                        // 列出根目录下所有文件
                                        FileSystem fileSystem = FileSystem.get(authConf);
                                        FileStatus[] files =
                                                fileSystem.listStatus(
                                                        new Path(
                                                                "hdfs://bdms-test/user/sloth/lyb/hdfs_meta1109"));
                                        for (FileStatus file : files) {
                                            System.out.println("KRB:" + file.getPath());
                                        }
                                        //                        UserGroupInformation.reset();
                                    }
                                    Thread.sleep(1500);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        })
                .start();

        // 初始化非认证配置文件
        String simpleDir =
                System.getProperty("user.dir")
                        + File.separator
                        + "file"
                        + File.separator
                        + "simple";
        Configuration noAuthConf = HDFSUtils.initConfiguration(simpleDir);
        noAuthConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        System.out.println(noAuthConf.get("hadoop.security.authentication"));

        // 该线程循环访问非认证集群
        new Thread(
                        () -> {
                            try {
                                while (true) {
                                    synchronized ("A") {
                                        // 重置认证信息
                                        UserGroupInformation.reset();
                                        UserGroupInformation.setConfiguration(noAuthConf);
                                        System.out.println(
                                                "当前用户是：" + UserGroupInformation.getCurrentUser());
                                        // 列出根目录下所有文件
                                        FileSystem fileSystem = FileSystem.get(noAuthConf);
                                        FileStatus[] files =
                                                fileSystem.listStatus(
                                                        new Path(
                                                                "hdfs://slothTest/user/sloth/lyb"));
                                        for (FileStatus file : files) {
                                            System.out.println("NONEKRB:" + file.getPath());
                                        }
                                    }
                                    Thread.sleep(1000);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        })
                .start();
    }
}

package hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KerberosDemo {
    private static final Logger log = LoggerFactory.getLogger(KerberosDemo.class);

    @Test
    public void setUp() throws IOException {
        initKerberos("config/krb5.conf",
                "liyubin",
                "config/liyubin.keytab");
    }


    public static void initKerberos(String krb5ConfPath, String principal, String keytab) throws IOException {
        System.setProperty("java.security.krb5.conf", krb5ConfPath);
        System.setProperty("sun.security.krb5.debug", "true");
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        log.warn("getting connection from kudu with kerberos");
        log.warn("----------current user: " + UserGroupInformation.getCurrentUser() + "----------");
        log.warn("----------login user: " + UserGroupInformation.getLoginUser() + "----------");
    }
}

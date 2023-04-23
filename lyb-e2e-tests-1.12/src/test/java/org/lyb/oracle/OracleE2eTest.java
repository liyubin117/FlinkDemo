package org.lyb.oracle;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleE2eTest {
    public static final String IMAGE = "gvenzl/oracle-xe:21-slim-faststart";
    private static final Logger LOG = LoggerFactory.getLogger(OracleE2eTest.class);
    private String jdbcUrl;
    private String user;
    private String passwd;
    private String defaultDB;

    @Before
    public void init() {
        OracleContainer oracle = new OracleContainer(IMAGE)
                .withUsername("LYB")
                .withPassword("LYB")
                .withCopyFileToContainer(MountableFile.forClasspathResource("oracle/init.sql"), "/container-entrypoint-startdb.d/init.sql");
        oracle.start();
        jdbcUrl = oracle.getJdbcUrl();
        user = oracle.getUsername();
        passwd = oracle.getPassword();
        defaultDB = oracle.getDatabaseName();

        LOG.info(jdbcUrl);
        LOG.info(user);
        LOG.info(passwd);
        LOG.info(defaultDB);
    }

    @Test
    public void test1() throws ClassNotFoundException, SQLException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection conn = DriverManager.getConnection(
                jdbcUrl,
                user,
                passwd);
        Statement stat = conn.createStatement();
        stat.execute("create user user1 identified by user1");
        stat.execute("GRANT UNLIMITED TABLESPACE TO user1");
        stat.execute("create table user1.t1 (id int, name varchar2(200))");
        stat.execute("insert into user1.t1 values (1, 'one')");
        Assertions.assertThat("lyb").isNotEqualTo(defaultDB);
        ResultSet rs = stat.executeQuery("select id, name from user1.t1");
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                System.out.print(rs.getObject(i + 1) + "\t");
            }
            System.out.println();
        }
    }
}

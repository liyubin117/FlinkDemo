package org.lyb.mysql;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class MysqlE2eTest {
    private static final String DOCKER_IMAGE_NAME = "mysql:5.7.32";
    /** 数据库 */
    private static final String DATABASE = "mine_database";
    /** 连接池类型 */
    private static final Class<?> POOL_TYPE_CLASS = HikariDataSource.class;

    public static MySQLContainer<?> mySqlContainer =
            new MySQLContainer<>(DockerImageName.parse(DOCKER_IMAGE_NAME))
                    .withDatabaseName(DATABASE)
                    // 初始化脚本
                    .withInitScript("mysql/init.sql")
            /// 配置文件
            /// .withConfigurationOverride("mysql/config")
            ;
    private String jdbcUrl;

    @Before
    public void setUp() {
        mySqlContainer.start();

        String jdbcUrl = mySqlContainer.getJdbcUrl();
        int endIndex = jdbcUrl.indexOf("?");
        String additionalSetting = "?characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false";
        if (endIndex < 0) {
            this.jdbcUrl = jdbcUrl + additionalSetting;
        } else {
            this.jdbcUrl = jdbcUrl.substring(0, endIndex) + additionalSetting;
        }
    }

    @Test
    public void test1() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn =
                DriverManager.getConnection(
                        jdbcUrl, mySqlContainer.getUsername(), mySqlContainer.getPassword());

        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select id, name from t1");
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                System.out.print(rs.getObject(i + 1) + "\t");
            }
            System.out.println();
        }
    }
}

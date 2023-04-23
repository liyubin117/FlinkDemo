package org.lyb.db2;

import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.lyb.utils.JdbcDatabaseContainerUtils.performQuery;


public class Db2E2eTest {
    public static final DockerImageName DB2_IMAGE = DockerImageName.parse("ibmcom/db2:11.5.0.0a");

    @Test
    public void testSimple() throws SQLException {
        try (Db2Container db2 = new Db2Container(DB2_IMAGE).acceptLicense()) {
            db2.start();

            ResultSet resultSet = performQuery(db2, "SELECT 1 FROM SYSIBM.SYSDUMMY1");

            int resultSetInt = resultSet.getInt(1);
            assertThat(resultSetInt).as("A basic SELECT query succeeds").isEqualTo(1);
        }
    }

    @Test
    public void testWithAdditionalUrlParamInJdbcUrl() {
        try (
            Db2Container db2 = new Db2Container(DB2_IMAGE)
                .withUrlParam("sslConnection", "false")
                .acceptLicense()
        ) {
            db2.start();

            String jdbcUrl = db2.getJdbcUrl();
            assertThat(jdbcUrl).contains(":sslConnection=false;");
        }
    }

    @Test
    public void testInsertDocker() throws ClassNotFoundException, SQLException {
        ResultSet resultSet;
        try (Db2Container db2 = new Db2Container(DB2_IMAGE).acceptLicense()) {
            db2.start();
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            Connection conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
            Statement statement = conn.createStatement();
            statement.execute(String.format("create table %s.t12 (id int)", db2.getDatabaseName()));
            statement.execute(String.format("insert into %s.t12 values (1)", db2.getDatabaseName()));
            resultSet = statement.executeQuery(String.format("select * from %s.t12", db2.getDatabaseName()));
        }
        resultSet.next();
        Assert.assertEquals(resultSet.getInt(1), 1);
    }
}

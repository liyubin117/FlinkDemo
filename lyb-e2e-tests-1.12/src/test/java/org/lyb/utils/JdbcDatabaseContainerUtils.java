package org.lyb.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

public class JdbcDatabaseContainerUtils {

    public static ResultSet performQuery(JdbcDatabaseContainer<?> container, String sql)
            throws SQLException {
        DataSource ds = getDataSource(container);
        Statement statement = ds.getConnection().createStatement();
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();

        resultSet.next();
        return resultSet;
    }

    public static DataSource getDataSource(JdbcDatabaseContainer<?> container) {
        BasicDataSource bds = new BasicDataSource();
        bds.setUrl(container.getJdbcUrl());
        bds.setUsername(container.getUsername());
        bds.setPassword(container.getPassword());
        bds.setDriverClassName(container.getDriverClassName());
        return bds;
    }
}

package org.gunda.jdbcsqltest;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by shivshi on 5/1/17.
 */
public class JdbcDriver {

    Connection conn = null;

    public JdbcDriver(Properties props) {
        conn = getConnection(props);
    }

    public static Connection getConnection(Properties props) {

        String JDBC_DRIVER = props.getProperty(Config.JDBC_DRIVER_CLASSNAME);
        String JDBC_DB_URL = props.getProperty(Config.JDBC_URL);

        //  Database credentials
        String USER = props.getProperty(Config.JDBC_USER);
        String PASS = props.getProperty(Config.JDBC_PASSWORD);

        Connection conn = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn =  DriverManager.getConnection(JDBC_DB_URL, USER, PASS);

        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load class " + e.getLocalizedMessage());
        } catch (SQLException e) {
            throw new IllegalStateException("Could not connect " + e.getLocalizedMessage());
        }

        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            // Ignore.
        }

        return conn;
    }

    public Connection getConnection() {
        return conn;
    }

    public  void printDBInfo() {

        System.out.println("\nDatabase Info.");
        try {
            DatabaseMetaData dbmeta = conn.getMetaData();
            System.out.println("\tDatabase Product    : " + dbmeta.getDatabaseProductName());
            System.out.println("\tDatabase Version    : " + dbmeta.getDatabaseProductVersion());
            System.out.println("\tJDBC Driver Name    : " + dbmeta.getDriverName());
            System.out.println("\tJDBC Driver Version : " + dbmeta.getDriverVersion());
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot access database metadata " + e.getLocalizedMessage());
        } finally {

        }
        System.out.println();
    }
}

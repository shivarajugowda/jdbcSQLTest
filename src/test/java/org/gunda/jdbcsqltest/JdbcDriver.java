package org.gunda.jdbcsqltest;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by shivshi on 5/1/17.
 */
public class JdbcDriver {

    Connection conn = null;

    public JdbcDriver(Config config) {
        conn = getConnection(config);
    }

    public static Connection getConnection(Config config) {

        String JDBC_DRIVER = config.getStringConfig(Config.JDBC_DRIVER_CLASSNAME);
        String JDBC_DB_URL = config.getStringConfig(Config.JDBC_URL);

        //  Database credentials
        String USER = config.getStringConfig(Config.JDBC_USER);
        String PASS = config.getStringConfig(Config.JDBC_PASSWORD);

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
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                // ignore.
            }
        }
        System.out.println("Database Info Finished.");
    }
}

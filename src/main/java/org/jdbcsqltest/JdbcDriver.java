package org.jdbcsqltest;

import java.sql.*;
import java.util.Properties;

/**
 * Created by shivshi on 5/1/17.
 */
public class JdbcDriver {

    private Connection conn = null;
    private String dbType;
    private static String SCHEMA_NAME = "testschema";
    private Properties props;

    public JdbcDriver(Properties props) throws SQLException {
        this.props = props;
        dbType = props.getProperty(Config.DATABASE);
        conn = getConnection(props);
    }

    public Connection getNewConnection() throws SQLException {
        return getConnection(props);
    }
    private Connection getConnection(Properties props) throws SQLException {

        String JDBC_DRIVER = props.getProperty(Config.JDBC_DRIVER_CLASSNAME);
        String JDBC_DB_URL = props.getProperty(Config.JDBC_URL);

        //  Database credentials
        String USER = props.getProperty(Config.JDBC_USER);
        String PASS = props.getProperty(Config.JDBC_PASSWORD);

        Connection conn = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(JDBC_DB_URL, USER, PASS);
            //conn.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load class " + e.getLocalizedMessage());
        } catch (SQLException e) {
            throw new IllegalStateException("Could not connect " + e.getLocalizedMessage());
        }

        setSchema(conn);
        return conn;
    }

    public Connection getConnection() {
        return conn;
    }

    public String getSchema() throws SQLException {

        if (Config.DATABASE_CIS.equals(dbType))
            return null;

        return SCHEMA_NAME;
    }
    private void setSchema(Connection connection) throws SQLException {

        if (Config.DATABASE_CIS.equals(dbType) || Config.DATABASE_SQL_SERVER.equals(dbType))
            return;

        Statement stmt = connection.createStatement();

        try {
        	stmt.executeUpdate("CREATE SCHEMA " + SCHEMA_NAME);
        } catch (SQLException ex){
            // Ignore if the schema already exists.
        }
               
        if(Config.DATABASE_PGSQL.equals(dbType))
        	stmt.executeUpdate("SET SCHEMA '" + SCHEMA_NAME + "'");         
        else
        	  stmt.executeUpdate("SET SCHEMA " + SCHEMA_NAME);

        stmt.close();
    }

    public void clearSchema() throws SQLException {

        if(Config.DATABASE_CIS.equals(dbType) || Config.DATABASE_DB2.equals(dbType) || Config.DATABASE_SQL_SERVER.equals(dbType))
            return;

        Statement stmt = conn.createStatement();
        try {
            stmt.executeUpdate("DROP SCHEMA " + SCHEMA_NAME + " CASCADE");
        } catch (SQLException ex){
            // Ignore if the schema does not exist.
        }
        stmt.executeUpdate("CREATE SCHEMA " + SCHEMA_NAME);
        stmt.close();

    }

    public void printDBInfo() {

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

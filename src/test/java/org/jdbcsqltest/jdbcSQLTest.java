package org.jdbcsqltest;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.Ignore;

import java.io.File;
import java.util.Properties;

/**
 * Created by shivshi on 5/2/17.
 */
public class jdbcSQLTest {

    public static Properties getDBProperties() throws Exception {
        Properties props = new Properties();

        // HSQLDB DATABASE connection properties.
        String HSQLDB_WORK_FOLDER = "./hsqldb_wk/";
        FileUtils.deleteDirectory(new File(HSQLDB_WORK_FOLDER));

        props.put(Config.DATABASE,               Config.DATABASE_HSQLDB);
        props.put(Config.JDBC_DRIVER_CLASSNAME,  "org.hsqldb.jdbc.JDBCDriver");
        props.put(Config.JDBC_URL,               "jdbc:hsqldb:file:"+ HSQLDB_WORK_FOLDER + ";hsqldb.default_table_type=memory;hsqldb.tx=mvcc;hx.compact_mem=true");
        props.put(Config.JDBC_USER,              "");
        props.put(Config.JDBC_PASSWORD,          "");


        props.put(Config.DATABASE,               Config.DATABASE_CIS);
        props.put(Config.JDBC_DRIVER_CLASSNAME,  "cs.jdbc.driver.CompositeDriver");
        props.put(Config.JDBC_URL,               "jdbc:compositesw:dbapi@localhost:9401?domain=composite&dataSource=testDS&connectTimeout=300");
        props.put(Config.JDBC_USER,              "admin");
        props.put(Config.JDBC_PASSWORD,          "admin");


        props.put(Config.DATABASE,                Config.DATABASE_PGSQL);
        props.put(Config.JDBC_DRIVER_CLASSNAME,  "org.postgresql.Driver");
        props.put(Config.JDBC_URL,               "jdbc:postgresql://localhost:15432/TPCH");
        props.put(Config.JDBC_USER,              "user1");
        props.put(Config.JDBC_PASSWORD,          "user1");

        return props;
    }

    @Ignore
    @Test
    public void testNist() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_NIST);
        props.put(Config.TEST_FOLDER, "./test/nist");
        Main.Main(props);
    }

    @Ignore
    @Test
    public void testSqllogictest() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_SQLLOGICTEST);
        props.put(Config.TEST_FOLDER, "./test/sqllogictest");
        Main.Main(props);
    }

    @Test
    public void testFoodmart() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_FOODMART);
        props.put(Config.TEST_FOLDER, "./test/foodmart");
        props.put(Config.POPULATE_SCHEMA, false);

        Main.Main(props);
    }

    @Test
    public void testTPCH() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_TPCH);
        props.put(Config.TEST_FOLDER, "./test/tpch");
        props.put(Config.POPULATE_SCHEMA, false);

        Main.Main(props);
    }

    @Test
    public void testTPCDS() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_TPCDS);
        props.put(Config.TEST_FOLDER, "./test/tpcds");
        props.put(Config.POPULATE_SCHEMA, false);

        Main.Main(props);
    }
}

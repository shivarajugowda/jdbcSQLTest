package org.jdbcsqltest;

import org.junit.Test;
import org.junit.Ignore;

import java.util.Properties;

/**
 * Created by shivshi on 5/2/17.
 */
public class jdbcSQLTest {

    public static Properties getDBProperties(){
        Properties props = new Properties();

        // DATABASE connection properties.
        props.put(Config.DATABASE,               "HSQLDB");
        props.put(Config.JDBC_DRIVER_CLASSNAME,  "org.hsqldb.jdbc.JDBCDriver");
        //props.put(Config.JDBC_URL,             "jdbc:hsqldb:file:./;hsqldb.default_table_type=cached;hsqldb.tx=mvcc;hsqldb.cache_size=6000000;shutdown=true");
        props.put(Config.JDBC_URL,               "jdbc:hsqldb:file:./;hsqldb.default_table_type=memory;hsqldb.tx=mvcc;hx.compact_mem=true");
        props.put(Config.JDBC_USER,              "");
        props.put(Config.JDBC_PASSWORD,          "");

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

    @Test
    public void testSqllogictest() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_SQLLOGICTEST);
        props.put(Config.TEST_FOLDER, "./test/sqllogictest");
        Main.Main(props);
    }
}

package org.jdbcsqltest;

/**
 * Created by shivshi on 5/1/17.
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class Config {

    // Database
    public static final String DATABASE = "DATABASE";

    public static final String DATABASE_HSQLDB = "HSQLDB";
    public static final String DATABASE_PGSQL  = "PostgreSQL";
    public static final String DATABASE_CIS    = "CIS";
    public static final String DATABASE_DB2    = "DB2";
    public static final String DATABASE_ORACLE = "Oracle";
    public static final String DATABASE_SQL_SERVER = "SQLServer";

    // The remaining properties come from config file.
    public static final String DATA_DIR = "data.dir";

    // JDBC driver properties
    public static final String JDBC_DRIVER_CLASSNAME = "jdbc.driver.classname";
    public static final String JDBC_URL = "jdbc.url";
    public static final String JDBC_USER = "jdbc.user";
    public static final String JDBC_PASSWORD = "jdbc.password";

    // Test details.
    public static final String TEST_TYPE = "test.type";
    public static final String RESOURCES_FOLDER = "resources.folder";

    public static final String TEST_TYPE_NIST = "test.type.nist";
    public static final String TEST_TYPE_SQLLOGICTEST = "test.type.sqllogictest";
    public static final String TEST_TYPE_FOODMART = "test.type.foodmart";
    public static final String TEST_TYPE_TPCH = "test.type.tpch";
    public static final String TEST_TYPE_TPCDS = "test.type.tpcds";

    public static final String POPULATE_SCHEMA = "populate.schema";  // Should populate schema, default true.

    // Queries
    public static final String QUERY_FILE = "query.file";
    public static final String QUERY_RESULTS_FOLDER = "query.results.folder";
    public static final String QUERY_RESULT_CHECK_FP_DELTA = "query.resultCheck.FP.delta";
    private static Config instance = null;

    // SCALE_FACTOR for TPCH and TPC-DS tests
    public static final String SCALE_FACTOR = "SF";
    public static final String SCALE_FACTOR_1 = "SF_1";
    public static final String SCALE_FACTOR_0_01 = "SF_0_01";

    private Properties props;

    protected Config(String path) {
        props = readConfig(path);
    }

    public static void Initialize(String configFile) {
        instance = new Config(configFile);
    }

    public static Config getInstance() {

        if (instance == null)
            throw new IllegalStateException("Config is not initialized");

        return instance;
    }

    private static Properties readConfig(String configFile) {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(configFile));
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't find  " + e.getLocalizedMessage());
        }

        return props;
    }

    public String getStringConfig(String key) {
        return props.getProperty(key);
    }

    public String getStringConfig(String key, String deflt) {
        return props.getProperty(key, deflt);
    }

    public int getIntConfig(String key, int def) {
        String value = props.getProperty(key);
        if (value == null || value.trim().isEmpty())
            return def;

        int valInt = Integer.valueOf(value.trim());

        return valInt;
    }

    public float getFloatConfig(String key, float def) {
        String value = props.getProperty(key);
        if (value == null || value.trim().isEmpty())
            return def;

        float valFloat = Float.valueOf(value.trim());

        return valFloat;
    }

    public boolean getBooleanConfig(String key, boolean def) {
        String value = props.getProperty(key);
        if (value == null || value.trim().isEmpty())
            return def;

        boolean valInt = Boolean.valueOf(value.trim());

        return valInt;
    }

    @SuppressWarnings("unchecked")
    public void printConfig() {

        System.out.println("\nConfg Settings.");

        List keys = new ArrayList();
        keys.addAll(props.keySet());
        Collections.sort(keys);

        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            String value;
            if (key.contains("password"))
                value = "*****";
            else
                value = props.getProperty(key);
            System.out.println("\t" + key + " => " + value);
        }
        System.out.println("Config Settings Finished.");
    }
}

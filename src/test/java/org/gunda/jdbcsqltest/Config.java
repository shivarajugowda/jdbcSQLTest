package org.gunda.jdbcsqltest;

/**
 * Created by shivshi on 5/1/17.
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Config {

    // Database
    public static final String DATABASE = "DATABASE";

    // The remaining properties come from config file.
    public static final String DATA_DIR = "data.dir";

    // JDBC driver properties
    public static final String JDBC_DRIVER_CLASSNAME = "jdbc.driver.classname";
    public static final String JDBC_URL = "jdbc.url";
    public static final String JDBC_USER = "jdbc.user";
    public static final String JDBC_PASSWORD = "jdbc.password";

    // Queries
    public static final String QUERY_FILE = "query.file";
    public static final String QUERY_RESULTS_FOLDER = "query.results.folder";
    public static final String QUERY_RESULT_CHECK_FP_DELTA = "query.resultCheck.FP.delta";
    private static Config instance = null;

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

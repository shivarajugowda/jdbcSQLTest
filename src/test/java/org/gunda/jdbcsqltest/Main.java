package org.gunda.jdbcsqltest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;


public class Main {

    private static final String CONFIG_FILE = "config.file";
    private static final String EXECUTE_QUERIES_ONLY = "execute.queries.only";
    private static final String CLEAN_DB_ONLY = "clean.db.only";


    public static void Main(Properties props) throws Exception {

        long startTime = System.currentTimeMillis();

        JdbcDriver jdbcDriver = new JdbcDriver(props);
        jdbcDriver.printDBInfo();

        File testFolder = new File(props.getProperty(Config.TEST_FOLDER));

        // Run Schema Scripts
        File schemaFolder = new File(testFolder, "schema");
        List<File> files = (List<File>) FileUtils.listFiles(schemaFolder, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        for (File file : files) {
            Script script = new SchemaScript(file);
            TestExecutor ex = new TestExecutor(jdbcDriver, script);
            ex.execute();
        }

        // Run SQL Scripts
        File sqlFolder = new File(testFolder, "test");
        files = (List<File>) FileUtils.listFiles(sqlFolder, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        for (File file : files) {
            Script script = new SqlScript(file);
            TestExecutor ex = new TestExecutor(jdbcDriver, script);
            ex.execute();
        }

        System.out.println("\nTest ended, Total time taken = " + (System.currentTimeMillis() - startTime) + " ms ");
    }
}


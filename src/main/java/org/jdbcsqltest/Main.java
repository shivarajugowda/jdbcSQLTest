package org.jdbcsqltest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.jdbcsqltest.nist.SchemaScript;
import org.jdbcsqltest.nist.SqlScript;
import org.jdbcsqltest.sqllogictest.SltScript;

import java.io.File;
import java.util.List;
import java.util.Properties;


public class Main {

    public static void Main(Properties props) throws Exception {

        long startTime = System.currentTimeMillis();

        JdbcDriver jdbcDriver = new JdbcDriver(props);
        jdbcDriver.printDBInfo();

        String testType = props.getProperty(Config.TEST_TYPE);

        if (Config.TEST_TYPE_NIST.equals(testType))
            testNist(jdbcDriver, props);
        else if (Config.TEST_TYPE_SQLLOGICTEST.equals(testType))
            testSqlLogicTest(jdbcDriver, props);

        jdbcDriver.getConnection().close();
        System.out.println("\nTest ended, Total time taken = " + (System.currentTimeMillis() - startTime) + " ms ");
    }

    public static void testNist(JdbcDriver jdbcDriver, Properties props) throws Exception {
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
    }

    public static void testSqlLogicTest(JdbcDriver jdbcDriver, Properties props) throws Exception {
        File testFolder = new File(props.getProperty(Config.TEST_FOLDER));

        // Run SQL Scripts
        List<File> files = (List<File>) FileUtils.listFiles(testFolder, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        for (File file : files) {
            jdbcDriver.clearSchema();
            Script script = new SltScript(file);
            TestExecutor ex = new TestExecutor(jdbcDriver, script);
            ex.execute();

        }
    }
}


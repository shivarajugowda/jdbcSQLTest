package org.jdbcsqltest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
//import org.jdbcsqltest.foodmart.FoodmartScript;
import org.jdbcsqltest.foodmart.FoodmartSchemaScript;
import org.jdbcsqltest.foodmart.FoodmartSqlScript;
import org.jdbcsqltest.nist.SchemaScript;
import org.jdbcsqltest.nist.SqlScript;
import org.jdbcsqltest.sqllogictest.SltScript;

import java.io.File;
import java.util.ArrayList;
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
        else if (Config.TEST_TYPE_FOODMART.equals(testType))
            testFoodmart(jdbcDriver, props);

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
        List<File> files = new ArrayList<File>();
        if(testFolder.isDirectory())
            files = (List<File>) FileUtils.listFiles(testFolder, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        else
            files.add(testFolder);


        for (File file : files) {
            jdbcDriver.clearSchema();
            String name = testFolder.isDirectory() ?
                            file.getAbsolutePath().substring(testFolder.getAbsolutePath().length() + 1) :
                            file.getName();
            Script script = new SltScript(name, file);
            TestExecutor ex = new TestExecutor(jdbcDriver, script);
            ex.execute();
        }
    }

    public static void testFoodmart(JdbcDriver jdbcDriver, Properties props) throws Exception {
        File testFolder = new File(props.getProperty(Config.TEST_FOLDER));

        // Clear database.
        jdbcDriver.clearSchema();

        // Run Schema Scripts
        File schemaFile = new File(testFolder, "foodmart.script.zip");
        Script script = new FoodmartSchemaScript(schemaFile);
        TestExecutor ex = new TestExecutor(jdbcDriver, script);
        ex.execute();

        // Run SQL Scripts
        File sqlFile = new File(testFolder, "queries.json.zip");
        script = new FoodmartSqlScript(sqlFile);
        ex = new TestExecutor(jdbcDriver, script);
        ex.execute();
    }
}


package org.jdbcsqltest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
//import org.jdbcsqltest.foodmart.FoodmartScript;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.jdbcsqltest.foodmart.FoodmartSchemaScript;
import org.jdbcsqltest.foodmart.FoodmartSqlScript;
import org.jdbcsqltest.nist.SchemaScript;
import org.jdbcsqltest.nist.SqlScript;
import org.jdbcsqltest.sqllogictest.SltScript;
import org.jdbcsqltest.tpcds.TpcdsPopulateData;
import org.jdbcsqltest.tpcds.TpcdsScript;
import org.jdbcsqltest.tpcds.TpcdsScript_bak;
import org.jdbcsqltest.tpch.TpchPopulateData;
import org.jdbcsqltest.tpch.TpchScript;

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
        else if (Config.TEST_TYPE_TPCH.equals(testType))
            testTPCH(jdbcDriver, props);
        else if (Config.TEST_TYPE_TPCDS.equals(testType))
            testTPCDS(jdbcDriver, props);

        jdbcDriver.getConnection().close();
        System.out.println("\nTest ended, Total time taken = " + (System.currentTimeMillis() - startTime) + " ms ");
    }

    public static void testNist(JdbcDriver jdbcDriver, Properties props) throws Exception {
        File testFolder = new File(props.getProperty(Config.RESOURCES_FOLDER));


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
        File testFolder = new File(props.getProperty(Config.RESOURCES_FOLDER));

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
        File testFolder = new File(props.getProperty(Config.RESOURCES_FOLDER));


        // Run Schema Scripts
        Boolean popSchema = (Boolean)props.get(Config.POPULATE_SCHEMA);
        popSchema = popSchema == null ? true : popSchema; // default true.
        if(popSchema) {
            jdbcDriver.clearSchema();  // Clear database.
            File schemaFile = new File(testFolder, "foodmart.script.zip");
            Script script = new FoodmartSchemaScript(schemaFile);
            TestExecutor ex = new TestExecutor(jdbcDriver, script);
            ex.execute();
            jdbcDriver.getConnection().commit();
        }

        // Run SQL Scripts
        File sqlFile = new File(testFolder, "queries.json.zip");
        Script sqlScript = new FoodmartSqlScript(sqlFile);
        TestExecutor exsql = new TestExecutor(jdbcDriver, sqlScript);
        exsql.execute();
    }

    public static void testTPCH(JdbcDriver jdbcDriver, Properties props) throws Exception {
        File testFolder = new File(props.getProperty(Config.RESOURCES_FOLDER));

        String sf = props.getProperty(Config.SCALE_FACTOR);
        sf = sf == null ? Config.SCALE_FACTOR_0_01 : sf;
        
        // Run Schema Scripts
        Boolean popSchema = (Boolean)props.get(Config.POPULATE_SCHEMA);
        popSchema = popSchema == null ? true : popSchema; // default true.
        if(popSchema) {
            jdbcDriver.clearSchema();  // Clear database.
            File schemaFolder = new File(testFolder, "schema");
            
            // Create Tables.
            Script script = new SchemaScript(new File(schemaFolder, "CreateTables.sql"));
            TestExecutor ex = new TestExecutor(jdbcDriver, script);
            ex.execute();
            
            // Populate Data.
            TpchPopulateData popd = new TpchPopulateData(jdbcDriver, sf);
            popd.execute();
            
            // Create Indexes and other constraints.
            script = new SchemaScript(new File(schemaFolder, "CreateIndexes.sql"));
            ex = new TestExecutor(jdbcDriver, script);
            ex.execute();
        }

        // Run SQL Scripts
        File sqlFolder = new File(testFolder, "queries");
        List<File> files = (List<File>) FileUtils.listFiles(sqlFolder, new WildcardFileFilter("*.sql"), TrueFileFilter.INSTANCE);
        for (File file : files) {
            TpchScript script = new TpchScript(file, sf);
            TestExecutor ex = new TestExecutor(jdbcDriver, script);
            ex.execute();
        }

    }

    public static void testTPCDS(JdbcDriver jdbcDriver, Properties props) throws Exception {
        File testFolder = new File(props.getProperty(Config.RESOURCES_FOLDER));

        // Run Schema Scripts
        Boolean popSchema = (Boolean)props.get(Config.POPULATE_SCHEMA);
        popSchema = popSchema == null ? true : popSchema; // default true.
        if(popSchema) {
            jdbcDriver.clearSchema();  // Clear database.
            File schemaFolder = new File(testFolder, "schema");
            List<File> files = (List<File>) FileUtils.listFiles(schemaFolder, new WildcardFileFilter("*.sql"), TrueFileFilter.INSTANCE);
            for (File file : files) {
                Script script = new SchemaScript(file);
                TestExecutor ex = new TestExecutor(jdbcDriver, script);
                ex.execute();
            }
            TpcdsPopulateData ex = new TpcdsPopulateData(jdbcDriver);
            ex.execute();
        }

        // Run SQL Scripts
        String sf = props.getProperty(Config.SCALE_FACTOR);
        sf = sf == null ? Config.SCALE_FACTOR_1 : sf;

        File sqlFolder = new File(testFolder, "test");
        List<File> files = (List<File>) FileUtils.listFiles(sqlFolder, new WildcardFileFilter("*.sql"), TrueFileFilter.INSTANCE);
        for (File file : files) {
            //System.out.println("Working on " + file.getName());
            TpcdsScript script = new TpcdsScript(file, sf);
            TestExecutor ex = new TestExecutor(jdbcDriver, script);
            ex.execute();
        }

        /*
        TpcdsScript_bak script = new TpcdsScript_bak("TPCDS");
        TestExecutor ex = new TestExecutor(jdbcDriver, script);
        ex.execute();
        */
    }
}


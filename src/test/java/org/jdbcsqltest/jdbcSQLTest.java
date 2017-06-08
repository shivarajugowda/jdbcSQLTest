package org.jdbcsqltest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.Test;
import org.junit.Ignore;

import java.io.File;
import java.util.Collection;
import java.util.List;
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
        
 
       

        return props;
    }

    @Ignore
    @Test
    public void testNist() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_NIST);
        props.put(Config.RESOURCES_FOLDER, "./resources/nist");
        Main.Main(props);
    }

    @Ignore
    @Test
    public void testSqllogictest() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_SQLLOGICTEST);
        props.put(Config.RESOURCES_FOLDER, "./resources/sqllogictest");
        Main.Main(props);
    }

    @Test
    public void testFoodmart() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_FOODMART);
        props.put(Config.RESOURCES_FOLDER, "./resources/foodmart");
        props.put(Config.POPULATE_SCHEMA, false);

        Main.Main(props);
    }

    @Test
    public void testTPCH() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_TPCH);
        props.put(Config.RESOURCES_FOLDER, "./resources/tpch");
        props.put(Config.SCALE_FACTOR, Config.SCALE_FACTOR_0_01);
        props.put(Config.POPULATE_SCHEMA, true);

        Main.Main(props);
    }

    @Test
    public void testTPCDS() throws Exception {
        Properties props = getDBProperties();

        // Test type.
        props.put(Config.TEST_TYPE,   Config.TEST_TYPE_TPCDS);
        props.put(Config.RESOURCES_FOLDER, "./resources/tpcds");
        props.put(Config.SCALE_FACTOR, Config.SCALE_FACTOR_0_01);
        props.put(Config.POPULATE_SCHEMA, false);

        Main.Main(props);
    }

    /*
    @Test
    public void temp() throws Exception {

        File testFolder = new File("./resources/tpch/queries");
        Collection<File> files = FileUtils.listFiles(testFolder, new WildcardFileFilter("*.tpl"), TrueFileFilter.INSTANCE);

        // Test type.
        for (File file : files) {
            String newname = FilenameUtils.getBaseName(file.getName()) + ".tpl";
            System.out.println("Working on " + file.getName() + " new name = " + newname);
            FileUtils.moveFile(file, new File(file.getParentFile(), newname));
            String sql = FileUtils.readFileToString(file, "UTF-8");
            sql = sql.replace(";", "");
            FileUtils.write(file, sql);
        }
    }
   */ 
    
    @Test
    public void temp() throws Exception {

    	String sql = "-- hello world \r\n--dkdkd\n create table";
    	String modsql = sql.replaceAll("--.*\r?\n","");
    	System.out.println("Orig : "+ sql);
    	System.out.println("Mod  : "+ modsql);
    }

}

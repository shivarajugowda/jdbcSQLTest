package org.jdbcsqltest;

import org.jdbcsqltest.Config;
import org.jdbcsqltest.JdbcDriver;
import org.jdbcsqltest.Script;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shivshi on 5/2/17.
 */
public class TestExecutor {

    Config config;
    JdbcDriver jdbcDriver;
    Script script;

    private float FLOATING_POINT_DELTA = 0.01f;


    private int ntests = 0, npassed = 0, nfailed = 0, nvalidated = 0;

    public TestExecutor(JdbcDriver conn, Script script) throws IOException {
        jdbcDriver = conn;
        this.script = script;
    }

    public void execute() throws SQLException {
        long totalTimeStart = System.currentTimeMillis();;

        String sql = script.getNextSQLCommand();

        while ( sql != null) {
            long startTime = System.currentTimeMillis();
            ntests++;
            Statement stmt = null;
            ResultSet rs = null;
            if(sql.startsWith("COMMIT")){
                jdbcDriver.getConnection().commit();
            } else if (sql.startsWith("ROLLBACK") ) {
                jdbcDriver.getConnection().rollback();
            } else{
                try {
                    stmt = jdbcDriver.getConnection().createStatement();

                    int nrows = 0;
                    if(script.isDML(sql)) {
                        nrows = stmt.executeUpdate(sql);
                    } else {
                        rs = stmt.executeQuery(sql);
                    }
                    if (script.validateResults(rs, nrows))
                        nvalidated++;
                    npassed++;
                } catch (Exception e) {
                    System.out.println("Failed to execute sql : " + sql + "\n" + e.getLocalizedMessage());
                    nfailed++;
                } finally {
                    if (stmt != null)
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                        }
                }
            }

            sql = script.getNextSQLCommand();
        }
        System.out.println( script.getName() + "\t: " +
                "\t#Tests = " + ntests +
                "\t#Passed = " + npassed +
                "\t#Failed = " + nfailed +
                "\t#Validated = " + nvalidated +
                "\tTime(ms) " + (System.currentTimeMillis() - totalTimeStart));

    }

}

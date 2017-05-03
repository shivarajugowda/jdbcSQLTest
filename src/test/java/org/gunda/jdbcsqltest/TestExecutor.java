package org.gunda.jdbcsqltest;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
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

    // Pattern to validate : "-- PASS:0052 If CITY = 'Xi_an% ss' ?";
    Pattern STRING_PATTERN = Pattern.compile("--\\s+PASS:(\\w+)\\s+if\\s+(\\w+)\\s+=\\s+'(.*)'\\s?\\?", Pattern.CASE_INSENSITIVE);

    // Pattern to validate : "-- PASS:0045 If PNUM = 20 ?" and "-- PASS:Setup if count = 6?"
    Pattern NUM_PATTERN = Pattern.compile("--\\s+PASS:(\\w+)\\s+if\\s+(.*)\\s+=\\s+(\\w+)\\s?\\?", Pattern.CASE_INSENSITIVE);

    // Pattern to validate : "-- PASS:0052 If 1 row is inserted?"
    Pattern NUM_ROWS_PATTERN = Pattern.compile("--\\s+PASS:(\\w+)\\s+if\\s+(\\w+)\\s+rows?\\s+[ia][sr]e?\\s+(\\w+).*\\?", Pattern.CASE_INSENSITIVE);

    private int ntests = 0, npassed = 0, nvalidated = 0;

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
                    rs = stmt.executeQuery(sql);
                    if(validateResults(rs, script.getValidationClause()))
                        nvalidated++;
                    npassed++;
                } catch (Exception e) {
                    System.out.println("Failed to execute sql : " + sql + "\n" + e.getLocalizedMessage());
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
                "\t#Validated = " + nvalidated +
                "\tTime(ms) " + (System.currentTimeMillis() - totalTimeStart));

    }

    private boolean validateResults(ResultSet rs, String valClause) throws SQLException {
        if(valClause == null || valClause.isEmpty())
            return false;

        Matcher macther = NUM_PATTERN.matcher(valClause);
        if (macther.find()) {
            String testName = macther.group(1);
            String colName  = macther.group(2);
            String value    = macther.group(3);

            rs.next();
            String result = rs.getString(1);
            if(!value.trim().equals(result.trim()))
                throw new IllegalStateException("Validation failed for " + testName + ". For column " + colName + " Expected = '" + value + "' Actual = '" + result + "'");

            return true;
        }

        macther = STRING_PATTERN.matcher(valClause);
        if (macther.find()) {
            String testName = macther.group(1);
            String colName  = macther.group(2);
            String value    = macther.group(3);

            rs.next();
            String result = rs.getString(1);
            if(!value.trim().equals(result.trim()))
                throw new IllegalStateException("Validation failed for " + testName + ". For column " + colName + " Expected = '" + value + "' Actual = '" + result + "'");

            return true;
        }

        macther = NUM_ROWS_PATTERN.matcher(valClause);
        if (macther.find()) {
            String testName = macther.group(1);
            String numRows  = macther.group(2);
            String oper     = macther.group(3);

            int rows = 0;
            if("SELECTED".equalsIgnoreCase(oper)) {
                while (rs.next()) {
                    rows++;
                }
                if (!String.valueOf(rows).equals(numRows.trim()))
                    throw new IllegalStateException("Validation failed for " + testName + ". Expected = " + numRows + "Actual = " + rows);
                return true;
            }
        }


        System.out.println("Unknown validation clause : " + valClause );
        return  false;
    }
}

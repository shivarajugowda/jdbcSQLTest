package org.jdbcsqltest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

        Script.SqlCommand cmd = script.getNextSQLCommand();
        Connection conn = jdbcDriver.getConnection();
        StringBuilder failedCases = new StringBuilder();

        while ( cmd != null) {
            long startTime = System.currentTimeMillis();
            ntests++;
            Statement stmt = null;
            ResultSet rs = null;

            if(cmd.sql.startsWith("COMMIT")){
                conn.commit();
            } else if (cmd.sql.startsWith("ROLLBACK") ) {
                conn.rollback();
            } else{
                try {
                    long queryStartTime = System.currentTimeMillis();
                    stmt = conn.createStatement();
                    int nrows = 0;
                    if(script.isDDL(cmd.sql)) {
                        nrows = stmt.executeUpdate(cmd.sql);
                    } else {
                        rs = stmt.executeQuery(cmd.sql);
                    }
                    if (script.validateResults(rs, nrows))
                        nvalidated++;
                    npassed++;

                    if(rs != null)
                        rs.close();

                    //System.out.println("Passed " + cmd.name + ", Time taken : " + (System.currentTimeMillis() - queryStartTime));
                } catch (Throwable e) {
                    nfailed++;
                    //System.out.println("Failed to execute id = " + cmd.name + ", sql = " + cmd.sql + "\n" + e);
                    //failedCases.append(cmd.name).append(",");
                   // throw new IllegalStateException(e);
                    //break;
                } finally {
                    if (stmt != null)
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                        }
                }
            }

            cmd = script.getNextSQLCommand();
        }
        //System.out.println("Failed cases : " + failedCases.toString());
        System.out.format("%-35s %1s %-15s %-17s %-17s %-20s %-15s \n",
                script.getName(),
                ":",
                "#Queries = " + ntests,
                "#Passed = " + npassed,
                "#Failed = " + nfailed,
                "#Validated = " + nvalidated,
                "Time(ms) " + (System.currentTimeMillis() - totalTimeStart));

    }

}

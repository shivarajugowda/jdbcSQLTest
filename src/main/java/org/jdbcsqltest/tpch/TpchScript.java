package org.jdbcsqltest.tpch;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.jdbcsqltest.Script;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

/**
 * Created by shivshi on 5/24/17.
 */
public class TpchScript extends Script {
    private float FLOATING_POINT_DELTA = 0.05f;
    public String sql;
    private String resultFile;
    int nextPtr = 0;

    public TpchScript(File file) throws Exception {
        super(file.getName());
        sql = FileUtils.readFileToString(file, "UTF-8");
        resultFile = FilenameUtils.removeExtension(file.getAbsolutePath()) + ".out";
    }

    public SqlCommand getNextSQLCommand() {
        if (nextPtr > 0)
            return null;

        nextPtr++;
        return new SqlCommand(this.getName(), sql);
    }

    public boolean validateResults(ResultSet rs, int nrows) throws Exception {
        if (nextPtr > 1)
            return false;

        ResultSetMetaData rsmeta = rs.getMetaData();
        BufferedReader dataReader = new BufferedReader(new FileReader(resultFile));
        String s = dataReader.readLine();
        String[] f = s.split("\\|");
        String resultMatchError = null;

        if (f.length != rsmeta.getColumnCount()) {
            throw new IllegalStateException("Result validation : FAILED. Number of Columns DO NOT MATCH. Expected=" + f.length + " Actual=" + rsmeta.getColumnCount());
        }

        while (rs.next()) {
            s = dataReader.readLine();
            if (s == null || s.trim().isEmpty()) {
                throw new IllegalStateException("Result validation : FAILED. Number of Actual Rows  more than Expected rows");
            }

            if (resultMatchError == null) {
                String[] tokens = s.split("\\|");
                for (int i = 0; i < rsmeta.getColumnCount(); i++) {
                    if (!checkResult(rsmeta.getColumnType(i + 1), rs.getString(i + 1), tokens[i])) {
                        resultMatchError = "Column " + rsmeta.getColumnName(i + 1) + ". DataType " + rsmeta.getColumnTypeName(i + 1) + " " + rsmeta.getColumnType(i + 1) + ". Value " + rs.getString(i + 1) + " != " + tokens[i].trim();
                        break;
                    }

                }
            }
        }

        if (((s = dataReader.readLine()) != null) && (!s.trim().isEmpty())) {
            throw new IllegalStateException("Result validation : FAILED. Number of Expected Rows  more than Actual rows");
        }

        if(resultMatchError != null)
            throw new IllegalStateException("Result validation : Number of Rows match but some values don't match : " + resultMatchError);
        dataReader.close();
        return true;
    }

    private boolean checkResult(int type, String expected, String actual) {
        boolean resultsMatch = false;

        // Check null condition, actual is always not null because of string.split().
        if(expected == null && actual.isEmpty())
            return true;
        else if (expected == null && ! actual.isEmpty() )
            return false;

        switch(type){
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.FLOAT:
                Double dblExp = new Double(expected);
                Double dblAct = new Double(actual);
                resultsMatch = Math.abs(dblExp - dblAct) < FLOATING_POINT_DELTA;
                break;
            default:
                resultsMatch = expected.trim().equals(actual.trim());

        }

        return resultsMatch;
    }

}

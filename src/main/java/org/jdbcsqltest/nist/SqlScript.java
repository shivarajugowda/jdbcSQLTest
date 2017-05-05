package org.jdbcsqltest.nist;

import org.apache.commons.io.FileUtils;
import org.jdbcsqltest.Script;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shivshi on 5/2/17.
 */
public class SqlScript extends Script {
    public int nextPtr = 0;
    List<String> sqls = new ArrayList<String>();
    List<String> validation = new ArrayList<String>();

    // Pattern to validate : "-- PASS:0052 If CITY = 'Xi_an% ss' ?";
    Pattern STRING_PATTERN = Pattern.compile("--\\s+PASS:(\\w+)\\s+if\\s+(\\S+)\\s+=\\s+'(.*)'\\s?\\?", Pattern.CASE_INSENSITIVE);

    // Pattern to validate : "-- PASS:0045 If PNUM = 20 ?" and "-- PASS:Setup if count = 6?"
    Pattern NUM_PATTERN = Pattern.compile("--\\s+PASS:(\\w+)\\s+if\\s+(\\S*)\\s+=\\s+(\\w+)\\s?\\?", Pattern.CASE_INSENSITIVE);

    // Pattern to validate : "-- PASS:0052 If 1 row is inserted?"
    Pattern NUM_ROWS_PATTERN = Pattern.compile("--\\s+PASS:(\\w+)\\s+if\\s+(\\w+)\\s+rows?\\s+(?:is\\s+|are\\s+)?(\\w+).*\\?", Pattern.CASE_INSENSITIVE);

    public SqlScript(File file) throws IOException {
        super(file.getName());
        List<String> lines = FileUtils.readLines(file, "UTF-8");
        String sql = "";
        for(int i=0; i < lines.size();i++) {
            String sl = lines.get(i);
            if(isComment(sl))
                continue;
            if(sl.trim().endsWith(";")) {
                sql += sl.trim();
                sqls.add(sql);
                sql = "";
                if( hasValidationLine(lines, i) )
                    validation.add(lines.get(++i));
                else
                    validation.add(null);
            } else {
                sql += sl.trim() + ' ';
            }
        }
    }

    private boolean isComment(String line){
        return line.trim().startsWith("--");
    }

    private boolean hasValidationLine(List<String> lines, int lineNo){
        if(lineNo >= lines.size())
            return false;

        return lines.get(lineNo+1).trim().startsWith("-- PASS");
    }

    public String getNextSQLCommand(){
        if(nextPtr < sqls.size())
            return sqls.get(nextPtr++);

        return null;
    }

    private String getValidationClause(){
        return validation.get(nextPtr - 1);
    }

    public boolean validateResults(ResultSet rs, int nrows) throws SQLException {
        String valClause = this.getValidationClause();
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

            if("SELECTED".equalsIgnoreCase(oper)) {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                if (!String.valueOf(rows).equals(numRows.trim()))
                    throw new IllegalStateException("Validation failed for " + testName + ". Expected = " + numRows + "Actual = " + rows);
                return true;
            } else if("INSERTED".equalsIgnoreCase(oper) || "DELETED".equalsIgnoreCase(oper)) {
                if (!String.valueOf(nrows).equals(numRows.trim()))
                    throw new IllegalStateException("Validation failed for " + testName + ". Expected = " + numRows + "Actual = " + nrows);
                return true;
            }
        }

        System.out.println("Unknown validation clause : " + valClause );
        return  false;
    }

}

package org.jdbcsqltest;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by shivshi on 5/3/17.
 */
public class Script {
    static float FLOATING_POINT_DELTA = 0.05f;

    public class SqlCommand{
        public String name;
        public String sql;
        public SqlCommand(String name, String sql){
            this.name = name;
            this.sql = sql;
        }
    }

    public String name;

    public Script(String name){
        this.name = name;
    }

    public SqlCommand getNextSQLCommand() {
        return null;
    }

    public String getLine() {
        return null;
    }

    public String getName(){
        return name;
    }

    public boolean validateResults(ResultSet rs, int nrows) throws Exception {
        return false;
    }

    public boolean isDDL(String sql){
        String lcsql = sql.toLowerCase().trim();
        if( lcsql.startsWith("create") ||
                lcsql.startsWith("alter") ||
                lcsql.startsWith("drop") ||
                lcsql.startsWith("insert") ||
                lcsql.startsWith("delete") ||
                lcsql.startsWith("update") )
            return true;
        return false;
    }

    /*
     * Given a test file and a scalefactor, returns the location of the result file.
     */
    public static String getResultFile(File file, String sf) {
        File parent = new File(file.getParent(), "answers_" + sf);
        String baseName = FilenameUtils.removeExtension(file.getName()) + ".ans";
        File resultFile = new File(parent, baseName);
        return resultFile.getAbsolutePath();
    }

    public static boolean checkResult(int type, String expected, String actual, float FLOATING_POINT_DELTA) {
        boolean resultsMatch = false;

        // Check null condition, actual is always not null because of string.split().
        if(expected == null && (actual == null || actual.isEmpty()))
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

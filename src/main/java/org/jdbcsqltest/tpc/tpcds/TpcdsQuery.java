package org.jdbcsqltest.tpc.tpcds;

import net.hydromatic.tpcds.query.Query;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.jdbcsqltest.Script;
import org.jdbcsqltest.tpc.TpcQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by shivshi on 5/25/17.
 */
public class TpcdsQuery extends TpcQuery {
    private float FLOATING_POINT_DELTA = 0.5f;
    int nextPtr = 0;

    private static final String[] DISABLED_IDS = {
       // ROLLUP.
       "q05", "q14", "q18", "q22", "q27", "q36", "q67", "q70", "q77", "q80", "q86",

       // Incorrect results
       //"q30", "q78",

       // TOO Slow or haven't finished.
       "q01", "q04", "q06", "q10", "q11", "q35", "q47", "q57", "q74", "q81",

       // 100+ secs.
       //"q95",

    };
    public static final Set<String> DISABLED_IDS_SET = new HashSet<String>(Arrays.asList(DISABLED_IDS));

    public TpcdsQuery(File file, String sf, String dbType) throws Exception {
        super(file, sf, dbType);
    }

    public SqlCommand getNextSQLCommand() {
        if (nextPtr > 0 || DISABLED_IDS_SET.contains(this.getName()))
            return null;

        /*
        String numStr = this.getName().substring(1,3);
        int num = Integer.valueOf(numStr);
        if (num != 35)
            return null;
         */
        
        nextPtr++;
        return new SqlCommand(this.getName(), sql);
    }

    public boolean validateResults_ORIG(ResultSet rs, int nrows) throws Exception {

        if(!resultFile.exists())
            return false;

        ResultSetMetaData rsmeta = rs.getMetaData();
        BufferedReader dataReader = new BufferedReader(new FileReader(resultFile));
        String s = dataReader.readLine(); // Ignore column Headers.
        s = dataReader.readLine(); // For demarcator "----- ---- ---"
        String[] header = s.trim().split("\\s");
        String resultMatchError = null;

        if (header.length != rsmeta.getColumnCount()) {
            throw new IllegalStateException("Result validation : FAILED. Number of Columns DO NOT MATCH. Expected=" + header.length + " Actual=" + rsmeta.getColumnCount());
        }

        while (rs.next()) {
            s = dataReader.readLine();
            if (s == null || s.trim().isEmpty()) {
                throw new IllegalStateException("Result validation : FAILED. Number of Actual Rows  more than Expected rows");
            }

            if (resultMatchError == null) {
                String[] tokens = splitLine(header, s);
                for (int i = 0; i < rsmeta.getColumnCount(); i++) {
                	String expected = tokens[i];
                	int colType = rsmeta.getColumnType(i + 1);
                	Object actual = rs.getObject(i+1);
                    if (!checkResult(colType, expected, actual, FLOATING_POINT_DELTA)) {
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

    /*
     * Split line based on header.
     */
    private String[] splitLine(String[] header, String line){
        String[] values = new String[header.length];

        //Replace tab with white space.
        line = expandTab(line);

        int start = 0;
        for(int i=0; i<header.length; i++){
            int colLength = header[i].length();
            try {
                String val = line.substring(start, Math.min(start + colLength, line.length())).trim();
                values[i] = "%".equalsIgnoreCase(val) ? null : val;
            } catch (Throwable t){
                throw new IllegalStateException(t);
            }
            start = start + colLength + 1;
            // The last few values values might be null.
            if(start > line.length())
                break;
        }

        return values;
    }

    private String expandTab(String line) {
        if(line == null)
            return null;

        int tabLength = 8;
        char[] in = line.toCharArray();
        StringBuilder sb = new StringBuilder();

        int idx = 0;
        for(int i=0; i<in.length; i++){
            final char ch = in[i];
            if(ch == '\t') {
                do {
                  sb.append(' ');
                  idx++;
                } while (idx % tabLength != 0);
            } else {
                sb.append(ch);
                idx++;
            }
        }

        return sb.toString();
    }
}

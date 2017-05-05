package org.jdbcsqltest.sqllogictest;

import org.apache.commons.codec.digest.DigestUtils;
import org.jdbcsqltest.Script;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shivshi on 5/2/17.
 */
public class SltScript extends Script {
    public int nextPtr = 0;
    List<Integer> lineNumbers = new ArrayList<Integer>();
    List<String> sqls = new ArrayList<String>();
    List<String> validation = new ArrayList<String>();

    // Pattern to validate : "30 values hashing to 3c13dee48d9356ae19af2515e05e6b54";
    Pattern HASH_PATTERN = Pattern.compile("(\\d+).*hashing to (\\w+)", Pattern.CASE_INSENSITIVE);

    public SltScript(File file) throws IOException {
        super(file.getName());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        scanner.useDelimiter("\\n\\s*[\\n]+");

        while (scanner.hasNext()) {
            String qblock = scanner.next();
            if (qblock.trim().isEmpty())
                continue;

            if(qblock.startsWith("hash-threshold"))
                continue;

            if(qblock.equalsIgnoreCase("halt"))
                break;

            String[] result = qblock.split(System.lineSeparator(), 2);

            String header = result[0];
            qblock = result[1];

            // TODO: Ignore skipif and onlyif for now.
            while(header.startsWith("skipif") || header.startsWith("onlyif"))  {
                result = qblock.split(System.lineSeparator(), 2);
                header = result[0];
                qblock = result[1];
            }

            result = qblock.split("---*\\n", 2);
            sqls.add(result[0]);

            if (result.length > 1)
                validation.add(result[1]);
            else
                validation.add(null);
        }
    }

    public String getNextSQLCommand() {
        if (nextPtr < sqls.size())
            return sqls.get(nextPtr++);

        return null;
    }

    private String getValidationClause() {
        return validation.get(nextPtr - 1);
    }

    public boolean validateResults(ResultSet rs, int nrows) throws SQLException {
        String valClause = this.getValidationClause();
        if (valClause == null || valClause.isEmpty())
            return false;

        // Build up the result set in the expected fashion.
        StringBuffer sb = new StringBuffer();
        int actValues = 0;
        int numCols = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for(int i=1; i<=numCols; i++) {
                actValues++;
                String val = rs.getString(i);
                sb.append(val == null ? "NULL" : val);
                sb.append("\n");
            }
        }
        String result = sb.toString();

        // If the validation clause in the format of an Hash function.
        Matcher macther = HASH_PATTERN.matcher(valClause);
        if (macther.find()) {
            String nvalues = macther.group(1);
            int expValues = Integer.valueOf(nvalues);
            String hash = macther.group(2);

            String actHash = DigestUtils.md5Hex(result.toString());
            if (!(expValues == actValues && hash.equals(actHash)))
                throw new IllegalStateException("Validation failed for Expected Rows = " + expValues + " Actual Rows = " + actValues +
                                                            " Expected hash = " + hash + " Actual hash = " + actHash);
            return true;

        }

        // Else compare the ResultSet values.
        if (!(valClause + "\n").equals(result))
            throw new IllegalStateException("Validation failed for \nExpected Rows =\n" + valClause + "\nActual Rows =\n" + result);

        return true;
    }

}

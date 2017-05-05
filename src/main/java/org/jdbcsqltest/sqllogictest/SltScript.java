package org.jdbcsqltest.sqllogictest;

import org.apache.commons.codec.digest.DigestUtils;
import org.jdbcsqltest.Script;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shivshi on 5/2/17.
 */
public class SltScript extends Script {
    public int nextPtr = 0;
    List<Integer> lineNumbers = new ArrayList<Integer>();
    List<SORT_TYPE> sortTypes = new ArrayList<SORT_TYPE>();
    List<String> sqls = new ArrayList<String>();
    List<String> validation = new ArrayList<String>();

    public enum SORT_TYPE {
        NO_SORT, ROW_SORT, VALUE_SORT
    }

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

            if (qblock.startsWith("hash-threshold"))
                continue;

            if (qblock.equalsIgnoreCase("halt"))
                break;

            String[] result = qblock.split(System.lineSeparator(), 2);

            String header = result[0];
            qblock = result[1];

            // TODO: Ignore skipif and onlyif for now.
            while (header.startsWith("skipif") || header.startsWith("onlyif")) {
                result = qblock.split(System.lineSeparator(), 2);
                header = result[0];
                qblock = result[1];
            }

            sortTypes.add(getSortType(header));

            result = qblock.split("---*\\n", 2);
            sqls.add(result[0]);

            if (result.length > 1)
                validation.add(result[1]);
            else
                validation.add(null);
        }
    }

    private SORT_TYPE getSortType(String header) {
        if (header.contains("rowsort"))
            return SORT_TYPE.ROW_SORT;
        else if (header.contains("valuesort"))
            return SORT_TYPE.VALUE_SORT;
        else
            return SORT_TYPE.NO_SORT;
    }

    public String getNextSQLCommand() {
        if (nextPtr < sqls.size())
            return sqls.get(nextPtr++);

        return null;
    }

    private String getValidationClause() {
        return validation.get(nextPtr - 1);
    }

    private SORT_TYPE getSortType() {
        return sortTypes.get(nextPtr - 1);
    }

    public boolean validateResults(ResultSet rs, int nrows) throws SQLException {
        String valClause = this.getValidationClause();
        if (valClause == null || valClause.isEmpty())
            return false;

        // Collect the result set into a temp buffer.
        boolean hasNull = false;
        int actValues = 0;
        int numCols = rs.getMetaData().getColumnCount();
        ArrayList<String[]> rows = new ArrayList<String[]>();
        while (rs.next()) {
            String[] row = new String[numCols];
            for (int i = 0; i < numCols; i++) {
                actValues++;
                String val = rs.getString(i + 1);
                if (val == null) {
                    hasNull = true;
                    val = "NULL";
                }
                row[i] = val;
            }
            rows.add(row);
        }

        // Sort rows if needed and format the result set.
        StringBuffer sb = new StringBuffer();
        switch (this.getSortType()){
            case ROW_SORT:
                Collections.sort(rows, new Comparator<String[]>() {
                    public int compare(final String[] entry1, final String[] entry2) {
                        // Assumes arrays are of equal length and no nulls.
                        for (int i = 0; i < entry1.length; i++) {
                            int res = entry1[i].compareTo(entry2[i]);
                            if (res == 0)
                                continue;
                            else
                                return res;
                        }
                        return 0;
                    }
                });
                for (String[] row : rows) {
                    for (String col : row) {
                        sb.append(col).append("\n");
                    }
                }
                break;
            case VALUE_SORT:
                String[] values = new String[actValues];
                int i = 0;
                for (String[] row : rows) {
                    for (String col : row) {
                        values[i++] = col;
                    }
                }
                Arrays.sort(values);
                for (String val : values) {
                    sb.append(val).append("\n");
                }
                break;
            default: // NO_SORT
                for (String[] row : rows) {
                    for (String col : row) {
                        sb.append(col).append("\n");
                    }
                }
                break;
        }
        String result = sb.toString();

        // If the validation clause in the format of an Hash function.
        Matcher macther = HASH_PATTERN.matcher(valClause);
        if (macther.find()) {
            String nvalues = macther.group(1);
            int expValues = Integer.valueOf(nvalues);
            String hash = macther.group(2);

            String actHash = DigestUtils.md5Hex(result.toString());
            if (!(expValues == actValues))
                throw new IllegalStateException("Validation failed for Expected Rows = " + expValues + " Actual Rows = " + actValues);

            if (!hasNull) {
                if (!hash.equals(actHash))
                    throw new IllegalStateException("Validation failed for Expected hash = " + hash + " Actual hash = " + actHash);
            }

            return true;
        }

        // Else compare the ResultSet values.
        if (!(valClause + "\n").equals(result))
            throw new IllegalStateException("Validation failed for \nExpected Rows =\n" + valClause + "\nActual Rows =\n" + result);

        return true;
    }

}

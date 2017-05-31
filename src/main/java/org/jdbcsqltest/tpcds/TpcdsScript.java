package org.jdbcsqltest.tpcds;

import net.hydromatic.tpcds.query.Query;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.jdbcsqltest.Script;

import java.io.File;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by shivshi on 5/25/17.
 */
public class TpcdsScript extends Script {
    public String sql;
    private String resultFile;
    int nextPtr = 0;

    private static final String[] DISABLED_IDS = {
       // ROLLUP.
       "q05", "q14", "q18", "q22", "q27", "q36", "q67", "q70", "q77", "q80", "q86",

       // TOO Slow or haven't finished.
       "q01", "q04", "q06", "q10", "q11", "q35", "q47", "q57", "q74", "q81",

       // 100+ secs.
       "q16", "q23", "q30", "q31", "q39", "q78", "q95",

       // 30+ secs.
       "q02", "q09", "q17", "q43", "q59", "q73",
    };
    public static final Set<String> DISABLED_IDS_SET = new HashSet<String>(Arrays.asList(DISABLED_IDS));

    public TpcdsScript(File file) throws Exception {
        super(FilenameUtils.getBaseName(file.getName()));
        sql = FileUtils.readFileToString(file, "UTF-8");
        resultFile = FilenameUtils.removeExtension(file.getAbsolutePath()) + ".out";
    }

    public SqlCommand getNextSQLCommand() {
        if (nextPtr > 0 || DISABLED_IDS_SET.contains(this.getName()))
            return null;

        nextPtr++;
        return new SqlCommand(this.getName(), sql);
    }

    public boolean validateResults(ResultSet rs, int nrows) throws Exception {
        return false;
    }
}

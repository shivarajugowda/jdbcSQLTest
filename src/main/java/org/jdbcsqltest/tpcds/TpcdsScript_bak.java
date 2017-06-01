package org.jdbcsqltest.tpcds;

import net.hydromatic.tpcds.query.Query;
import org.apache.commons.io.FileUtils;
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
public class TpcdsScript_bak extends Script {
    public String sql;
    private Query[] queries;
    int nextPtr = 0;

    private static final String[] DISABLED_IDS = {
            "Q02", //Performance reasons. Renable later.
       "Q04", "Q05", "Q06", "Q09", "Q10", "Q11",
    };
    public static final Set<String> DISABLED_IDS_SET = new HashSet<String>(Arrays.asList(DISABLED_IDS));

    public TpcdsScript_bak(String name) throws Exception{
        super(name);
        queries = Query.values();
        /*
        File file = new File("tpcds.queries");
        for(Query query: queries){
            String data = "\n--" + query.name() + "\n" + query.sql(new Random(0)) + "\n";
            FileUtils.writeStringToFile(file, data, true);
        }
        */
    }

    public SqlCommand getNextSQLCommand() {
        SqlCommand sqlCommand = getNextSQLCommandInt();
        while(sqlCommand != null && DISABLED_IDS_SET.contains(sqlCommand.name)){
            sqlCommand = getNextSQLCommandInt();
        }
        return sqlCommand;
    }

    public SqlCommand getNextSQLCommandInt() {
        if (nextPtr > queries.length)
            return null;

        Query query = queries[nextPtr++];
        return new SqlCommand(query.name(), query.sql(new Random(0)));
    }

    public boolean validateResults(ResultSet rs, int nrows) throws Exception {
        return false;
    }
}

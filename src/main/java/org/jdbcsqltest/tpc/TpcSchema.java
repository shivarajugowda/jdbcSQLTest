package org.jdbcsqltest.tpc;

import org.jdbcsqltest.Config;
import org.jdbcsqltest.Script;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.zip.ZipInputStream;

/**
 * Created by shivshi on 5/2/17.
 */
public class TpcSchema extends Script {
    public int currentPtr = 0;
    List<String> sqls = new ArrayList<String>();

    public TpcSchema(File file, String dbType) throws IOException {
        super(file.getName());

        Scanner sc = new Scanner(new BufferedReader(new FileReader(file)));
        sc.useDelimiter(";");

        while (sc.hasNext()) {
            String sql = sc.next();
            if(sql != null && (!sql.trim().isEmpty()) )
                sqls.add(fixSQL(dbType, sql));
        }
    }

	private String fixSQL(String dbType, String sql) {
		
		// Remove comment lines.
		sql = sql.replaceAll("--.*\r?\n","");
		
		if(Config.DATABASE_ORACLE.equalsIgnoreCase(dbType)){
			sql = sql.replaceAll("bigint", "number(19)");
		}
        return sql.trim();
	}
    public SqlCommand getNextSQLCommand(){
        if(currentPtr < sqls.size())
            return new SqlCommand(String.valueOf(currentPtr), sqls.get(currentPtr++));

        return null;
    }

}

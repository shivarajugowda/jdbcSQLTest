package org.jdbcsqltest.tpcds;

import org.jdbcsqltest.Script;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by shivshi on 5/2/17.
 */
public class TpcdsSchemaScript extends Script {
    public int currentPtr = 0;
    List<String> sqls = new ArrayList<String>();

    public TpcdsSchemaScript(File file) throws IOException {
        super(file.getName());

        Scanner sc = new Scanner(new BufferedReader(new FileReader(file)));
        sc.useDelimiter(";");

        while (sc.hasNext()) {
            String sql = sc.next();
            if(sql != null && (!sql.trim().isEmpty()) )
                sqls.add(sql.trim());
        }
    }

    public SqlCommand getNextSQLCommand(){
        if(currentPtr < sqls.size())
            return new SqlCommand(String.valueOf(currentPtr), sqls.get(currentPtr++));

        return null;
    }

}

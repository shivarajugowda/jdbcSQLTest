package org.jdbcsqltest.tpch;

import org.jdbcsqltest.Script;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.zip.ZipInputStream;

/**
 * Created by shivshi on 5/2/17.
 */
public class TpchSchemaScript extends Script {
    public int currentPtr = 0;
    List<String> sqls = new ArrayList<String>();

    public TpchSchemaScript(File file) throws IOException {
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

package org.gunda.jdbcsqltest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

/**
 * Created by shivshi on 5/2/17.
 */
public class ScriptColon {
    public List<String> lines = null;
    public int currentPtr = 0;
    Scanner scanner;

    public ScriptColon(File file) throws IOException {
        scanner = new Scanner(new BufferedReader(new FileReader(file)));
        scanner.useDelimiter(";");
    }

    public String getNextSQLCommand(){
        String sql = null;
        if(scanner.hasNext()){
            sql = scanner.next();
            if(sql != null && (!sql.trim().isEmpty()) )
                sql = sql.trim();
        }

        return sql;
    }

}

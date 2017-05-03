package org.gunda.jdbcsqltest;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shivshi on 5/2/17.
 */
public class SqlScript extends Script {
    public int currentPtr = 0;
    List<String> sqls = new ArrayList<String>();
    List<String> validation = new ArrayList<String>();

    public SqlScript(File file) throws IOException {
        super(file.getName());
        List<String> lines = FileUtils.readLines(file, "UTF-8");
        String sql = "";
        for(int i=0; i < lines.size();i++) {
            String sl = lines.get(i);
            if(isComment(sl))
                continue;
            if(sl.trim().endsWith(";")) {
                sql += sl.trim();
                sqls.add(sql);
                sql = "";
                if( hasValidationLine(lines, i) )
                    validation.add(lines.get(++i));
                else
                    validation.add(null);
            } else {
                sql += sl.trim() + ' ';
            }
        }
    }

    private boolean isComment(String line){
        return line.trim().startsWith("--");
    }

    private boolean hasValidationLine(List<String> lines, int lineNo){
        if(lineNo >= lines.size())
            return false;

        return lines.get(lineNo+1).trim().startsWith("-- PASS");
    }

    public String getNextSQLCommand(){
        if(currentPtr < sqls.size())
            return sqls.get(currentPtr++);

        return null;
    }

    public String getValidationClause(){
        if(currentPtr < validation.size())
            return validation.get(currentPtr-1);

        return null;
    }

}

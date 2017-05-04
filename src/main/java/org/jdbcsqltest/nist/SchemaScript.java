package org.jdbcsqltest.nist;

import org.apache.commons.io.FileUtils;
import org.jdbcsqltest.Script;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shivshi on 5/2/17.
 */
public class SchemaScript extends Script {
    public int currentPtr = 0;
    List<String> sqls = new ArrayList<String>();

    public SchemaScript(File file) throws IOException {
        super(file.getName());
        List<String> lines = FileUtils.readLines(file, "UTF-8");
        String sql = "";
        for(int i=0; i < lines.size();i++) {
            String sl = lines.get(i);
            sl = sl.split("--")[0];
            if(sl.trim().isEmpty()) {
                if(!sql.trim().isEmpty())
                    sqls.add(sql);
                sql = "";
                continue;
            }
            sql += sl.trim() + ' ';
        }
        if(!sql.trim().isEmpty())
            sqls.add(sql);
    }

    public String getNextSQLCommand(){
        if(currentPtr < sqls.size())
            return sqls.get(currentPtr++);

        return null;
    }

}

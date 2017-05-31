package org.jdbcsqltest.foodmart;

import org.jdbcsqltest.Script;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.zip.ZipInputStream;

/**
 * Created by shivshi on 5/2/17.
 */
public class FoodmartSchemaScript extends Script {
    public int currentPtr = 0;
    List<String> sqls = new ArrayList<String>();

    public FoodmartSchemaScript(File file) throws IOException {
        super(file.getName());

        ZipInputStream zipStream = new ZipInputStream(new FileInputStream(file));
        zipStream.getNextEntry();

        Scanner sc = new Scanner(zipStream);
        while (sc.hasNextLine()) {
            String qblock = sc.nextLine();

            // Handle DOS carriage return.
            //qblock = qblock.replaceAll("\r", "");

            if (qblock.trim().isEmpty())
                continue;

            sqls.add(qblock);
        }
    }

    public SqlCommand getNextSQLCommand(){
        if(currentPtr < sqls.size())
            return new SqlCommand(String.valueOf(currentPtr), sqls.get(currentPtr++));

        return null;
    }

}

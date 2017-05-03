package org.gunda.jdbcsqltest;

/**
 * Created by shivshi on 5/3/17.
 */
public class Script {
    public String name;

    public Script(String name){
        this.name = name;
    }

    public String getNextSQLCommand() {
        return null;
    }

    public String getLine() {
        return null;
    }

    public String getValidationClause() {
        return null;
    }

    public String getName(){
        return name;
    }
}

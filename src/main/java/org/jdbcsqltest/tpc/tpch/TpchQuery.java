package org.jdbcsqltest.tpc.tpch;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.jdbcsqltest.Script;
import org.jdbcsqltest.tpc.TpcQuery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

/**
 * Created by shivshi on 5/24/17.
 */
public class TpchQuery extends TpcQuery {
    private float FLOATING_POINT_DELTA = 0.05f;
    int nextPtr = 0;

    public TpchQuery(File file, String sf, String dbType) throws Exception {
        super(file, sf, dbType);
    }

    public SqlCommand getNextSQLCommand() {
    	
    	if(!"q18".equalsIgnoreCase(name)) {
    		//return null;
    	}
    	
        if (nextPtr > 0)
            return null;

        nextPtr++;
        return new SqlCommand(this.getName(), sql);
    }

}

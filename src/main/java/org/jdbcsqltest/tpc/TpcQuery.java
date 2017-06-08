package org.jdbcsqltest.tpc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.jdbcsqltest.Config;
import org.jdbcsqltest.Script;

public class TpcQuery extends Script {

    private float FLOATING_POINT_DELTA = 0.5f;
    
    // Pattern to validate : "-- define _LIMIT=100;"
    Pattern LIMIT_PATTERN = Pattern.compile("--\\s+define\\s+_LIMIT\\s?=\\s?(\\w+)\\s?", Pattern.CASE_INSENSITIVE);
    public static String LIMIT_START = "[_LIMITA]";
    public static String LIMIT_MID 	 = "[_LIMITB]";
    public static String LIMIT_END   = "[_LIMITC]";
    
    Map<String, Map<String, String>> limitPatterns = populateLimitPatterns();
    
    protected String sql;
    protected File resultFile;
    
	public TpcQuery(File file, String sf, String dbType) throws Exception {
        super(FilenameUtils.getBaseName(file.getName()));
        sql = FileUtils.readFileToString(file, "UTF-8");
        sql = this.fixLimit(dbType, sql);
        resultFile = getResultFile(file, sf); 
	}
	
    public boolean validateResults(ResultSet rs, int nrows) throws Exception {
    	   
    	/*
    	//if("q15.sql".equalsIgnoreCase(name)) {
    		writeResults(resultFile, rs);
    		if(1==1)
    			return false;
    	//}
    	*/    	
        if (!resultFile.exists())
            return false;

        ResultSetMetaData rsmeta = rs.getMetaData();
        BufferedReader dataReader = new BufferedReader(new FileReader(resultFile));
        String s = dataReader.readLine();
        String[] f = s.split("\\|");
        String resultMatchError = null;

        if (f.length != rsmeta.getColumnCount()) {
            throw new IllegalStateException("Result validation : FAILED. Number of Columns DO NOT MATCH. Expected=" + f.length + " Actual=" + rsmeta.getColumnCount());
        }

        while (rs.next()) {
            s = dataReader.readLine();
            if (s == null) {
                throw new IllegalStateException("Result validation : FAILED. Number of Actual Rows  more than Expected rows");
            }

            if (resultMatchError == null) {
                String[] tokens = s.split("\\|");
                for (int i = 0; i < rsmeta.getColumnCount(); i++) {
                	String expected = tokens[i];
                	int colType = rsmeta.getColumnType(i + 1);
                	Object actual = rs.getObject(i+1);               	
                    if (!checkResult(colType, expected, actual, FLOATING_POINT_DELTA)) {
                        resultMatchError = "Column " + rsmeta.getColumnName(i + 1) + ". DataType " + rsmeta.getColumnTypeName(i + 1) + " " + rsmeta.getColumnType(i + 1) + ". Value " + rs.getString(i + 1) + " != " + tokens[i].trim();
                        break;
                    }

                }
            }
        }

        if (((s = dataReader.readLine()) != null) && (!s.trim().isEmpty())) {
            throw new IllegalStateException("Result validation : FAILED. Number of Expected Rows  more than Actual rows");
        }

        if(resultMatchError != null)
            throw new IllegalStateException("Result validation : Number of Rows match but some values don't match : " + resultMatchError);
        dataReader.close();
        return true;
    }
	
	private static Map<String, Map<String, String>> populateLimitPatterns() {
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		
		// HSQLDB or PostgresSQL : "SELECT * FROM t LIMIT 10"
		Map<String, String> map = new HashMap<String, String>();
		map.put(LIMIT_END, " LIMIT %d ");
		
		result.put(Config.DATABASE_HSQLDB, map);
		result.put(Config.DATABASE_PGSQL, map);
		
		// SQLServer : "SELECT TOP 10 * FROM t"
		map = new HashMap<String, String>();
		map.put(LIMIT_MID, " TOP %d ");
		
		result.put(Config.DATABASE_SQL_SERVER, map);
		
		// CIS, DB2 : "SELECT * FROM t FETCH FIRST 100 ROWS ONLY"
		map = new HashMap<String, String>();
		map.put(LIMIT_END, " FETCH FIRST %d ROWS ONLY ");
		
		result.put(Config.DATABASE_CIS, map);
		result.put(Config.DATABASE_DB2, map);
		
		// Oracle : "SELECT * FROM t FETCH FIRST 100 ROWS ONLY"
		map = new HashMap<String, String>();
		map.put(LIMIT_START, " SELECT * FROM ( ");
		map.put(LIMIT_END, 	 " ) WHERE ROWNUM <= %d ");
		
		result.put(Config.DATABASE_ORACLE, map);
		return result;
	}
	
	public String fixLimit(String dbType, String sql){
        Matcher macther = LIMIT_PATTERN.matcher(sql);
        if (macther.find()) {
            String limitRows = macther.group(1);
            Map<String, String> pat = limitPatterns.get(dbType);
            
            String val = pat.get(LIMIT_START);
            val = val == null? "": val;
            val = val.replace("%d", limitRows);
            sql = sql.replace(LIMIT_START, val);
            
            val = pat.get(LIMIT_MID);
            val = val == null? "": val;
            val = val.replace("%d", limitRows);
            sql = sql.replace(LIMIT_MID, val);
            
            val = pat.get(LIMIT_END);
            val = val == null? "": val;
            val = val.replace("%d", limitRows);
            sql = sql.replace(LIMIT_END, val);
        }
        return sql;
	}

	/*
	 * Given a test file and a scalefactor, returns the location of the result
	 * file.
	 */
	public static File getResultFile(File file, String sf) {
		File parent = new File(file.getParentFile().getParent(), "results");
		parent = new File(parent, sf);
		String baseName = FilenameUtils.removeExtension(file.getName()) + ".out";
		return new File(parent, baseName);
	}
	
	public static boolean checkResult(int type, String expected, Object actual, float FLOATING_POINT_DELTA) {
		boolean resultsMatch = false;

		// Check null condition, expected is always not null because of string.split().
		if (actual == null && expected.trim().isEmpty())
			return true;
		else if (actual == null && !expected.trim().isEmpty())
			return false;

		switch (type) {
		case Types.DECIMAL:
		case Types.NUMERIC:
		case Types.REAL:
		case Types.INTEGER:
		case Types.DOUBLE:
		case Types.FLOAT:
			Double dblExp = new Double(expected);
			Double dblAct = new Double(actual.toString());
			resultsMatch = Math.abs(dblExp - dblAct) < FLOATING_POINT_DELTA;
			break;
        // Different DBs(ex. Oracle) interchange DATE and TIMESTAMP data types. Handle them consistently here.
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			long tsAct = ((java.util.Date)actual).getTime();
			long tsExp = parseTimestamp(expected);
			resultsMatch = tsAct == tsExp;
			break;
		default:
			resultsMatch = expected.trim().equals(actual.toString().trim());
		}
		
		if(!resultsMatch)
			System.out.println("Hello world");

		return resultsMatch;
	}
	
	private static long parseTimestamp(String str){
		try {
			Date d = Date.valueOf(str);
			return d.getTime();
		} catch(IllegalArgumentException e) {
			
		}
		
		try {
			Time d = Time.valueOf(str);
			return d.getTime();
		} catch(IllegalArgumentException e) {
			
		}
		
		try {
			Timestamp d = Timestamp.valueOf(str);
			return d.getTime();
		} catch(IllegalArgumentException e) {
			
		}
		
		throw new IllegalStateException("Unknow time stamp format : " + str);		
	}

	public void writeResults(File file, ResultSet rs) throws Exception {
		BufferedWriter dataWriter = new BufferedWriter(new FileWriter(file));
		ResultSetMetaData rsmeta = rs.getMetaData();
		
		StringBuilder sb = new StringBuilder(); 
		int numCols = rsmeta.getColumnCount();
		for (int i = 1; i <= numCols; i++) {
			sb.append(rsmeta.getColumnName(i));
			if(i<numCols)
				sb.append('|');
		}
		dataWriter.append(sb.toString());
		
		while (rs.next()) {			
			sb = new StringBuilder(); 
			sb.append(" ");
			for (int i = 1; i <= numCols; i++) {
				Object obj = rs.getString(i);
				if(obj != null)
					sb.append(obj);
				if(i<numCols)
					sb.append('|');
			}
			dataWriter.newLine();
			dataWriter.append(sb.toString());
		}
		dataWriter.close();
	}
}

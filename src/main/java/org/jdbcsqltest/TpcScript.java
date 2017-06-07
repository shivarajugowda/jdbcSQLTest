package org.jdbcsqltest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;

public class TpcScript extends Script {

    // Pattern to validate : "-- define _LIMIT=100;"
    Pattern LIMIT_PATTERN = Pattern.compile("--\\s+define\\s+_LIMIT\\s?=\\s?(\\w+);", Pattern.CASE_INSENSITIVE);
    public static String LIMIT_START = "[_LIMITA]";
    public static String LIMIT_MID 	 = "[_LIMITB]";
    public static String LIMIT_END   = "[_LIMITC]";
    
    Map<String, Map<String, String>> limitPatterns = populateLimitPatterns();
    
	public TpcScript(String name) {
		super(name);
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
	
	public static boolean checkResult(int type, String expected, String actual,
			float FLOATING_POINT_DELTA) {
		boolean resultsMatch = false;

		// Check null condition, actual is always not null because of
		// string.split().
		if (expected == null && (actual == null || actual.trim().isEmpty()))
			return true;
		else if (expected == null && !actual.isEmpty())
			return false;

		switch (type) {
		case Types.DECIMAL:
		case Types.NUMERIC:
		case Types.REAL:
		case Types.INTEGER:
		case Types.DOUBLE:
		case Types.FLOAT:
			Double dblExp = new Double(expected);
			Double dblAct = new Double(actual);
			resultsMatch = Math.abs(dblExp - dblAct) < FLOATING_POINT_DELTA;
			break;
		default:
			resultsMatch = expected.trim().equals(actual.trim());

		}

		return resultsMatch;
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

package org.jdbcsqltest;

import org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by shivshi on 5/3/17.
 */
public class Script {
	static float FLOATING_POINT_DELTA = 0.05f;

	public class SqlCommand {
		public String name;
		public String sql;

		public SqlCommand(String name, String sql) {
			this.name = name;
			this.sql = sql;
		}
	}

	public String name;

	public Script(String name) {
		this.name = name;
	}

	public SqlCommand getNextSQLCommand() {
		return null;
	}

	public String getLine() {
		return null;
	}

	public String getName() {
		return name;
	}

	public boolean validateResults(ResultSet rs, int nrows) throws Exception {
		return false;
	}

	public boolean isDDL(String sql) {
		String lcsql = sql.toLowerCase().trim();
		if (lcsql.startsWith("create") || lcsql.startsWith("alter")
				|| lcsql.startsWith("drop") || lcsql.startsWith("insert")
				|| lcsql.startsWith("delete") || lcsql.startsWith("update"))
			return true;
		return false;
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

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
}

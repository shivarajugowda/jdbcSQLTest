package org.jdbcsqltest.tpc.tpcds;

import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;

import org.jdbcsqltest.Config;
import org.jdbcsqltest.JdbcDriver;

import io.airlift.tpch.TpchColumn;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by shivshi on 5/24/17.
 */
public class TpcdsPopulateData {
	private int BATCH_SIZE = 1000;
	private double SCALE_FACTOR = 1;
	JdbcDriver conn;

	public TpcdsPopulateData(JdbcDriver conn, String sf) {
		this.conn = conn;
		if (Config.SCALE_FACTOR_1.equalsIgnoreCase(sf))
			SCALE_FACTOR = 1.0;
		else
			SCALE_FACTOR = 0.01;
	}

	public void execute() throws SQLException {
		for (Table tpcdsTable : Table.getBaseTables()) {
			// Ignore DBGEN_VERSION
			if ("DBGEN_VERSION".equalsIgnoreCase(tpcdsTable.getName()))
				continue;
			long startTime = System.currentTimeMillis();
			long nrows = populateTable(tpcdsTable);
			System.out.format("%-35s %1s %-15s %-15s \n", "Populate "
					+ tpcdsTable.getName(), ":", "#Rows " + nrows,
					"Time(ms) " + (System.currentTimeMillis() - startTime));
		}
	}

	private long populateTable(Table tpcdsTable) throws SQLException {
		Column[] columns = tpcdsTable.getColumns();

		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(tpcdsTable.getName().toLowerCase())
				.append(" VALUES (");
		for (int i = 0; i < columns.length; i++) {
			sb.append("?");
			if (i == (columns.length - 1))
				sb.append(")");
			else
				sb.append(",");
		}
		ArrayList<Integer> columnTypes = getColumnTypes(tpcdsTable.getName()
				.toLowerCase());

		PreparedStatement pstmt = conn.getConnection().prepareStatement(
				sb.toString());

		// Minimum value for scale = 1. To accomplish fractional scale, limit
		// the rows.
		int scale = SCALE_FACTOR > 1 ? (int) SCALE_FACTOR : 1;
		Integer MAX_ROWS = null;
		if (SCALE_FACTOR < 1) {
			// Size of the biggest table, inventory = 11745000
			MAX_ROWS = (int) (11745000 * SCALE_FACTOR);
		}

		Session session = Session.getDefaultSession().withTable(tpcdsTable)
				.withScale(scale);
		Results results = Results.constructResults(tpcdsTable, session);
		Iterator res = results.iterator();
		long nrows = 0;
		while (res.hasNext()) {
			List parentAndChildRows = (List) res.next();
			List<String> row = (List<String>) parentAndChildRows.get(0);
			for (int i = 0; i < columns.length; i++) {
				Column col = columns[i];
				String val = row.get(i);
				/*
				 * if(val == null || val.trim().isEmpty()) pstmt.setNull(i+1,
				 * columnTypes.get(i)); else pstmt.setObject(i+1, val,
				 * columnTypes.get(i));
				 */
				switch (col.getType().getBase()) {
				case IDENTIFIER:
					pstmt.setObject(i + 1, val, Types.BIGINT);
					break;
				case INTEGER:
					pstmt.setObject(i + 1, val, Types.INTEGER);
					break;
				case DATE:
					pstmt.setObject(i + 1, val, Types.DATE);
					break;
				case TIME:
					pstmt.setObject(i + 1, val, Types.TIME);
					break;
				case DECIMAL:
					pstmt.setObject(i + 1, val, Types.DECIMAL);
					break;
				case VARCHAR:
				case CHAR:
					pstmt.setString(i + 1, val);
					break;
				default:
					throw new IllegalArgumentException("Unsupported type "
							+ col.getType());
				}
			}
			// pstmt.execute();
			pstmt.addBatch();
			if ((++nrows % BATCH_SIZE) == 0)
				pstmt.executeBatch();

			// For scale factors < 1.
			if (MAX_ROWS != null && nrows > MAX_ROWS) {
				break;
			}
		}
		// For remaining rows.
		if ((nrows % BATCH_SIZE) != 0)
			pstmt.executeBatch();

		pstmt.close();
		return nrows;
	}

	private ArrayList<Integer> getColumnTypes(String tableName)
			throws SQLException {
		ArrayList<Integer> result = new ArrayList<Integer>();

		PreparedStatement pstmt = conn.getConnection().prepareStatement(
				"SELECT * FROM " + tableName);
		ResultSet rs = pstmt.executeQuery();
		ResultSetMetaData rsmeta = rs.getMetaData();

		for (int i = 0; i < rsmeta.getColumnCount(); i++) {
			result.add(rsmeta.getColumnType(i + 1));

		}
		rs.close();
		pstmt.close();
		return result;
	}
}

package org.jdbcsqltest.tpcds;

import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import org.jdbcsqltest.JdbcDriver;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by shivshi on 5/24/17.
 */
public class TpcdsPopulateData {
    private int BATCH_SIZE = 1000;
    private int TPCH_SCALE_FACTOR = 1;
    JdbcDriver conn;

    public TpcdsPopulateData(JdbcDriver conn) {
        this.conn = conn;
    }

    public void execute() throws SQLException {
        for (Table tpcdsTable : Table.getBaseTables()) {
            long startTime = System.currentTimeMillis();
            //long nrows = populateTable(Table.CUSTOMER);
            long nrows = populateTable(tpcdsTable);
            System.out.format("%-35s %1s %-15s %-15s \n",
                    "Populate table " + tpcdsTable.getName(),
                    ":",
                    "#Rows = " + nrows,
                    "Time(ms) " + (System.currentTimeMillis() - startTime));
        }
    }

    private long populateTable(Table tpcdsTable) throws SQLException {
        Column[] columns = tpcdsTable.getColumns();

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tpcdsTable.getName()).append(" VALUES (");
        for (int i = 0; i < columns.length; i++) {
            sb.append("?");
            if (i == (columns.length - 1))
                sb.append(")");
            else
                sb.append(",");
        }
        ArrayList<Integer> columnTypes = getColumnTypes(tpcdsTable.getName());

        PreparedStatement pstmt = conn.getConnection().prepareStatement(sb.toString());

        Session session = Session.getDefaultSession().withTable(tpcdsTable).withScale(TPCH_SCALE_FACTOR);
        Results results = Results.constructResults(tpcdsTable, session);
        Iterator res = results.iterator();
        long nrows = 0;
        while (res.hasNext()) {
            List parentAndChildRows = (List) res.next();
            List<String> row = (List<String>) parentAndChildRows.get(0);
            for (int i = 0; i < columns.length; i++) {
                String val = row.get(i);
                if(val == null || val.trim().isEmpty())
                    pstmt.setNull(i+1, columnTypes.get(i));
                else
                    pstmt.setObject(i+1, val, columnTypes.get(i));
            }
            //pstmt.execute();
            pstmt.addBatch();
            if ((++nrows % BATCH_SIZE) == 0)
                pstmt.executeBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        return nrows;
    }

    private ArrayList<Integer> getColumnTypes(String tableName){
        ArrayList<Integer> result = new ArrayList<Integer>();

        try {
            DatabaseMetaData dbmeta = conn.getConnection().getMetaData();
            ResultSet columns = dbmeta.getColumns(null, conn.getSchema(), tableName, null);

            while (columns.next()) {
                int datatype = columns.getInt("DATA_TYPE");
                result.add(datatype);
            }

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return result;
    }
}

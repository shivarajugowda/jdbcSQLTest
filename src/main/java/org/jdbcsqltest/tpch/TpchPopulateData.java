package org.jdbcsqltest.tpch;

import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import org.jdbcsqltest.Config;
import org.jdbcsqltest.JdbcDriver;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by shivshi on 5/24/17.
 */
public class TpchPopulateData {
    private int BATCH_SIZE = 1000;
    private double SCALE_FACTOR = 1.0;
    JdbcDriver conn;

    public TpchPopulateData(JdbcDriver conn, String sf){
        this.conn = conn;
        if(Config.SCALE_FACTOR_1.equalsIgnoreCase(sf))
        	SCALE_FACTOR = 1.0;
        else
        	SCALE_FACTOR = 0.01;        
    }

    public void execute() throws SQLException {
        for (TpchTable<?> tpchTable: TpchTable.getTables()) {
            long startTime = System.currentTimeMillis();
            long nrows = populateTable(tpchTable);
            System.out.format("%-35s %1s %-15s %-15s \n",
                    "Populate table " + tpchTable.getTableName(),
                    ":",
                    "#Rows = " + nrows,
                    "Time(ms) " + (System.currentTimeMillis() - startTime));
        }
    }

    private <E extends TpchEntity> long populateTable(TpchTable<E> tpchTable) throws SQLException {;
        List<TpchColumn<E>> columns =  tpchTable.getColumns();

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tpchTable.getTableName()).append(" VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            sb.append("?");
            if(i == (columns.size() -1) )
                sb.append(")");
            else
                sb.append(",");
        }
        PreparedStatement pstmt = conn.getConnection().prepareStatement(sb.toString());

        long nrows = 0;
        for (E row : tpchTable.createGenerator(SCALE_FACTOR, 1, 1)) {
            for (int i = 0; i < columns.size(); i++) {
                TpchColumn<E> column = columns.get(i);
                switch(column.getType().getBase()) {
                    case IDENTIFIER:
                        pstmt.setLong(i+1, column.getIdentifier(row));
                        break;
                    case INTEGER:
                        pstmt.setInt(i+1, column.getInteger(row));
                        break;
                    case DATE:
                        pstmt.setDate(i+1, Date.valueOf(column.getString(row)));
                        break;
                    case DOUBLE:
                        pstmt.setDouble(i+1, column.getDouble(row));
                        break;
                    case VARCHAR:
                        pstmt.setString(i+1, column.getString(row));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
            pstmt.addBatch();
            if( (++nrows % BATCH_SIZE) == 0)
                pstmt.executeBatch();
        }
        // For remaining rows.
        if( (nrows % BATCH_SIZE) != 0)
        	pstmt.executeBatch();
        return nrows;
    }
}

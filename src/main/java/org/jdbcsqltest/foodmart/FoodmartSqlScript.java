package org.jdbcsqltest.foodmart;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.jdbcsqltest.Script;

import java.io.*;
import java.sql.ResultSet;
import java.util.*;
import java.util.zip.ZipInputStream;

import org.junit.Assert;
/**
 * Created by shivshi on 5/2/17.
 */
public class FoodmartSqlScript extends Script {
    int nextIndex = 0;
    private JsonReader reader;
    Gson gson = new Gson();
    FoodmartRoot root;

    // Test cases which have issues mostly because of unsupported issues.
    private static final Integer[] DISABLED_IDS = {
            // These are the failures on a Postgres database.

            // Failures most likely due to collation.
            16,59,77,96,97,98,99,118,127,218,239,367,368,420,471,546,547,586,587,604,630,704,751,
            756,804,805,810,832,843,854,871,872,2099,2250,2290,2291,2423,2427,2455,2527,2602,2604,
            2621,2728,2735,2765,2793,2794,2795,2796,2797,2798,2799,2800,2802,2803,2805,2806,2807,
            2808,2809,2810,2811,2812,2813,2814,2815,2816,2817,2818,2819,2820,2821,2822,2824,2825,
            2826,2828,2829,2830,2831,2832,2834,2835,2836,2837,2839,2840,2841,2842,2843,2845,2847,
            2848,2849,2851,2852,2853,2854,2855,2856,2857,2858,2859,2860,2861,2863,2864,2865,2866,
            2867,2868,2869,5368,5385,5387,5392,5393,5395,5401,5443,5643,5682,5753,5779,5781,5800,
            6027,6076,6127,6146,6179,6181,6384,6450,6460,6494,6502,6556,6560,6565,6580,6590,6594,

            // Failures due to missing tables.
            6081,6082,6083,6084,6085,6086,6087,6088,6089,6090,6097,6098,6099,6100,6101,6102,6103,
            6104,6105,6106,6107,6108,6109,6110,6111,6112,6113,6114,6115,

            // CIS does not support "SELECT * FROM (VALUES (1))
            56, 57, 58,5662, 5663, 5664, 5665, 5666, 5667, 5668, 5669, 5670, 5671, 5675, 5676,
            5677, 5678, 5679, 5680, 5681, 5870, 5871, 5872, 5873, 5874, 5875, 5957,

            // Othe CIS issues, related to null first, and other unknown issues(looks benign).
            3030, 3482, 3494, 3538, 3546, 5239, 5589, 5618, 5671, 6050, 6056, 6739,
    };

    public static final Set<Integer> DISABLED_IDS_SET = new HashSet<Integer>(Arrays.asList(DISABLED_IDS));

    // JSON root element.
    public static class FoodmartRoot {
        public final List<FoodmartQuery> queries = new ArrayList<FoodmartQuery>();
    }

    // JSON query element.
    public static class FoodmartQuery {
        public int id;
        public String sql;
        public final List<FoodmartColumn> columns = new ArrayList<FoodmartColumn>();
        public final List<List<String>> rows = new ArrayList<List<String>>();
    }

    // JSON column element.
    public static class FoodmartColumn {
        public String name;
        public String type;
    }

    public FoodmartSqlScript(File file) throws IOException {
        super(file.getName());

        ZipInputStream zipStream = new ZipInputStream(new FileInputStream(file));
        zipStream.getNextEntry();

        reader = new JsonReader(new InputStreamReader(zipStream));
        root = gson.fromJson(reader, FoodmartRoot.class);
    }


    public SqlCommand getNextSQLCommand() {
        FoodmartQuery query = nextIndex < root.queries.size() ? root.queries.get(nextIndex++) : null;
        while(query != null && DISABLED_IDS_SET.contains(query.id)) {
            query = nextIndex < root.queries.size() ? root.queries.get(nextIndex++) : null;
        }
        // System.out.println("Id = " + query.id);
        if(query == null || query.id > 8000)
            return  null;

        String sql = query.sql == null ? null : query.sql.replaceAll("NULLS LAST", "");
        sql = sql == null ? null : sql.replaceAll("NULLS FIRST", "");
        sql = sql == null ? null : sql.replaceAll("select", "select {option disable_push}");
        return new SqlCommand(String.valueOf(query.id), sql);
    }

    public boolean validateResults(ResultSet rs, int nrows) throws Exception {

        int numCols = rs.getMetaData().getColumnCount();
        FoodmartQuery query = root.queries.get(nextIndex - 1);
        List<List<String>> results = query.rows;

        String msg =  "Query id " + query.id;
        Assert.assertEquals( msg + " Number of Columns does not match : ",
            numCols, query.columns.size() );

        int rownum = 0;
        msg = msg + " Row values do not match : ";
        while (rs.next()) {
            List<String> row = results.get(rownum++);

            // TODO: Provide Mechanism to compare un sorted result sets.
            // For now just check the number of rows.
            if(query.rows.size() > 1 && !query.sql.contains("order by"))
                continue;

            for (int i = 0; i < numCols; i++) {
                String val = rs.getString(i + 1);
                String colType = query.columns.get(i).type;

                if("String".equalsIgnoreCase(colType))
                    Assert.assertEquals(msg, row.get(i), val);
                else if("short".equalsIgnoreCase(colType))
                    Assert.assertEquals(msg, Short.valueOf(row.get(i)), Short.valueOf(val));
                else if("int".equalsIgnoreCase(colType))
                    Assert.assertEquals(msg, Integer.valueOf(row.get(i)), Integer.valueOf(val));
                else if("double".equalsIgnoreCase(colType))
                    Assert.assertEquals(msg, Double.valueOf(row.get(i)), Double.valueOf(val));
            }
        }

        return true;
    }

}

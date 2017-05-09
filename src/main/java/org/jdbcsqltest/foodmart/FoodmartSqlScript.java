package org.jdbcsqltest.foodmart;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.jdbcsqltest.Script;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    // From Apache Calcite test code.
    private static final Integer[] DISABLED_IDS = {
            58, 83, 202, 204, 205, 206, 207, 209, 211, 231, 247, 275, 309, 383, 384,
            385, 448, 449, 471, 494, 495, 496, 497, 499, 500, 501, 502, 503, 505, 506,
            507, 514, 516, 518, 520, 534, 551, 563, 566, 571, 628, 629, 630, 644, 649,
            650, 651, 653, 654, 655, 656, 657, 658, 659, 669, 722, 731, 732, 737, 748,
            750, 756, 774, 777, 778, 779, 781, 782, 783, 811, 818, 819, 820, 2057,
            2059, 2060, 2073, 2088, 2098, 2099, 2136, 2151, 2158, 2162, 2163, 2164,
            2165, 2166, 2167, 2168, 2169, 2170, 2171, 2172, 2173, 2174, 2175, 2176,
            2177, 2178, 2179, 2180, 2181, 2187, 2190, 2191, 2235, 2245, 2264, 2265,
            2266, 2267, 2268, 2270, 2271, 2279, 2327, 2328, 2341, 2356, 2365, 2374,
            2415, 2416, 2424, 2432, 2455, 2456, 2457, 2518, 2521, 2528, 2542, 2570,
            2578, 2579, 2580, 2581, 2594, 2598, 2749, 2774, 2778, 2780, 2781, 2786,
            2787, 2790, 2791, 2876, 2883, 5226, 5227, 5228, 5229, 5230, 5238, 5239,
            5249, 5279, 5281, 5282, 5283, 5284, 5286, 5288, 5291, 5415, 5444, 5445,
            5446, 5447, 5448, 5452, 5459, 5460, 5461, 5517, 5519, 5558, 5560, 5561,
            5562, 5572, 5573, 5576, 5577, 5607, 5644, 5648, 5657, 5664, 5665, 5667,
            5671, 5682, 5700, 5743, 5748, 5749, 5750, 5751, 5775, 5776, 5777, 5785,
            5793, 5796, 5797, 5810, 5811, 5814, 5816, 5852, 5874, 5875, 5910, 5953,
            5960, 5971, 5975, 5983, 6016, 6028, 6030, 6031, 6033, 6034, 6081, 6082,
            6083, 6084, 6085, 6086, 6087, 6088, 6089, 6090, 6097, 6098, 6099, 6100,
            6101, 6102, 6103, 6104, 6105, 6106, 6107, 6108, 6109, 6110, 6111, 6112,
            6113, 6114, 6115, 6141, 6150, 6156, 6160, 6164, 6168, 6169, 6172, 6177,
            6180, 6181, 6185, 6187, 6188, 6190, 6191, 6193, 6194, 6196, 6197, 6199,
            6200, 6202, 6203, 6205, 6206, 6208, 6209, 6211, 6212, 6214, 6215, 6217,
            6218, 6220, 6221, 6223, 6224, 6226, 6227, 6229, 6230, 6232, 6233, 6235,
            6236, 6238, 6239, 6241, 6242, 6244, 6245, 6247, 6248, 6250, 6251, 6253,
            6254, 6256, 6257, 6259, 6260, 6262, 6263, 6265, 6266, 6268, 6269,

            // failed
            5677, 5681,

            // 2nd run
            6271, 6272, 6274, 6275, 6277, 6278, 6280, 6281, 6283, 6284, 6286, 6287,
            6289, 6290, 6292, 6293, 6295, 6296, 6298, 6299, 6301, 6302, 6304, 6305,
            6307, 6308, 6310, 6311, 6313, 6314, 6316, 6317, 6319, 6327, 6328, 6337,
            6338, 6339, 6341, 6345, 6346, 6348, 6349, 6354, 6355, 6359, 6366, 6368,
            6369, 6375, 6376, 6377, 6389, 6396, 6400, 6422, 6424, 6445, 6447, 6449,
            6450, 6454, 6456, 6470, 6479, 6480, 6491, 6509, 6518, 6522, 6561, 6562,
            6564, 6566, 6578, 6581, 6582, 6583, 6587, 6591, 6594, 6603, 6610, 6613,
            6615, 6618, 6619, 6622, 6627, 6632, 6635, 6643, 6650, 6651, 6652, 6653,
            6656, 6659, 6668, 6670, 6720, 6726, 6735, 6737, 6739,

            // timeout oor OOM
            420, 423, 5218, 5219, 5616, 5617, 5618, 5891, 5892, 5895, 5896, 5898, 5899,
            5900, 5901, 5902, 6080, 6091,

            // bugs
            6597, // CALCITE-403
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


    public String getNextSQLCommand() {
        FoodmartQuery query = nextIndex < root.queries.size() ? root.queries.get(nextIndex++) : null;
        while(query != null && DISABLED_IDS_SET.contains(query.id)) {
            query = nextIndex < root.queries.size() ? root.queries.get(nextIndex++) : null;
        }
        // System.out.println("Id = " + query.id);
        return (query != null && query.id < 10)? query.sql : null;
    }

    public boolean validateResults(ResultSet rs, int nrows) throws SQLException {

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

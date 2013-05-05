/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package voltdb.realtime.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@ProcInfo(
    partitionInfo = "vehicle_positions.trip_id:0",
    singlePartition = true
)
public class GetAvgStopDelays extends VoltProcedure {
    public final SQLStmt getDelaysSQL =
        new SQLStmt("SELECT stop_sequence, (total_delay / num_delays) AS avg_delay " +
                    "FROM v_trip_updates_delay_by_stop " +
                    "WHERE trip_id = ? " +
                    "ORDER BY stop_sequence;");

    public final SQLStmt getStopNameSQL =
        new SQLStmt("SELECT stop_sequence, stop_name " +
                    "FROM stops as s, stop_times as t " +
                    "WHERE trip_id = ? AND stop_sequence = ? AND s.stop_id = t.stop_id;");

    /**
     * Given the trip_id, get the average stop delays based on historical data.
     */
    public VoltTable run(String trip_id) {
        /*
         * get delays from the view
         * get stop names from the returned stop sequence IDs
         */

        voltQueueSQL(getDelaysSQL, trip_id);
        VoltTable delays = voltExecuteSQL()[0];

        if (delays.getRowCount() == 0) {
            // There's no such trip
            return null;
        }

        Map<Integer, Long> stopDelays = parseDelays(delays);

        for (int stop_sequence : stopDelays.keySet()) {
            voltQueueSQL(getStopNameSQL, trip_id, stop_sequence);
        }
        VoltTable[] stopNamesResult = voltExecuteSQL();

        Map<Integer, String> stopNames = parseStopNames(stopNamesResult);

        VoltTable delaysWithName =
            new VoltTable(new VoltTable.ColumnInfo("stop_name", VoltType.STRING),
                          new VoltTable.ColumnInfo("avg_delay", VoltType.BIGINT));
        for (Map.Entry<Integer, Long> delayEntry : stopDelays.entrySet()) {
            delaysWithName.addRow(stopNames.get(delayEntry.getKey()),
                                  delayEntry.getValue());
        }

        return delaysWithName;
    }

    private static Map<Integer, Long> parseDelays(VoltTable delays)
    {
        Map<Integer, Long> stopDelays = new TreeMap<Integer, Long>();

        while (delays.advanceRow()) {
            stopDelays.put((int) delays.getLong("stop_sequence"),
                           delays.getLong("avg_delay"));
        }

        return stopDelays;
    }

    private static Map<Integer, String> parseStopNames(VoltTable[] namesResult)
    {
        Map<Integer, String> stopNames = new HashMap<Integer, String>();

        for (VoltTable nameTable : namesResult) {
            while (nameTable.advanceRow()) {
                stopNames.put((int) nameTable.getLong("stop_sequence"),
                              nameTable.getString("stop_name"));
            }
        }

        return stopNames;
    }
}

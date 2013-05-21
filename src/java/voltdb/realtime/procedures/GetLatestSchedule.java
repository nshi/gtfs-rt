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
import voltdb.CommonUtils;

import java.util.HashMap;
import java.util.Map;

@ProcInfo(
    partitionInfo = "vehicle_positions.trip_id:0",
    singlePartition = true
)
public class GetLatestSchedule extends VoltProcedure {
    public final SQLStmt getStopTimesSQL =
        new SQLStmt("SELECT stop_sequence, stop_name, arrival_time " +
                    "FROM stop_times as t, stops as s " +
                    "WHERE trip_id = ? AND t.stop_id = s.stop_id " +
                    "ORDER BY arrival_time;");

    /**
     * Given the trip_id, calculate the latest schedule based on the original
     * schedule, the latest trip updates and the latest vehicle position.
     */
    public VoltTable run(String trip_id) {
        /*
         * get stop_times in arrival times order
         * get trip updates and apply any changes
         */

        voltQueueSQL(getStopTimesSQL, trip_id);
        VoltTable[] result = voltExecuteSQL();

        if (result[0].getRowCount() == 0) {
            // There's no such trip
            return null;
        }

        VoltTable schedule = CommonUtils.createTableFromTemplate(result[0]);

        long currentDelay = 0;
        while (result[0].advanceRow()) {
            int stop_sequence = (int) result[0].getLong("stop_sequence");
            schedule.addRow(stop_sequence,
                            result[0].getString("stop_name"),
                            result[0].getTimestampAsLong("arrival_time") + currentDelay);
        }

        return schedule;
    }
}

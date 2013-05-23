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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@ProcInfo(
    partitionInfo = "trips.trip_id:0",
    singlePartition = true
)
public class EffectStopTimeUpdates extends VoltProcedure {
    private static final SimpleDateFormat dateFormat = CommonUtils.getNoonBasedDateFormat();

    /// Override a prior updates' effects.
    public static final SQLStmt clearPriorSQL = new SQLStmt(
        "DELETE FROM effective_stop_times WHERE start_date = ? AND trip_id = ?;");


    /// Collect input.
    public static final SQLStmt workingSetSQL = new SQLStmt(
        "SELECT stu.delay delay, st.stop_sequence seq, st.arrival_usec arrival, st.departure_usec departure " +
        "FROM stop_times st LEFT JOIN stop_time_updates stu " +
        "ON stu.trip_id = st.trip_id AND stu.stop_sequence = st.stop_sequence " +
        "AND stu.start_date = ? AND stu.time_usec = ? " +
        "WHERE st.trip_id = ? " +
        "ORDER BY 2, 1, 3, 4;");  //"ORDER BY 2;" is unique, but determinism detection isn't smart enough


    /// Main effect.
    public static final SQLStmt insertSQL =
        new SQLStmt("INSERT INTO effective_stop_times VALUES (?, ?, ?, ?, ?, ?, ?);");

    /**
     */
    public long run(String trip_id, String start_date, long ts) throws ParseException
    {
        Date start = dateFormat.parse(start_date);
        voltQueueSQL(clearPriorSQL, start_date, trip_id);
        voltQueueSQL(workingSetSQL, start_date, ts, trip_id);
        VoltTable workingSet = voltExecuteSQL()[0];
        long delay = 0;

        while (workingSet.advanceRow()) {
            long next_delay = workingSet.getLong(0);
            // Only update the delay if there was an update at this stop.
            if ( ! workingSet.wasNull()) {
                delay = next_delay;
            }

            if (delay == 0) {
                continue; // This part of the schedule effectively returned to normal.
            }
            else {
                // stu.delay st.stop_sequence st.arrival_usec st.departure_usec
                int stop_sequence = (int) workingSet.getLong(1);
                long arrival = workingSet.getLong(2);
                long departure = workingSet.getLong(3);
                arrival += delay;
                // Assume that delay has no effect on the stop's duration
                // -- there appears to be no way to express that idea in the model.
                // Logically, we could "look ahead" to the next stop / next delay and enforce that the
                // departure at least did not exceed the next arrival (which may have a different delay),
                // but, for now for simplicity, we currently just allow such contradictions.
                departure += delay;
                voltQueueSQL(insertSQL,
                             trip_id, start_date, start, ts, arrival, departure, stop_sequence);
            }
        }
        voltExecuteSQL();
        return 1;
    }
}

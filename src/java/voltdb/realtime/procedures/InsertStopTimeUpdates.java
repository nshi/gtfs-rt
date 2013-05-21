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
    partitionInfo = "trip_updates.trip_id:0",
    singlePartition = true
)
public class InsertStopTimeUpdates extends VoltProcedure {
    private final SimpleDateFormat dateFormat = CommonUtils.getDateFormat();

    /// Validation - is there a stop in the base schedule?
    public final SQLStmt getStopSQL =
        new SQLStmt("SELECT count(*) FROM stop_times WHERE trip_id = ? AND stop_sequence = ?;");

    public final SQLStmt insertSQL =
        new SQLStmt("INSERT INTO stop_time_updates VALUES (?, ?, ?, ?, ?, ?);");

    /**
     */
    public long run(String trip_id, String start_date, long ts, int stop_sequence, long delay)
            throws ParseException {
        Date start = dateFormat.parse(start_date);
        voltQueueSQL(getStopSQL, trip_id, stop_sequence);
        VoltTable[] result = voltExecuteSQL();
        long stopCount = result[0].asScalarLong();
        if (stopCount != 1) {
            // No such stop, (or too many?) drop this record
            return 0;
        }

        voltQueueSQL(insertSQL, trip_id, start, start, ts, stop_sequence, delay);
        voltExecuteSQL();
        return 1;
    }
}

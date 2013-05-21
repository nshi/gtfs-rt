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
public class InsertUpdate extends VoltProcedure {
    private final SimpleDateFormat dateFormat = CommonUtils.getDateFormat();

    /// Garbage collection.
    /// This MIGHT be more effective if it was not timestamp qualified, as that would collect
    /// trip_updates for other trip_ids on the same partition that had become inactive.
    public final SQLStmt deleteOldSQL =
        new SQLStmt("DELETE FROM trip_updates WHERE trip_id = ? AND timestamp < ?;");

    /// Validation #1. Guard against out-of-order arrival of conflicting updates.
    public final SQLStmt getLastSQL =
        new SQLStmt("SELECT COUNT(*) FROM trip_updates WHERE trip_id = ? AND timestamp >= ?;");
    /// Validation #2. Guard against corrupted trip id or corrupted or out of sync base schedule.
    public final SQLStmt getTripSQL =
        new SQLStmt("SELECT COUNT(*) FROM trips WHERE trip_id = ?;");

    /// Main effect.
    public final SQLStmt insertSQL =
        new SQLStmt("INSERT INTO trip_updates VALUES (?, ?, ?, ?, ?, ?);");

    /**
     * @param history the number of seconds of history to keep in the database
     */
    public long run(String trip_id, String start_date, long ts,
                    byte relationship, long history)
            throws ParseException {
        Date start = dateFormat.parse(start_date);
        long currentTime = getTransactionTime().getTime();
        // Entries before this timestamp will be deleted
        long expiration = currentTime - history;

        voltQueueSQL(deleteOldSQL, trip_id, expiration);  // -> result[0] (ignored)
        voltQueueSQL(getLastSQL, trip_id, ts);            // -> result[1]
        voltQueueSQL(getTripSQL, trip_id);                // -> result[2]
        VoltTable[] result = voltExecuteSQL();
        long newerRecords = result[1].asScalarLong();
        if (newerRecords > 0) {
            // There are newer records for this trip, drop this one
            return 0;
        }
        long tripCount = result[2].asScalarLong();
        if (tripCount != 1) {
            // No such trip, drop this record
            return 0;
        }

        voltQueueSQL(insertSQL, trip_id, start, start, ts, ts, relationship);
        voltExecuteSQL();
        return 1;
    }
}

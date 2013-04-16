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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@ProcInfo(
    partitionInfo = "vehicle_positions.trip_id:0",
    singlePartition = true
)
public class InsertPosition extends VoltProcedure {
    private static final SimpleDateFormat dateFormat =
        new SimpleDateFormat("yyyyMMdd");

    public final SQLStmt getTripSQL =
        new SQLStmt("SELECT COUNT(*) FROM trips WHERE trip_id = ?;");

    public final SQLStmt getLastSQL =
        new SQLStmt("SELECT COUNT(*) FROM vehicle_positions WHERE trip_id = ? AND timestamp >= ?;");

    public final SQLStmt insertSQL =
        new SQLStmt("INSERT INTO vehicle_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

    public final SQLStmt deleteOldSQL =
        new SQLStmt("DELETE FROM vehicle_positions WHERE trip_id = ? AND timestamp < ?;");

    /**
     * @param history the number of seconds of history to keep in the database
     */
    public long run(String trip_id, String start_date, byte relationship,
                    double lat, double lon, int stop_sequence, String stop_id,
                    long ts, long history)
            throws ParseException {
        long currentTime = getTransactionTime().getTime();
        // Entries before this timestamp will be deleted
        long expiration = currentTime - history;

        Date start = dateFormat.parse(start_date);
        // Convert timestamp from seconds to micros
        long tsInMillis = ts * 1000 * 1000;

        voltQueueSQL(deleteOldSQL, trip_id, expiration);
        voltQueueSQL(getLastSQL, trip_id, tsInMillis);
        voltQueueSQL(getTripSQL, trip_id);
        VoltTable[] result = voltExecuteSQL();
        long newerRecords = result[1].asScalarLong();
        long tripCount = result[2].asScalarLong();

        if (newerRecords > 0) {
            // There are newer records for this trip, drop this one
            return 0;
        }
        if (tripCount != 1) {
            // No such trip, drop this record
            return 0;
        }

        voltQueueSQL(insertSQL, trip_id, start, tsInMillis, stop_sequence,
                     stop_id, relationship, lat, lon);
        voltExecuteSQL();
        return 1;
    }
}

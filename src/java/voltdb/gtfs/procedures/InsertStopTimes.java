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

package voltdb.gtfs.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import voltdb.CommonUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@ProcInfo(
    partitionInfo = "stop_times.trip_id:0",
    singlePartition = true
)
public class InsertStopTimes extends VoltProcedure {
    private final SimpleDateFormat dateFormat = CommonUtils.getTimeFormat();

    public final SQLStmt insertSQL =
        new SQLStmt("INSERT INTO stop_times VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] run(String trip_id, String arrival_str,
                           String departure_str, String stop_id,
                           int stop_seq, String stop_headsign,
                           byte pickup_type, byte drop_off_type)
        throws ParseException {
        Date arrival_time = dateFormat.parse(arrival_str);
        Date departure_time = dateFormat.parse(departure_str);

        voltQueueSQL(insertSQL, trip_id, arrival_str, arrival_time, departure_str, departure_time,
                     stop_id, stop_seq, stop_headsign, pickup_type, drop_off_type);
        return voltExecuteSQL();
    }
}

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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InsertStops extends VoltProcedure {
    public final SQLStmt insertSQL =
        new SQLStmt("INSERT INTO stops VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] run(String stop_id, String stop_code, String stop_name,
                           String stop_desc, double stop_lat, double stop_lon,
                           String zone_id, String stop_url,
                           String location_type, String parent_station) {
        byte loc_type_byte = 0;
        if (location_type != null && !location_type.trim().isEmpty()) {
            loc_type_byte = Byte.parseByte(location_type.trim());
        }

        voltQueueSQL(insertSQL, stop_id, stop_code, stop_name, stop_desc,
                     stop_lat, stop_lon, zone_id, stop_url, loc_type_byte,
                     parent_station);
        return voltExecuteSQL();
    }
}

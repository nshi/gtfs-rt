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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class InsertCalendarDates extends VoltProcedure {
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public final SQLStmt insertSQL =
        new SQLStmt("INSERT INTO calendar_dates VALUES (?, ?, ?);");

    public VoltTable[] run(String service_id, String dateStr, byte exception_type)
        throws ParseException {
        Date date = dateFormat.parse(dateStr);

        voltQueueSQL(insertSQL, service_id, date, exception_type);
        return voltExecuteSQL();
    }
}

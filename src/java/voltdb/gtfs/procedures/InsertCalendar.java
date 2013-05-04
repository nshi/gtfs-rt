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

public class InsertCalendar extends VoltProcedure {
    public static final byte MON  = 1 << 0;
    public static final byte TUE  = 1 << 1;
    public static final byte WED  = 1 << 2;
    public static final byte THUR = 1 << 3;
    public static final byte FRI  = 1 << 4;
    public static final byte SAT  = 1 << 5;
    public static final byte SUN  = 1 << 6;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public final SQLStmt insertSQL =
        new SQLStmt("INSERT INTO calendar VALUES (?, ?, ?, ?);");

    public VoltTable[] run(String service_id, byte mon, byte tue, byte wed,
                           byte thur, byte fri, byte sat, byte sun,
                           String start_date, String end_date)
            throws ParseException {
        byte weekdays = (byte) (MON * mon
                                | TUE * tue
                                | WED * wed
                                | THUR * thur
                                | FRI * fri
                                | SAT * sat
                                | SUN * sun);
        Date start = dateFormat.parse(start_date);
        Date end = dateFormat.parse(end_date);

        voltQueueSQL(insertSQL, service_id, weekdays, start, end);
        return voltExecuteSQL();
    }
}

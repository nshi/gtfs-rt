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

package voltdb;

import org.voltdb.VoltTable;

import java.text.FieldPosition;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class CommonUtils {
    private static class NoonBasedDateFormat extends SimpleDateFormat {
        final SimpleDateFormat outputFormat;
        NoonBasedDateFormat(String format)
        {
            super(format + " HH:mm:ss");
            outputFormat = new SimpleDateFormat(format);
        }
        // This formatter is intended for "date stamps" representing midnight.
        // It may slightly re-interpret midnight as an hour ahead or behind the normal
        // time value of midnight on days that contain an early morning daylight savings adjustment.
        // It essentially jumps the gun on that transition, springing forward or falling back
        // at the end of the previous day, just PRIOR to midnight.
        public Date parse(String input) throws ParseException
        {
            Date noonBased = super.parse(input + " 12:00:00");
            noonBased.setTime(noonBased.getTime() - 12*60*60*1000); // 12 hours of millis before noon.
            return noonBased;
        }

        // Reverse the parse process to "correct" the date for a midnight timestamp that became 11:00PM.
        // The reworking here should have no effect on other (true or 1:00AM) values of midnight.
        public StringBuffer format(Date date,
                                   StringBuffer toAppendTo,
                                   FieldPosition fieldPosition)
        {
            // Add 11 1/2 hours to the "midnight" time to get 10:30AM, 11:30AM, or 12:30PM.
            Date patched = new Date(date.getTime() + (11*60+30)*60*1000);
            // Truncate the unexpected time digits to leave the adjusted (next morning) date.
            return outputFormat.format(patched, toAppendTo, fieldPosition);
        }
    };

    
    private static SimpleDateFormat getNoonBasedDateFormatInTimeZone(String format, String timezone)
    {
        final SimpleDateFormat dateFormat = new NoonBasedDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        return dateFormat;
    }

    private static SimpleDateFormat getDateFormatInTimeZone(String format, String timezone)
    {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        return dateFormat;
    }

    /**
     * Create a SimpleDateFormat that parses time in the format "HH:mm:ss" in UTC/GMT to
     * generate pure offset times that can be added to date timestamps.
     */
    public static SimpleDateFormat getTimeFormat()
    {
        return getDateFormatInTimeZone("HH:mm:ss", "UTC");
    }

    /**
     * Create a SimpleDateFormat that parses date in the format "yyyyMMdd" in
     * Eastern Time.
     */
    public static SimpleDateFormat getDateFormat()
    {
        return getNoonBasedDateFormatInTimeZone("yyyyMMdd", "America/New_York");
    }

    /**
     * Create a SimpleDateFormat that parses date in the format "yyyyMMdd" in
     * Eastern Time but uses the GTFS concept of start-of-day always being 12 hours before noon.
     */
    public static SimpleDateFormat getNoonBasedDateFormat()
    {
        return getNoonBasedDateFormatInTimeZone("yyyyMMdd", "America/New_York");
    }

    /**
     * Create a SimpleDateFormat that parses date in the format "yyyyMMdd" in
     * Eastern Time.
     */
    public static SimpleDateFormat getWeekdayFormat()
    {
        return getDateFormatInTimeZone("EEE", "America/New_York");
    }


    public static VoltTable createTableFromTemplate(VoltTable template)
    {
        return privCreateTableFromTemplate(template, template.getColumnCount());
    }
    
    public static VoltTable createNarrowedTableFromTemplate(VoltTable template, int column_count)
    {
        return privCreateTableFromTemplate(template, column_count);
    }

    private static VoltTable privCreateTableFromTemplate(VoltTable template, int column_count)
    {
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[column_count];
        for (int i = 0; i < column_count; i++) {
            columns[i] = new VoltTable.ColumnInfo(template.getColumnName(i),
                                                  template.getColumnType(i));
        }
        return new VoltTable(columns);
    }

    public static Set<Long> createSetFromTableColumn(VoltTable table,
                                                     int column)
    {
        Set<Long> set = new HashSet<Long>();
        loadSetFromTableColumn(table, column, set);
        return set;
    }

    public static void loadSetFromTableColumn(VoltTable table,
                                              int column,
                                              Set<Long> set)
    {
        while (table.advanceRow()) {
            set.add(table.getLong(column));
        }
    }

}

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

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import voltdb.CommonUtils;

@ProcInfo(
    partitionInfo = "trips.trip_id:0",
    singlePartition = true
)
public class FindBetterTime extends VoltProcedure {
    private static final SimpleDateFormat dateFormat = CommonUtils.getNoonBasedDateFormat();
    private static final SimpleDateFormat weekdayFormat = CommonUtils.getWeekdayFormat();
    private static final SimpleDateFormat timeFormat = CommonUtils.getTimeFormat();
    private static final Map<String, Byte> dayMap = new HashMap<String, Byte>();

    static {
        dayMap.put("Mon", (byte)0);
        dayMap.put("Tue", (byte)1);
        dayMap.put("Wed", (byte)2);
        dayMap.put("Thu", (byte)3);
        dayMap.put("Fri", (byte)4);
        dayMap.put("Sat", (byte)5);
        dayMap.put("Sun", (byte)6);
    }   

    public static final SQLStmt getScheduledStopTimesSQL =
        // Get the scheduled stops possibly repeated to reflect effective (rescheduled) times.
        new SQLStmt("SELECT est.start_usec + est.arrival_usec arrival_datetime, " +
                    "est.start_usec start_date, " +
                    "st.arrival_usec scheduled_arrival, " +
                    "est.arrival_usec effective_arrival " +
                    "FROM stop_times st LEFT JOIN effective_stop_times est " +
                    "ON st.trip_id = est.trip_id AND st.stop_sequence = est.stop_sequence " +
                    "AND est.start_usec + est.arrival_usec >= ? " +
                    "WHERE st.trip_id = ? " +
                    "AND st.stop_sequence = ? " +
                    "ORDER BY 3, 1, 2, 4 " + // Usefully, ORDER BY 3
                    ";");                    // Others have no effect but smell deterministic.

    public static final SQLStmt getCalendarForTrip =
        new SQLStmt("SELECT c.weekdays, c.start_usec, c.end_usec " +
                    "FROM trips, calendar c " +
                    "WHERE c.service_id = trips.service_id " +
                    "AND trips.trip_id = ? " +
                    "AND c.end_usec >= ? AND c.start_usec < ? " +
                    ";");

    public static final SQLStmt getFirstAddedCalendarDateForTrip =
        new SQLStmt("SELECT c.date_usec " +
                    "FROM trips, calendar_dates c " +
                    "WHERE c.service_id = trips.service_id " +
                    "AND trips.trip_id = ? " +
                    "AND c.date_usec >= ? AND c.date_usec < ? " +
                    "AND c.exception_type = 1 " +
                    "ORDER BY date_usec " +
                    "LIMIT 1 " +
                    ";");

    public static final SQLStmt getDroppedCalendarDatesForTrip =
        new SQLStmt("SELECT c.date_usec " +
                    "FROM trips, calendar_dates c " +
                    "WHERE c.service_id = trips.service_id " +
                    "AND trips.trip_id = ? " +
                    "AND c.date_usec >= ? AND c.date_usec < ? " +
                    "AND c.exception_type = 2 " +
                    "ORDER BY date_usec " +
                    ";");
    /**
     * Given the stop_id, route_id, and earliest allowed arrival date and time,
     * calculate the next arrival, its trip_id and trip start_date
     * based on the original schedule and the latest trip updates.
     */
    public VoltTable run(String trip_id, int stop_sequence, long earliest, long latest)
        throws ParseException
    {
        voltQueueSQL(getScheduledStopTimesSQL, earliest, trip_id, stop_sequence);
        VoltTable[] results = voltExecuteSQL();
        VoltTable result = results[0];

        if (result.getRowCount() == 0) {
            // There's no trip currently running that uses this stop?
            return null;
        }

        result.advanceRow();
        final Set<Long> forbiddenTripStarts = new HashSet<Long>();
        // This arrival detail will be useful when considering ON TIME arrivals outside the loop.
        long scheduled_arrival = result.getLong("scheduled_arrival");
        long best_start_date = Long.MAX_VALUE;
        long best_arrival = Long.MAX_VALUE;

        // The outer join will produce one row with null RHS columns (effective stop time) for a
        // trip having no trip updates  -- or at least having no updates that affect this stop.
        // OR it will get one or more rows with non-null RHS columns for a trip with updates
        // to this stop. This does NOT imply that one of the start dates that got trip updates
        // is the one of interest. There may be a start date with an ON TIME arrival that is earlier.
        // OR all updated trip arrivals may have past, so that the next ON TIME arrival is best.
        long eff_start_date = result.getLong("start_date");
        if ( ! result.wasNull()) {
            do {
                // Updated stop times have a side effect of eliminating their start dates
                // from consideration for finding an ON TIME arrival.
                forbiddenTripStarts.add(eff_start_date);

                // If there are multiple rows for the trip id
                // and a previous one had an acceptable arrival,
                // there's no need to consider trip updates for later arrivals.
                if (best_start_date != Long.MAX_VALUE) {
                    continue;
                }
            
                long eff_arrival = result.getLong("arrival_datetime");
                if (eff_arrival < earliest) {
                    // This is a past stop time. It still remains to be seen if there is a later
                    // updated start time OR a better non-updated start time.
                    continue;
                }
                // This is the best updated stop time. All that remains is to determine if there is a
                // better non-updated start time, which should wait until all (other) updated
                // start_dates have been taken out of the running.
                // Don't consider ON TIME trips that start later than this.
                best_start_date = eff_start_date;
                best_arrival = eff_arrival;
            } while (result.advanceRow());
        }

        VoltTable schedule = CommonUtils.createNarrowedTableFromTemplate(result, 2);

        // It's time now to consider whether there is a better non-updated ON TIME stop time.
        // For a trip id that had updates, subtract the on time arrival time from the
        // present date/time and walk it forward by "exact days" to find the first candidate
        // start date allowed by the trip calendar. Stop walking forward at the current
        // best trip start date (if any).
        // For simplicity, this does not consider the case that an updated trip is delayed for
        // more than 24 hours so that an ON TIME plan that started the next day arrives sooner.
        // TODO: that would just require a short forward walk to confirm or rule out?
        if ( ! getTripStartDateAtOrAfter(schedule,
                                         trip_id,
                                         earliest - scheduled_arrival, //<= earliest possible start_date
                                         scheduled_arrival,
                                         best_start_date,
                                         forbiddenTripStarts)) {
            schedule.addRow(best_start_date, best_arrival);
        }
        return schedule;
    }

    private static long truncateToStartOfGtfsDay(long time) throws ParseException
    {
        Date pastMidnight = new Date(time);
        String day = dateFormat.format(pastMidnight);
        Date midnight = dateFormat.parse(day);
        return midnight.getTime();
    }

    private boolean getTripStartDateAtOrAfter(VoltTable schedule,
                                              String trip_id,
                                              long earliest_start,
                                              long scheduled_arrival,
                                              long too_late_to_start,
                                              Set<Long> forbiddenTripStarts)
    throws ParseException
    {
        voltQueueSQL(getCalendarForTrip, trip_id, earliest_start, too_late_to_start);
        voltQueueSQL(getFirstAddedCalendarDateForTrip, trip_id, earliest_start, too_late_to_start);
        voltQueueSQL(getDroppedCalendarDatesForTrip, trip_id, earliest_start, too_late_to_start);
        VoltTable[] results = voltExecuteSQL();
        VoltTable calendar = results[0];
        VoltTable adds = results[1];
        if (calendar.getRowCount() == 0 && adds.getRowCount() == 0) {
            // This trip is not in service over the period of interest.
            return false;
        }
        Set<Long> drops = CommonUtils.createSetFromTableColumn(results[2], 0);

        // Along with dropped start dates, also ignore valid calendar start dates that
        // were considered already in their updated form.
        drops.addAll(forbiddenTripStarts);

        long first_added = Long.MAX_VALUE;
        if (adds.getRowCount() > 0) {
            adds.advanceRow();
            first_added = adds.getLong(0) / 1000; // micros to millis.
            too_late_to_start = first_added; // This beats later calendar dates.
            adds = null;
        }

        long cal_start = Long.MAX_VALUE;
        long cal_stop = 0;
        byte weekdays = 0;
        if (calendar.getRowCount() != 0) {
            calendar.advanceRow();
            cal_start = calendar.getLong(1);
            if (cal_start >= first_added) {
                cal_start = Long.MAX_VALUE; // The added date beats later calendar dates.
            } else {
                weekdays = (byte)calendar.getLong(0);
                cal_stop = calendar.getLong(2)+1;
                if (cal_stop < first_added) {
                     // The calendar scan stops at its end, before any added date.
                    too_late_to_start = cal_stop;
                }
            }
        }

        if (cal_start != Long.MAX_VALUE) {
            byte weekday_index = weekdayOfDate(cal_start);
            while (cal_start < too_late_to_start) {
                if (weekdayIsOk(weekday_index, weekdays) && ! drops.contains(cal_start*1000)) {
                    schedule.addRow(cal_start + scheduled_arrival, cal_start);
                    return true;
                }
                // Add 35 hours (semi-arbitrarily) in millis to one midnight value
                // and truncate down to (the next) midnight to advance the day.
                cal_start = truncateToStartOfGtfsDay(cal_start+35*60*60*1000);
                ++weekday_index;
                weekday_index %= 7;
            }
        }
        if (first_added != Long.MAX_VALUE) {
            schedule.addRow(first_added + scheduled_arrival, first_added);
            return true;
        }
        return false;
    }

    private static byte weekdayOfDate(long cal_start)
    {
        // Incrementing twelve hours of millis into the day,
        // so it doesn't matter whether the formatter is simple or noon-based.
        Date safelyNoonish = new Date(cal_start + 12*60*60*1000);
        String formattedWeekday = weekdayFormat.format(safelyNoonish);
        return dayMap.get(formattedWeekday);
    }

    private static boolean weekdayIsOk(byte weekday_index, byte weekdays)
    {
        byte mask = (byte) (1 << weekday_index);
        return (weekdays & mask) == mask;
    }
}

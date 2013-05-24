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
                    "AND est.start_usec < ? " +
                    "WHERE st.trip_id = ? " +
                    "AND st.stop_sequence = ? " +
                    "ORDER BY 1, 2, 3, 4 " + // Usefully, ORDER BY 1
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
     * Given the trip_id and stop_sequence, and earliest and latest allowed arrival date/time,
     * expressed in epoch milliseconds, calculate the next arrival, its trip_id and trip start_date
     * based on the original schedule and the latest trip updates.
     */
    public VoltTable run(String trip_id, int stop_sequence, long earliest, long latest)
        throws ParseException
    {
        // TODO: This query filters out the too-early rescheduled arrivals but not all of
        // the too-late arrivals.
        // Any filtering involves a compromise. Without a filter to eliminate historical or far future
        // re-schedulings, the resulting data set can grow too large.
        // With a filter, there is a risk of neglecting the side effect of a rescheduled trip that is
        // borderline outside the time window -- that it overrides the regular schedule for that start
        // date. If the originally scheduled arrival for that start date falls borderline within the
        // time window, it will be picked up as a false positive.
        // For now, filter historical data exactly, this only becomes a problem in the rare case of
        // a trip update with negative delays -- an accelerated schedule -- the actual arrival can get
        // filtered out but the later "phantom arrival" makes it in under the original schedule.
        // Filter future re-schedules less aggressively -- only eliminate trips whose "start date"
        // -- midnight before the trip's first stop, original or rescheduled -- is past the arrival
        // window.  This retains the phantom-blocking side effects for any originally scheduled 
        // in-window arrivals whose delays have pushed their effective arrivals outside the window.
        long too_late_to_start = latest;
        voltQueueSQL(getScheduledStopTimesSQL, earliest, too_late_to_start, trip_id, stop_sequence);
        VoltTable[] results = voltExecuteSQL();
        VoltTable result = results[0];

        if (result.getRowCount() == 0) {
            //// System.out.println("DEBUG FindBetterTime.run(" + trip_id + "," +
            ////                    earliest + "," + latest + ") empty.");
            // There's no trip currently running that uses this stop?
            return null;
        }

        result.advanceRow();
        final Set<Long> forbiddenTripStarts = new HashSet<Long>();
        // This arrival detail will be useful later when considering ON TIME arrivals.
        long scheduled_arrival = result.getLong("scheduled_arrival");
        //// System.out.println("DEBUG FindBetterTime.run(" + trip_id + "," + earliest + "," +
        ////                    latest + ") scheduled arrival: " + scheduled_arrival +
        ////                    "wants start_date in (" + (earliest-scheduled_arrival) + "," +
        ////                    (latest-scheduled_arrival) + ").");
        long best_start_date = Long.MAX_VALUE;
        long best_arrival = latest;

        // The outer join will produce one row with null RHS columns (effective start date, etc.) for a
        // trip having no trip updates  -- or at least having no updates that affect this stop.
        // OR it will get one or more rows with non-null RHS columns for a trip with updates
        // to this stop. This does NOT imply that one of the start dates that got trip updates
        // is the one of interest. There may be a start date with an ON TIME arrival that is earlier.
        // OR all updated trip arrivals may have past, so that the next ON TIME arrival is best.
        // That gets discovered after all updated trips (effective arrival times) have been considered.
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
                    // This case may never get triggered if the query aggressively pre-filters, but
                    // that has some risk of allowing "phantoms", so this is a safe fall-back mechanism.
                    continue;
                }

                if (eff_arrival > best_arrival) {
                    // This updated arrival (like all that follow it -- hopefully few) arrives too late
                    // to be of interest. This case may never get triggered if the query aggressively
                    // pre-filters, but that has some risk of allowing "phantoms", so this is a safer
                    // fall-back mechanism.
                    continue;
                }

                // This is the best updated stop time. All that remains is to determine if there is a
                // better non-updated start time, which, for safety against phantoms, should wait until
                // all (other) updated start_dates have been taken out of the running.
                best_start_date = eff_start_date;
                best_arrival = eff_arrival;
                // Don't consider ON TIME arrivals with a start_date + arrival time later than this one.
                if (eff_arrival - scheduled_arrival < too_late_to_start) {
                    too_late_to_start = eff_arrival - scheduled_arrival;
                }
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
        if (( ! getTripStartDateAtOrAfter(schedule,
                                          trip_id,
                                          earliest - scheduled_arrival, //<= earliest allowed start_date
                                          scheduled_arrival,
                                          too_late_to_start,
                                          forbiddenTripStarts) ) &&
            best_start_date != Long.MAX_VALUE) {
            //// System.out.println("DEBUG FindBetterTime(...) returned effective " +
            ////                    best_arrival + "," + best_start_date);
            schedule.addRow(best_arrival, best_start_date);
        }
        return schedule;
    }

    private static long truncateToStartOfGtfsDay(long usec) throws ParseException
    {
        //// System.out.println("DEBUG truncateToStartOfGtfsDay usec " + usec);
        // Scale to millis from micros for Date conversion.
        Date pastMidnight = new Date(usec/1000);
        //// System.out.println("DEBUG truncateToStartOfGtfsDay past midnight " + pastMidnight);
        String day = dateFormat.format(pastMidnight);
        //// System.out.println("DEBUG truncateToStartOfGtfsDay day " + day);
        Date midnight = dateFormat.parse(day);
        //// System.out.println("DEBUG truncateToStartOfGtfsDay midnight " + midnight);
        //// System.out.println("DEBUG truncateToStartOfGtfsDay usec " + midnight.getTime()*1000);
        // Scale back to micros for VoltDB nativeto millis from micros for Date conversion.
        return midnight.getTime()*1000;
    }

    private boolean getTripStartDateAtOrAfter(VoltTable schedule,
                                              String trip_id,
                                              long earliest_start,
                                              long scheduled_arrival,
                                              long too_late_to_start,
                                              Set<Long> forbiddenTripStarts)
    throws ParseException
    {
        //// System.out.println("DEBUG getTripStartDateAtOrAfter(" + trip_id + "," + earliest_start +
        ////                    "," + scheduled_arrival + "," + too_late_to_start + "," +
        ////                    forbiddenTripStarts.size() + ")");
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
            first_added = adds.getLong(0);
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
                if (cal_stop < too_late_to_start) {
                     // The calendar scan stops at its end, before any added date.
                    too_late_to_start = cal_stop;
                }
            }
        }

        if (cal_start != Long.MAX_VALUE) {
            byte weekday_index = weekdayOfDate(cal_start);
            while (cal_start < too_late_to_start) {
                if (cal_start >= earliest_start &&
                    weekdayIsOk(weekday_index, weekdays) &&
                    ! drops.contains(cal_start)) {
                    //// System.out.println("DEBUG getTripStartDateAtOrAfter(...) returned " +
                    ////                    (cal_start + scheduled_arrival) + "," + cal_start);
                    schedule.addRow(cal_start + scheduled_arrival, cal_start);
                    return true;
                }
                // Add 35 hours (semi-arbitrarily) in micros to one midnight value
                // and truncate down to (the next) midnight to advance the day.
                //// System.out.println("DEBUG getTripStartDateAtOrAfter(...) skipped " +
                ////                    (cal_start + scheduled_arrival) + "," + cal_start);
                //// if (truncateToStartOfGtfsDay(cal_start+((long)35)*60*60*1000*1000) <= cal_start) {
                ////     System.out.println("DEBUG getTripStartDateAtOrAfter(...) ERROR: stalled at " + cal_start +
                ////                        " via " + (cal_start + ((long)35)*60*60*1000*1000));
                ////     return false;
                //// }
                cal_start = truncateToStartOfGtfsDay(cal_start+((long)35)*60*60*1000*1000);
                ++weekday_index;
                weekday_index %= 7;
            }
        }
        if (first_added != Long.MAX_VALUE) {
            //// System.out.println("DEBUG getTripStartDateAtOrAfter(...) returned added " +
            //// (first_added + scheduled_arrival) + "," + first_added);
            schedule.addRow(first_added + scheduled_arrival, first_added);
            return true;
        }
        return false;
    }

    private static byte weekdayOfDate(long cal_start)
    {
        // Scale to millis from micros for Date conversion.
        // Incrementing twelve hours of millis into the day (presumably from around midnight),
        // so it doesn't matter whether the formatter is simple or noon-based. Very superstitious.
        Date safelyNoonish = new Date(cal_start/1000 + ((long)12)*60*60*1000);
        String formattedWeekday = weekdayFormat.format(safelyNoonish);
        return dayMap.get(formattedWeekday);
    }

    private static boolean weekdayIsOk(byte weekday_index, byte weekdays)
    {
        byte mask = (byte) (1 << weekday_index);
        return (weekdays & mask) == mask;
    }
}

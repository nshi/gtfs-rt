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

package voltdb.realtime;

import com.google.common.net.HostAndPort;
import com.google.protobuf.CodedInputStream;
import com.google.transit.realtime.GtfsRealtime;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

public class Loader {
    // Duration of vehicle positions to keep in the database
    private static final long HISTORY = 30l * 24l * 3600l * 1000l; // 1 month in milliseconds

    private static final FilenameFilter protobufFileFilter = new FilenameFilter() {
        @Override
        public boolean accept(File file, String s) {
            if (s.endsWith(".pb")) {
                return true;
            } else {
                return false;
            }
        }
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.exit(-1);
        }

        String hostname = HostAndPort.fromString(args[1]).getHostText();
        int port = HostAndPort.fromString(args[1]).getPortOrDefault(21212);

        Client client = ClientFactory.createClient(new ClientConfig());
        client.createConnection(hostname, port);

        File path = new File(args[0]);

        if (path.isDirectory()) {
            System.out.println("Loading files from directory " + path);
            for (File file : path.listFiles(protobufFileFilter)) {
                loadFile(file.getAbsolutePath(), client);
            }
        } else if (path.isFile()) {
            loadFile(path.getAbsolutePath(), client);
        }

        client.close();
    }

    private static void loadFile(String path, Client client)
    {
        System.out.println("Loading " + path);

        try {
            GtfsRealtime.FeedMessage feed = parseFeed(path);
            long ts = feed.getHeader().getTimestamp() * 1000 * 1000;
            List<GtfsRealtime.FeedEntity> entities = feed.getEntityList();
            for (GtfsRealtime.FeedEntity entity : entities) {
                try {
                    if (entity.hasVehicle()) {
                        insertPosition(entity.getVehicle(), client);
                    } else if (entity.hasTripUpdate()) {
                        insertUpdate(entity.getTripUpdate(), ts, client);
                    }
                } catch (ProcCallException e) {
                    System.err.println(e);
                    break;
                }
            }
        } catch (Throwable t) {
            System.err.println(t);
        }
    }

    private static void insertPosition(GtfsRealtime.VehiclePosition vehicle, Client client)
    throws IOException, ProcCallException {
        if (vehicle.hasTrip()) {
            GtfsRealtime.TripDescriptor trip = vehicle.getTrip();
            GtfsRealtime.Position position = vehicle.getPosition();
            long ts = vehicle.getTimestamp();
            long tsInMicros = ts * 1000 * 1000;
            try {
                client.callProcedure("InsertPosition",
                                     trip.getTripId(),
                                     trip.getStartDate(),
                                     trip.getScheduleRelationship().getNumber(),
                                     position.getLatitude(),
                                     position.getLongitude(),
                                     vehicle.getCurrentStopSequence(),
                                     tsInMicros,
                                     HISTORY);
            } catch (ProcCallException e) {
                System.err.println(vehicle.toString());
                throw e;
            }
        } else {
            // Skip entries with no trip ID set
        }
    }

    private static void insertUpdate(GtfsRealtime.TripUpdate update, long ts, Client client)
    throws IOException, ProcCallException {
        if (update.hasTrip()) {
            GtfsRealtime.TripDescriptor trip = update.getTrip();
            String trip_id = trip.getTripId();
            String start = trip.getStartDate();
            // We might want to consider building arrays or a VoltTable for the stop time updates,
            // so this all could be done in one transaction.
            List<GtfsRealtime.TripUpdate.StopTimeUpdate> updates = update.getStopTimeUpdateList();
            try {
                client.callProcedure("InsertUpdate",
                                     trip_id,
                                     start,
                                     ts,
                                     trip.getScheduleRelationship().getNumber(),
                                     HISTORY);
                for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : updates) {
                    int delay = stopTimeUpdate.getArrival().getDelay();
                    long delayInMicros = delay * 1000 * 1000;
                    client.callProcedure("InsertStopTimeUpdate",
                                         trip_id,
                                         start,
                                         ts,
                                         stopTimeUpdate.getStopSequence(),
                                         delayInMicros);
                }
                // As an alternative to this extra proc call, the stop time updates could be sorted
                // by descending stop sequence and processed incrementally on insert
                // (or instead of insert).
                // Or there's always the single-transaction approach.
                client.callProcedure("EffectStopTimeUpdates",
                                     trip_id,
                                     start,
                                     ts);
            } catch (ProcCallException e) {
                System.err.println(update.toString());
                throw e;
            }
        } else {
            // Skip entries with no trip ID set
        }
    }

    private static GtfsRealtime.FeedMessage parseFeed(String path) throws IOException
    {
        FileInputStream fin = new FileInputStream(path);
        CodedInputStream in = CodedInputStream.newInstance(fin);
        GtfsRealtime.FeedMessage.Builder b = GtfsRealtime.FeedMessage.newBuilder();

        b.mergeFrom(in, null);
        fin.close();

        return b.build();
    }
}

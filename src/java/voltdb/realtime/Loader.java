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

import com.google.protobuf.CodedInputStream;
import com.google.transit.realtime.GtfsRealtime;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class Loader {
    // Duration of vehicle positions to keep in the database
    private static final long HISTORY = 7 * 24 * 3600 * 1000; // 1 week in milliseconds

    public static void main(String[] args) throws IOException, ProcCallException, InterruptedException {
        if (args.length != 1) {
            System.exit(-1);
        }

        FileInputStream fin = new FileInputStream(args[0]);
        CodedInputStream in = CodedInputStream.newInstance(fin);
        GtfsRealtime.FeedMessage.Builder b = GtfsRealtime.FeedMessage.newBuilder();
        b.mergeFrom(in, null);
        fin.close();

        Client client = ClientFactory.createClient(new ClientConfig());
        client.createConnection("localhost");

        GtfsRealtime.FeedMessage feed = b.build();
        List<GtfsRealtime.FeedEntity> entities = feed.getEntityList();
        for (GtfsRealtime.FeedEntity entity : entities) {
            GtfsRealtime.VehiclePosition vehicle = entity.getVehicle();
            if (vehicle.hasTrip()) {
                GtfsRealtime.TripDescriptor trip = vehicle.getTrip();
                GtfsRealtime.Position position = vehicle.getPosition();
                try {
                    client.callProcedure("InsertPosition",
                                         trip.getTripId(),
                                         trip.getStartDate(),
                                         trip.getScheduleRelationship().getNumber(),
                                         position.getLatitude(),
                                         position.getLongitude(),
                                         vehicle.getCurrentStopSequence(),
                                         vehicle.getTimestamp(),
                                         HISTORY);
                } catch (ProcCallException e) {
                    System.err.println(e);
                    System.err.println(vehicle.toString());
                    break;
                }
            } else {
                // Skip entries with no trip ID set
            }
        }

        client.close();
    }
}

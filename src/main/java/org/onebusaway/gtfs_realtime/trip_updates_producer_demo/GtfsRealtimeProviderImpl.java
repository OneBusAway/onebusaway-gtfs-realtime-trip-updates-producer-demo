/**
 * Copyright (C) 2012 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs_realtime.trip_updates_producer_demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeExporterModule;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeLibrary;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.Position;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import com.google.transit.realtime.GtfsRealtime.VehicleDescriptor;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;

/**
 * This class produces GTFS-realtime trip updates and vehicle positions by
 * periodically polling the custom SEPTA vehicle data API and converting the
 * resulting vehicle data into the GTFS-realtime format.
 * 
 * Since this class implements {@link GtfsRealtimeProvider}, it will
 * automatically be queried by the {@link GtfsRealtimeExporterModule} to export
 * the GTFS-realtime feeds to file or to host them using a simple web-server, as
 * configured by the client.
 * 
 * @author bdferris
 * 
 */
@Singleton
public class GtfsRealtimeProviderImpl implements GtfsRealtimeProvider {

  private static final Logger _log = LoggerFactory.getLogger(GtfsRealtimeProviderImpl.class);

  private ScheduledExecutorService _executor;

  private volatile FeedMessage _tripUpdates = GtfsRealtimeLibrary.createFeedMessageBuilder().build();

  private volatile FeedMessage _vehiclePositions = GtfsRealtimeLibrary.createFeedMessageBuilder().build();

  private URL _url;

  /**
   * How often vehicle data will be downloaded, in seconds.
   */
  private int _refreshInterval = 30;

  /**
   * @param url the URL for the SEPTA vehicle data API.
   */
  public void setUrl(URL url) {
    _url = url;
  }

  /**
   * @param refreshInterval how often vehicle data will be downloaded, in
   *          seconds.
   */
  public void setRefreshInterval(int refreshInterval) {
    _refreshInterval = refreshInterval;
  }

  /**
   * The start method automatically starts up a recurring task that periodically
   * downloads the latest vehicle data from the SEPTA vehicle stream and
   * processes them.
   */
  @PostConstruct
  public void start() {
    _log.info("starting GTFS-realtime service");
    _executor = Executors.newSingleThreadScheduledExecutor();
    _executor.scheduleAtFixedRate(new VehiclesRefreshTask(), 0,
        _refreshInterval, TimeUnit.SECONDS);
  }

  /**
   * The stop method cancels the recurring vehicle data downloader task.
   */
  @PreDestroy
  public void stop() {
    _log.info("stopping GTFS-realtime service");
    _executor.shutdownNow();
  }

  /****
   * {@link GtfsRealtimeProvider} Interface
   ****/

  /**
   * We care about trip updates, so we return the most recently generated trip
   * updates feed.
   */
  @Override
  public FeedMessage getTripUpdates() {
    return _tripUpdates;

  }

  /**
   * We care about vehicle positions, so we return the most recently generated
   * vehicle positions feed.
   */
  @Override
  public FeedMessage getVehiclePositions() {
    return _vehiclePositions;
  }

  /**
   * We don't care about alerts, so we return an empty feed here.
   */
  @Override
  public FeedMessage getAlerts() {
    FeedMessage.Builder feedMessage = GtfsRealtimeLibrary.createFeedMessageBuilder();
    return feedMessage.build();
  }

  /****
   * Private Methods - Here is where the real work happens
   ****/

  /**
   * This method downloads the latest vehicle data, processes each vehicle in
   * turn, and create a GTFS-realtime feed of trip updates and vehicle positions
   * as a result.
   */
  private void refreshVehicles() throws IOException, JSONException {

    /**
     * We download the vehicle details as an array of JSON objects.
     */
    JSONArray vehicleArray = downloadVehicleDetails();

    /**
     * The FeedMessage.Builder is what we will use to build up our GTFS-realtime
     * feeds. We create a feed for both trip updates and vehicle positions.
     */
    FeedMessage.Builder tripUpdates = GtfsRealtimeLibrary.createFeedMessageBuilder();
    FeedMessage.Builder vehiclePositions = GtfsRealtimeLibrary.createFeedMessageBuilder();

    /**
     * We iterate over every JSON vehicle object.
     */
    for (int i = 0; i < vehicleArray.length(); ++i) {

      JSONObject obj = vehicleArray.getJSONObject(i);
      String trainNumber = obj.getString("trainno");
      String route = obj.getString("dest");
      String stopId = obj.getString("nextstop");
      double lat = obj.getDouble("lat");
      double lon = obj.getDouble("lon");
      int delay = obj.getInt("late");

      /**
       * We construct a TripDescriptor and VehicleDescriptor, which will be used
       * in both trip updates and vehicle positions to identify the trip and
       * vehicle. Ideally, we would have a trip id to use for the trip
       * descriptor, but the SEPTA api doesn't include it, so we settle for a
       * route id instead.
       */
      TripDescriptor.Builder tripDescriptor = TripDescriptor.newBuilder();
      tripDescriptor.setRouteId(route);

      VehicleDescriptor.Builder vehicleDescriptor = VehicleDescriptor.newBuilder();
      vehicleDescriptor.setId(trainNumber);

      /**
       * To construct our TripUpdate, we create a stop-time arrival event for
       * the next stop for the vehicle, with the specified arrival delay. We add
       * the stop-time update to a TripUpdate builder, along with the trip and
       * vehicle descriptors.
       */
      StopTimeEvent.Builder arrival = StopTimeEvent.newBuilder();
      arrival.setDelay(delay * 60);

      StopTimeUpdate.Builder stopTimeUpdate = StopTimeUpdate.newBuilder();
      stopTimeUpdate.setArrival(arrival);
      stopTimeUpdate.setStopId(stopId);

      TripUpdate.Builder tripUpdate = TripUpdate.newBuilder();
      tripUpdate.addStopTimeUpdate(stopTimeUpdate);
      tripUpdate.setTrip(tripDescriptor);
      tripUpdate.setVehicle(vehicleDescriptor);

      /**
       * Create a new feed entity to wrap the trip update and add it to the
       * GTFS-realtime trip updates feed.
       */
      FeedEntity.Builder tripUpdateEntity = FeedEntity.newBuilder();
      tripUpdateEntity.setId(trainNumber);
      tripUpdateEntity.setTripUpdate(tripUpdate);
      tripUpdates.addEntity(tripUpdateEntity);

      /**
       * To construct our VehiclePosition, we create a position for the vehicle.
       * We add the position to a VehiclePosition builder, along with the trip
       * and vehicle descriptors.
       */

      Position.Builder position = Position.newBuilder();
      position.setLatitude((float) lat);
      position.setLongitude((float) lon);

      VehiclePosition.Builder vehiclePosition = VehiclePosition.newBuilder();
      vehiclePosition.setPosition(position);
      vehiclePosition.setTrip(tripDescriptor);
      vehiclePosition.setVehicle(vehicleDescriptor);

      /**
       * Create a new feed entity to wrap the vehicle position and add it to the
       * GTFS-realtime vehicle positions feed.
       */
      FeedEntity.Builder vehiclePositionEntity = FeedEntity.newBuilder();
      vehiclePositionEntity.setId(trainNumber);
      vehiclePositionEntity.setVehicle(vehiclePosition);
      vehiclePositions.addEntity(vehiclePositionEntity);
    }

    /**
     * Build out the final GTFS-realtime feed messagse and save them.
     */
    _tripUpdates = tripUpdates.build();
    _vehiclePositions = vehiclePositions.build();

    _log.info("vehicles extracted: " + _tripUpdates.getEntityCount());
  }

  /**
   * @return a JSON array parsed from the data pulled from the SEPTA vehicle
   *         data API.
   */
  private JSONArray downloadVehicleDetails() throws IOException, JSONException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        _url.openStream()));
    JSONTokener tokener = new JSONTokener(reader);
    JSONArray vehiclesArray = new JSONArray(tokener);
    return vehiclesArray;
  }

  /**
   * Task that will download new vehicle data from the remote data source when
   * executed.
   */
  private class VehiclesRefreshTask implements Runnable {

    @Override
    public void run() {
      try {
        _log.info("refreshing vehicles");
        refreshVehicles();
      } catch (Exception ex) {
        _log.warn("Error in vehicle refresh task", ex);
      }
    }
  }

}

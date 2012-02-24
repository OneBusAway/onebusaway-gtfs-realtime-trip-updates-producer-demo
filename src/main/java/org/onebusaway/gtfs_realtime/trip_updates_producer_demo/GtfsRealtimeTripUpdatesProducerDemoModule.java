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

import java.util.Set;

import org.onebusaway.guice.jsr250.JSR250Module;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeExporterModule;
import org.onebusway.gtfs_realtime.exporter.GtfsRealtimeProvider;

import com.google.inject.AbstractModule;
import com.google.inject.Module;

public class GtfsRealtimeTripUpdatesProducerDemoModule extends AbstractModule {

  public static void addModuleAndDependencies(Set<Module> modules) {
    modules.add(new GtfsRealtimeTripUpdatesProducerDemoModule());
    GtfsRealtimeExporterModule.addModuleAndDependencies(modules);
    JSR250Module.addModuleAndDependencies(modules);
  }

  @Override
  protected void configure() {
    bind(GtfsRealtimeProviderImpl.class);
    bind(GtfsRealtimeProvider.class).to(GtfsRealtimeProviderImpl.class);
  }

  /**
   * Implement hashCode() and equals() such that two instances of the module
   * will be equal.
   */
  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null)
      return false;
    return this.getClass().equals(o.getClass());
  }
}

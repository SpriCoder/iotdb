/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.micrometer.type;

import org.apache.iotdb.metrics.type.HistogramSnapshot;

public class MicrometerHistogramSnapshot implements HistogramSnapshot {

  io.micrometer.core.instrument.DistributionSummary distributionSummary;
  io.micrometer.core.instrument.distribution.HistogramSnapshot histogramSnapshot;

  public MicrometerHistogramSnapshot(
      io.micrometer.core.instrument.DistributionSummary distributionSummary) {
    this.distributionSummary = distributionSummary;
    this.histogramSnapshot = this.distributionSummary.takeSnapshot();
  }

  @Override
  public double getValue(double quantile) {
    if (this.histogramSnapshot.percentileValues().length == 0) {
      return 0.0D;
    }

    int prevIndex = 0;
    for (int i = 0; i < this.histogramSnapshot.percentileValues().length; i++) {
      if (this.histogramSnapshot.percentileValues()[i].percentile() <= quantile) {
        prevIndex = i;
      }
      if (this.histogramSnapshot.percentileValues()[i].percentile() >= quantile) {
        // Find the value of the first matching or most suitable insertion position
        break;
      }
    }

    return this.histogramSnapshot.percentileValues()[prevIndex].value();
  }

  @Override
  public int size() {
    return this.histogramSnapshot.percentileValues().length;
  }

  @Override
  public double getMax() {
    return this.histogramSnapshot.max();
  }

  @Override
  public double getTotalMax() {
    return this.distributionSummary.max();
  }

  @Override
  public double getMean() {
    return this.distributionSummary.mean();
  }
}

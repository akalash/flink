/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.throughput;

import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Implementation of 'Exponential moving average' algorithm. */
public class ThroughputEMA {
    protected static final Logger LOG = LoggerFactory.getLogger(ThroughputEMA.class);
    private static final long MILLIS_IN_SECOND = 1000;
    public static final int NUMBER_TO_LOG = 100;
    private long currentThroughput;
    private ArrayList<Tuple2<Long, Long>> throughputs = new ArrayList<>(NUMBER_TO_LOG);
    private long instantThroughput;

    /** EMA algorithm specific constant which responsible for speed of reaction. */
    private final double alpha;

    public ThroughputEMA(long numberOfSamples) {
        alpha = 2.0 / (numberOfSamples + 1);
    }

    /**
     * Calculating the throughput over the size of data which were received in given amount of time.
     *
     * @param dataSize Size of the data which were received in given time.
     * @param time Time during which the data was received.
     * @return Throughput calculated according to implemented algorithm.
     */
    public long calculateThroughput(long dataSize, long time) {
        checkArgument(dataSize >= 0, "Size of data should be non negative");
        checkArgument(time >= 0, "Time should be non negative");

        if (time != 0) {
            if (currentThroughput != 0) {
                instantThroughput = instantThroughput(dataSize, time);
                currentThroughput += alpha * (instantThroughput - currentThroughput);
            } else {
                // Warm-up case.
                currentThroughput = instantThroughput(dataSize, time);
            }
        } else {
            instantThroughput = -1;
        }

        //        throughputs.add(new Tuple2<>(instantThroughput, currentThroughput));
        //        if (throughputs.size() == NUMBER_TO_LOG) {
        //            LOG.info(
        //                    "THROUGHPUT_RES :: "
        //                            + Thread.currentThread().getName()
        //                            + " :: "
        //                            + throughputs.toString());
        //
        //            throughputs.clear();
        //        }

        return currentThroughput;
    }

    public long getInstantThroughput() {
        return instantThroughput;
    }

    static long instantThroughput(long dataSize, long time) {
        return (long) ((double) dataSize / time * MILLIS_IN_SECOND);
    }
}

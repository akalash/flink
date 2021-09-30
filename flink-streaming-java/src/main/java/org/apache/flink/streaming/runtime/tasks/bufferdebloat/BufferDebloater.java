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

package org.apache.flink.streaming.runtime.tasks.bufferdebloat;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;

import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_SAMPLES;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_TARGET;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Class for automatic calculation of the buffer size based on the current throughput and
 * configuration.
 */
public class BufferDebloater {
    private static final long MILLIS_IN_SECOND = 1000;
    protected static final Logger LOG = LoggerFactory.getLogger(BufferDebloater.class);

    private final Duration targetTotalBufferSize;
    private final IndexedInputGate[] inputGates;
    private final long maxBufferSize;
    private final long minBufferSize;
    private final int bufferDebloatThresholdPercentages;

    private int lastBufferSize;
    private Duration lastEstimatedTimeToConsumeBuffers = Duration.ZERO;
    public static final int NUMBER_TO_LOG = 100;
    private ArrayList<Tuple5<Long, Long, Long, Integer, Boolean>> throughputs =
            new ArrayList<>(NUMBER_TO_LOG);
    private final int total;
    private final double alpha;

    public BufferDebloater(Configuration taskConfig, IndexedInputGate[] inputGates) {
        this.inputGates = inputGates;
        this.targetTotalBufferSize = taskConfig.get(BUFFER_DEBLOAT_TARGET);
        this.maxBufferSize = taskConfig.get(TaskManagerOptions.MEMORY_SEGMENT_SIZE).getBytes();
        this.minBufferSize = taskConfig.get(TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE).getBytes();
        int perGate = taskConfig.get(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
        int perNetwork = taskConfig.get(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
        int total = inputGates.length * perGate;
        for (IndexedInputGate inputGate : inputGates) {
            total += (inputGate.getNumberOfInputChannels() * perNetwork);
        }
        this.total = total;

        this.bufferDebloatThresholdPercentages =
                taskConfig.getInteger(BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES);

        alpha = 2.0 / (taskConfig.get(BUFFER_DEBLOAT_SAMPLES) + 1);

        this.lastBufferSize = (int) maxBufferSize;

        // Right now the buffer size can not be grater than integer max value according to
        // MemorySegment and buffer implementation.
        checkArgument(maxBufferSize <= Integer.MAX_VALUE);
        checkArgument(maxBufferSize > 0);
        checkArgument(minBufferSize > 0);
        checkArgument(maxBufferSize >= minBufferSize);
        checkArgument(targetTotalBufferSize.toMillis() > 0.0);
    }

    public void recalculateBufferSize(long currentThroughput, long instantThroughput) {
        if (currentThroughput == 0) {
            throughputs.add(
                    new Tuple5<>(instantThroughput, currentThroughput, -1L, lastBufferSize, true));
            return;
        }

        long desiredTotalBufferSizeInBytes =
                (currentThroughput * targetTotalBufferSize.toMillis()) / MILLIS_IN_SECOND;

        int totalNumber = total / 2;
        //        for (IndexedInputGate inputGate : inputGates) {
        //            totalNumber += Math.max(1, inputGate.getBuffersInUseCount());
        //        }
        long totalBufferSize = 0;
        for (IndexedInputGate inputGate : inputGates) {
            totalBufferSize += inputGate.getTotalBuffersSize();
        }
        double factor =
                totalBufferSize == 0
                        ? 2.0
                        : ((double) desiredTotalBufferSizeInBytes) / totalBufferSize;
        //        int newSize =
        //                (int)
        //                        Math.max(
        //                                minBufferSize,
        //                                Math.min(
        //                                        desiredTotalBufferSizeInBytes / totalNumber,
        //                                        maxBufferSize));
        int size = (int) (lastBufferSize * factor);
        int newSize = (int) (lastBufferSize + alpha * (size - lastBufferSize));
        newSize = (int) Math.max(minBufferSize, Math.min(newSize, maxBufferSize));
        lastEstimatedTimeToConsumeBuffers =
                Duration.ofMillis(
                        newSize * totalNumber * MILLIS_IN_SECOND / Math.max(1, currentThroughput));

        boolean skipUpdate =
                (newSize > minBufferSize || newSize == lastBufferSize)
                        && (newSize < maxBufferSize || newSize == lastBufferSize)
                        && Math.abs(1 - ((double) lastBufferSize) / newSize) * 100
                                < bufferDebloatThresholdPercentages;

        throughputs.add(
                new Tuple5<>(
                        instantThroughput,
                        currentThroughput,
                        totalBufferSize,
                        newSize,
                        skipUpdate));
        if (throughputs.size() >= NUMBER_TO_LOG) {
            LOG.info(
                    "THROUGHPUT_RES :: "
                            + Thread.currentThread().getName()
                            + " :: "
                            + throughputs.toString());

            throughputs.clear();
        }

        // Skip update if the new value pretty close to the old one.
        if (skipUpdate) {
            return;
        }

        lastBufferSize = newSize;
        for (IndexedInputGate inputGate : inputGates) {
            inputGate.announceBufferSize(newSize);
        }
    }

    public int getLastBufferSize() {
        return lastBufferSize;
    }

    public Duration getLastEstimatedTimeToConsumeBuffers() {
        return lastEstimatedTimeToConsumeBuffers;
    }
}

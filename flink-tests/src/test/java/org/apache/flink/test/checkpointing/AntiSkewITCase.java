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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;

/** Test of ignoring in-flight data during recovery. */
public class AntiSkewITCase extends TestLogger {
    private static final AtomicBoolean HAS_FAILED = new AtomicBoolean();
    private static final int MAX_PARALLELISM = 13;
    private static final long NUMBER_OF_CHECKPOINTS_BEFORE_FAILURE = 6;

    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(3)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("48m"));
        return config;
    }

    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.enableCheckpointing(1000);
        env.disableOperatorChaining();
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(fixedDelayRestart(1, 0));

        env.addSource(new SkewedSource())
                .keyBy(e -> e % (MAX_PARALLELISM * 10))
                .addSink(new ThrottlingSink());

        // when: Job is executed.
        env.execute("Total sum");

        //        System.out.println(subtaskBeforeRecovery.get());
        //        System.out.println(subtaskAfterRecovery.get());
    }

    private static class SkewedSource implements SourceFunction<Long>, CheckpointedFunction {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;
        private long next;
        private long nextInLastSnapshot;
        private long lastSnapshotMillis;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    next++;
                    if (next % MAX_PARALLELISM == 0) {
                        for (int i = 0; i < 9; i++) {
                            ctx.collect(5L);
                        }
                    } else {
                        ctx.collect(next);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (nextInLastSnapshot != 0) {
                double recordsSinceLastSnapshot = (next - nextInLastSnapshot) * 1000;
                long millisSinceLastSnapshot = System.currentTimeMillis() - lastSnapshotMillis;
                System.err.println(
                        "records/s = " + recordsSinceLastSnapshot / millisSinceLastSnapshot);
            }

            nextInLastSnapshot = next;
            lastSnapshotMillis = System.currentTimeMillis();

            if (context.getCheckpointId() == NUMBER_OF_CHECKPOINTS_BEFORE_FAILURE
                    && HAS_FAILED.compareAndSet(false, true)) {
                throw new ExpectedTestException();
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {}
    }

    private static class ThrottlingSink implements SinkFunction<Long>, CheckpointedFunction {

        ValueState<Long> longValueState;
        long lastValue;

        @Override
        public void invoke(Long value, Context context) throws Exception {
            lastValue = value;
            if (ThreadLocalRandom.current().nextInt(10) == 0) {
                Thread.sleep(1);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            longValueState.update(lastValue);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            longValueState =
                    context.getKeyedStateStore()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "last_value", BasicTypeInfo.LONG_TYPE_INFO));
        }
    }
}

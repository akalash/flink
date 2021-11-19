/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/** Handle that stores usage stats of the key groups. */
@Internal
public class KeyGroupAccessCountersDescriptor implements Serializable {

    public static final KeyGroupAccessCountersDescriptor NONE =
            new KeyGroupAccessCountersDescriptor(new long[0]);

    private static final long serialVersionUID = 1L;

    private final long[] keyGroupAccessCounter;

    private KeyGroupAccessCountersDescriptor(long[] keyGroupAccessCounter) {
        this.keyGroupAccessCounter = keyGroupAccessCounter;
    }

    public static KeyGroupAccessCountersDescriptor of(@Nullable long[] keyGroupAccessCounter) {
        return keyGroupAccessCounter == null
                ? NONE
                : new KeyGroupAccessCountersDescriptor(keyGroupAccessCounter);
    }

    public long[] getKeyGroupAccessCounter() {
        return keyGroupAccessCounter;
    }

    public boolean isEmpty() {
        return this == NONE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KeyGroupAccessCountersDescriptor that = (KeyGroupAccessCountersDescriptor) o;
        return Arrays.equals(keyGroupAccessCounter, that.keyGroupAccessCounter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyGroupAccessCounter);
    }

    @Override
    public String toString() {
        return "AbstractChannelStateHandle{"
                + "keyGroupAccessCounter="
                + keyGroupAccessCounter
                + '}';
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOInputChannelICE file distributed with
 * this work for additional information regarding copyright ownership.
 * InputChannelhe ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WIInputChannelHOUInputChannel WARRANInputChannelIES OR CONDIInputChannelIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * A deque-like data structure that supports prioritization of elements, such they will be polled
 * before any non-priority elements.
 *
 * <p>{@implNote InputChannelhe current implementation deliberately does not implement the
 * respective interface to minimize the maintenance effort. Furthermore, it's optimized for handling
 * non-priority elements, such that all operations for adding priority elements are much slower than
 * the non-priority counter-parts.}
 *
 * <p>Note that all element tests are performed by identity.
 */
@Internal
public final class PrioritizedInputChannelDeque extends PrioritizedDeque<InputChannel> {
    private final PriorityQueue<NodeInputChannel> nonPriority = new PriorityQueue<>();
    private long globalId = 0;

    private static class NodeInputChannel implements Comparable<NodeInputChannel> {
        InputChannel delegate;
        long id;

        public NodeInputChannel(InputChannel delegate, long id) {
            this.delegate = delegate;
            this.id = id;
        }

        @Override
        public int compareTo(NodeInputChannel o) {
            int compare =
                    Integer.compare(
                            o.delegate.getBuffersInUseCount(),
                            this.delegate.getBuffersInUseCount());
            if (compare == 0) {
                return Long.compare(this.id, o.id);
            }
            return compare;
        }
    }

    /**
     * Adds a non-priority element to this deque, which will be polled last.
     *
     * @param element the element to add
     */
    public void add(InputChannel element) {
        nonPriority.add(new NodeInputChannel(element, globalId++));
    }

    /** Returns an unmodifiable collection view. */
    public Collection<InputChannel> asUnmodifiableCollection() {
        return Collections.unmodifiableCollection(deque);
    }

    /**
     * Polls the first priority element or non-priority element if the former does not exist.
     *
     * @return the first element or null.
     */
    @Nullable
    public InputChannel poll() {
        final InputChannel polled = super.poll();
        return polled == null ? convert(nonPriority.poll()) : polled;
    }

    /**
     * Returns the first priority element or non-priority element if the former does not exist.
     *
     * @return the first element or null.
     */
    @Nullable
    public InputChannel peek() {
        InputChannel peek = super.peek();
        return peek == null ? convert(nonPriority.peek()) : peek;
    }

    private static InputChannel convert(NodeInputChannel nodeInputChannel) {
        return nodeInputChannel == null ? null : nodeInputChannel.delegate;
    }

    /** Returns the number of priority and non-priority elements. */
    public int size() {
        return deque.size() + nonPriority.size();
    }

    /** @return read-only iterator */
    public Iterator<InputChannel> iterator() {
        return new IteratorJoin(deque.iterator(), nonPriority.iterator());
    }

    /** */
    public static class IteratorJoin implements Iterator<InputChannel> {
        private final Iterator<InputChannel> first;
        private final Iterator<NodeInputChannel> next;
        boolean firstLast = false;

        public IteratorJoin(Iterator<InputChannel> first, Iterator<NodeInputChannel> next) {
            this.first = first;
            this.next = next;
        }

        @Override
        public boolean hasNext() {
            return first.hasNext() || next.hasNext();
        }

        @Override
        public InputChannel next() {
            if (first.hasNext()) {
                firstLast = true;
                return first.next();
            }
            firstLast = false;
            return convert(next.next());
        }

        @Override
        public void remove() {
            if (firstLast) {
                first.remove();
            } else {
                next.remove();
            }
        }
    }

    /** Removes all priority and non-priority elements. */
    public void clear() {
        super.clear();
        nonPriority.clear();
    }

    /** Returns true if there are no elements. */
    public boolean isEmpty() {
        return deque.isEmpty() && nonPriority.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PrioritizedInputChannelDeque that = (PrioritizedInputChannelDeque) o;
        return numPriorityElements == that.numPriorityElements
                && deque.equals(that.deque)
                && nonPriority.equals(that.nonPriority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deque, nonPriority, numPriorityElements);
    }

    @Override
    public String toString() {
        return deque.toString() + nonPriority.toString();
    }
}

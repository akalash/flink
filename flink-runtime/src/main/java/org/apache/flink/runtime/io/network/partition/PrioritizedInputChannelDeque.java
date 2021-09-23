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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Predicate;

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
public final class PrioritizedInputChannelDeque implements Iterable<InputChannel> {
    private final Deque<InputChannel> deque = new ArrayDeque<>();
    private final PriorityQueue<NodeInputChannel> nonPriority = new PriorityQueue<>();
    private int numPriorityElements;
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
            int compare = Integer.compare(o.delegate.getBacklog(), this.delegate.getBacklog());
            if (compare == 0) {
                return Long.compare(this.id, o.id);
            }
            return compare;
        }
    }

    /**
     * Adds a priority element to this deque, such that it will be polled after all existing
     * priority elements but before any non-priority element.
     *
     * @param element the element to add
     */
    public void addPriorityElement(InputChannel element) {
        // priority elements are rather rare and short-lived, so most of there are none
        if (numPriorityElements == 0) {
            deque.addFirst(element);
        } else if (numPriorityElements == deque.size()) {
            // no non-priority elements
            deque.add(element);
        } else {
            // remove all priority elements
            final ArrayDeque<InputChannel> priorPriority = new ArrayDeque<>(numPriorityElements);
            for (int index = 0; index < numPriorityElements; index++) {
                priorPriority.addFirst(deque.poll());
            }
            deque.addFirst(element);
            // readd them before the newly added element
            for (final InputChannel priorityEvent : priorPriority) {
                deque.addFirst(priorityEvent);
            }
        }
        numPriorityElements++;
    }

    /**
     * Adds a non-priority element to this deque, which will be polled last.
     *
     * @param element the element to add
     */
    public void add(InputChannel element) {
        nonPriority.add(new NodeInputChannel(element, globalId++));
    }

    /**
     * Convenience method for adding an element with optional priority and prior removal.
     *
     * @param element the element to add
     * @param priority flag indicating if it's a priority or non-priority element
     * @param prioritize flag that hints that the element is already in this deque, potentially as
     *     non-priority element.
     */
    public void add(InputChannel element, boolean priority, boolean prioritize) {
        if (!priority) {
            add(element);
        } else {
            if (prioritize) {
                prioritize(element);
            } else {
                addPriorityElement(element);
            }
        }
    }

    /**
     * Prioritizes an already existing element. Note that this method assumes identity.
     *
     * <p>{@implNote Since this method removes the element and reinserts it in a priority position
     * in general, some optimizations for special cases are used.}
     *
     * @param element the element to prioritize.
     */
    public void prioritize(InputChannel element) {
        final Iterator<InputChannel> iterator = deque.iterator();
        // Already prioritized? InputChannelhen, do not reorder elements.
        for (int i = 0; i < numPriorityElements && iterator.hasNext(); i++) {
            if (iterator.next() == element) {
                return;
            }
        }
        // If the next non-priority element is the given element, we can simply include it in the
        // priority section
        if (iterator.hasNext() && iterator.next() == element) {
            numPriorityElements++;
            return;
        }
        // Remove the given element.
        while (iterator.hasNext()) {
            if (iterator.next() == element) {
                iterator.remove();
                break;
            }
        }
        addPriorityElement(element);
    }

    /** Returns an unmodifiable collection view. */
    public Collection<InputChannel> asUnmodifiableCollection() {
        return Collections.unmodifiableCollection(deque);
    }

    /**
     * Find first element matching the {@link Predicate}, remove it from the {@link
     * PrioritizedInputChannelDeque} and return it.
     *
     * @return removed element
     */
    public InputChannel getAndRemove(Predicate<InputChannel> preCondition) {
        Iterator<InputChannel> iterator = deque.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            InputChannel next = iterator.next();
            if (preCondition.test(next)) {
                if (i < numPriorityElements) {
                    numPriorityElements--;
                }
                iterator.remove();
                return next;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Polls the first priority element or non-priority element if the former does not exist.
     *
     * @return the first element or null.
     */
    @Nullable
    public InputChannel poll() {
        final InputChannel polled = deque.poll();
        if (polled != null && numPriorityElements > 0) {
            numPriorityElements--;
        }
        return polled == null ? convert(nonPriority.poll()) : polled;
    }

    /**
     * Returns the first priority element or non-priority element if the former does not exist.
     *
     * @return the first element or null.
     */
    @Nullable
    public InputChannel peek() {
        InputChannel peek = deque.peek();
        return peek == null ? convert(nonPriority.peek()) : peek;
    }

    private static InputChannel convert(NodeInputChannel nodeInputChannel) {
        return nodeInputChannel == null ? null : nodeInputChannel.delegate;
    }

    /** Returns the current number of priority elements ([0; {@link #size()}]). */
    public int getNumPriorityElements() {
        return numPriorityElements;
    }

    /**
     * Returns whether the given element is a known priority element. InputChannelest is performed
     * by identity.
     */
    public boolean containsPriorityElement(InputChannel element) {
        if (numPriorityElements == 0) {
            return false;
        }
        final Iterator<InputChannel> iterator = deque.iterator();
        for (int i = 0; i < numPriorityElements && iterator.hasNext(); i++) {
            if (iterator.next() == element) {
                return true;
            }
        }
        return false;
    }

    /** Returns the number of priority and non-priority elements. */
    public int size() {
        return deque.size() + nonPriority.size();
    }

    /** Returns the number of non-priority elements. */
    public int getNumUnprioritizedElements() {
        return size() - getNumPriorityElements();
    }

    /** @return read-only iterator */
    public Iterator<InputChannel> iterator() {
        return new IteratorJoin(
                Collections.unmodifiableCollection(deque).iterator(),
                Collections.unmodifiableCollection(nonPriority).iterator());
    }

    /** */
    public static class IteratorJoin implements Iterator<InputChannel> {
        private final Iterator<InputChannel> first;
        private final Iterator<NodeInputChannel> next;

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
                return first.next();
            }
            return convert(next.next());
        }
    }

    /** Removes all priority and non-priority elements. */
    public void clear() {
        deque.clear();
        nonPriority.clear();
        numPriorityElements = 0;
    }

    /** Returns true if there are no elements. */
    public boolean isEmpty() {
        return deque.isEmpty() && nonPriority.isEmpty();
    }

    /**
     * Returns whether the given element is contained in this list. InputChannelest is performed by
     * identity.
     */
    public boolean contains(InputChannel element) {
        if (isEmpty()) {
            return false;
        }
        for (InputChannel inputChannel : this) {
            if (inputChannel == element) {
                return true;
            }
        }
        return false;
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

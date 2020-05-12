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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.PARTIAL_RECORD;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;

/**
 * @param <T> The type of the record to be deserialized.
 */
public class SpillingAdaptiveSpanningRecordDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T> {

	static final int LENGTH_BYTES = Integer.BYTES;

	private final NonSpanningWrapper nonSpanningWrapper;

	private final SpanningWrapper spanningWrapper;

	private Buffer currentBuffer;

	public SpillingAdaptiveSpanningRecordDeserializer(String[] tmpDirectories) {
		this.nonSpanningWrapper = new NonSpanningWrapper();
		this.spanningWrapper = new SpanningWrapper(tmpDirectories);
	}

	@Override
	public void setNextBuffer(Buffer buffer) throws IOException {
		currentBuffer = buffer;

		int offset = buffer.getMemorySegmentOffset();
		MemorySegment segment = buffer.getMemorySegment();
		int numBytes = buffer.getSize();

		// check if some spanning record deserialization is pending
		if (spanningWrapper.getNumGatheredBytes() > 0) {
			spanningWrapper.addNextChunkFromMemorySegment(segment, offset, numBytes);
		} else {
			nonSpanningWrapper.initializeFromMemorySegment(segment, offset, numBytes + offset);
		}
	}

	@Override
	public Buffer getCurrentBuffer () {
		Buffer tmp = currentBuffer;
		currentBuffer = null;
		return tmp;
	}

	@Override
	public Optional<Buffer> getUnconsumedBuffer() throws IOException {
		final Optional<MemorySegment> unconsumedSegment;
		if (nonSpanningWrapper.hasRemaining()) {
			unconsumedSegment = nonSpanningWrapper.getUnconsumedSegment();
		} else {
			unconsumedSegment = spanningWrapper.getUnconsumedSegment();
		}
		return unconsumedSegment.map(segment -> new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, DATA_BUFFER, segment.size()));
	}

	@Override
	public DeserializationResult getNextRecord(T target) throws IOException {
		// always check the non-spanning wrapper first.
		// this should be the majority of the cases for small records
		// for large records, this portion of the work is very small in comparison anyways

		if (nonSpanningWrapper.hasCompleteLength()) {
			return readNonSpanningRecord(target);

		} else if (nonSpanningWrapper.hasRemaining()) {
			nonSpanningWrapper.transferTo(spanningWrapper.lengthBuffer);
			return PARTIAL_RECORD;

		} else if (spanningWrapper.hasFullRecord()) {
			target.read(spanningWrapper.getInputView());
			spanningWrapper.transferLeftOverTo(nonSpanningWrapper);
			return nonSpanningWrapper.hasRemaining() ? INTERMEDIATE_RECORD_FROM_BUFFER : LAST_RECORD_FROM_BUFFER;

		} else {
			return PARTIAL_RECORD;
		}
	}

	private DeserializationResult readNonSpanningRecord(T target) throws IOException {
		NextRecordResponse response = nonSpanningWrapper.getNextRecord(target);
		if (response.result == PARTIAL_RECORD) {
			spanningWrapper.transferFrom(nonSpanningWrapper, response.bytesLeft);
		}
		return response.result;
	}

	@Override
	public void clear() {
		this.nonSpanningWrapper.clear();
		this.spanningWrapper.clear();
	}

	@Override
	public boolean hasUnfinishedData() {
		return this.nonSpanningWrapper.hasRemaining() || this.spanningWrapper.getNumGatheredBytes() > 0;
	}

	@NotThreadSafe
	static class NextRecordResponse {
		DeserializationResult result;
		int bytesLeft;

		NextRecordResponse(DeserializationResult result, int bytesLeft) {
			this.result = result;
			this.bytesLeft = bytesLeft;
		}

		public NextRecordResponse updated(DeserializationResult result, int bytesLeft) {
			this.result = result;
			this.bytesLeft = bytesLeft;
			return this;
		}
	}
}

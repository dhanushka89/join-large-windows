/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * 
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.query;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.Event;

/**
 * CBFBasedWindow class joins two events as the SimpleWindow. Before going
 * through the Window it checks with the Bloom Filter whether the join attribute
 * exists and only if it is true, it proceeds with Window search.
 * 
 */
public class CBFBasedWindow extends SimpleWindow {

	private static final Logger LOGGER = Logger.getLogger(CustomWindowTest.class);

	private int joinAttributeId;
	private CountingBloomFilter filter;
	private Queue<Event> eventQueue;
	private double falsePositiveRate;

	/**
	 * Counting bloom filter will be created with the windowSize and with
	 * specifying value of the filtering attribute for the window and for the
	 * bloom filter
	 * 
	 * @param windowSize
	 *            No of events in the window
	 * @param windowFilterAttributeId
	 *            Filtering attribute location of the object array
	 * @param filterValue
	 *            Filtering condition value
	 * @param joinAttributeId
	 *            Joining attribute location of the object array
	 */
	public CBFBasedWindow(int windowSize, int windowFilterAttributeId, double filterValue,
	                      int joinAttributeId) {
		super(windowSize, windowFilterAttributeId, filterValue);
		this.joinAttributeId = joinAttributeId;

		eventQueue = new LinkedList<Event>();

		this.falsePositiveRate = 0.01;
		int optimalBloomFilterSize = optimalBloomFilterSize(windowSize, falsePositiveRate);

		filter =
		         new CountingBloomFilter(optimalBloomFilterSize,
		                                 optimalNoOfHash(optimalBloomFilterSize, windowSize),
		                                 Hash.MURMUR_HASH);
	}

	@Override
	public void addEvent(Event event) {
		if ((double) event.getData(windowFilterAttributeId) == filterValue) {
			if (eventQueue.size() == windowSize) {
				filter.delete(new Key(eventQueue.remove().getData(joinAttributeId).toString()
				                                .getBytes()));
			}
			eventQueue.add(event);
			Key key = new Key(event.getData(joinAttributeId).toString().getBytes());
			filter.add(key);
		}
	}

	@Override
	public void joinEvent(Event stream2Event, int stream2EventJoinAttributeId,
	                      int joinWindowAttributeId) {
		Key key = new Key(stream2Event.getData(stream2EventJoinAttributeId).toString().getBytes());

		if (filter.membershipTest(key)) {

			Iterator<Event> iterator = eventQueue.iterator();
			while (iterator.hasNext()) {
				Event evt = (Event) iterator.next();
				if ((double) evt.getData(joinWindowAttributeId) == (double) (stream2Event.getData(stream2EventJoinAttributeId))) {
					Event joinEvent =
					                  concatenateEvents(evt, stream2Event,
					                                    stream2EventJoinAttributeId);
				}
			}
		}
	}

	/**
	 * @param stream2Event
	 * @param stream2EventJoinAttributeId
	 */
	public void joinEvent(Event stream2Event, int stream2EventJoinAttributeId) {
		joinEvent(stream2Event, stream2EventJoinAttributeId, joinAttributeId);
	}

	/**
	 * Calculates the optimal size of the bloom filter in bits, given
	 * noOfElements and falsePositiveRate
	 * 
	 * @param noOfElements
	 *            Expected number of elements inserted in the bloom filter
	 * @param falsePositiveRate
	 *            Tolerable false positive rate
	 * @return Optimal Size of the bloom filter in bits
	 */
	private static int optimalBloomFilterSize(long noOfElements, double falsePositiveRate) {
		return (int) Math.ceil(-1 * (noOfElements * Math.log(falsePositiveRate)) /
		                       Math.pow(Math.log(2), 2));
	}

	/**
	 * Calculate the optimal number of hash functions for the bloom filter using
	 * bloom filter size and no of elements inserted
	 * 
	 * @param bloomFilterSize
	 *            Bloom Filter vector size
	 * @param noOfElements
	 *            Expected number of elements inserted to the filter
	 * @return Optimal number of hash functions
	 */

	private static int optimalNoOfHash(int bloomFilterSize, long noOfElements) {
		return (int) Math.round(bloomFilterSize / noOfElements * Math.log(2));
	}
}

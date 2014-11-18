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

package org.wso2.siddhi.bloomfilter;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.wso2.siddhi.core.event.Event;

/**
 * CBFBasedWindow class joins two events as the SimpleWindow. Before going
 * through the Window it checks with the Bloom Filter whether the join attribute
 * exists and only if it is true, it proceeds with Window search.
 * 
 */
public class CBFBasedWindow extends SimpleWindow {

	protected int joinAttributeId;
	protected double falsePositiveRate;

	private CountingBloomFilter filter;

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
	public CBFBasedWindow(int windowSize, double falsePositiveRate, int windowFilterAttributeId,
	                      double filterValue, int joinAttributeId) {
		super(windowSize, windowFilterAttributeId, filterValue);
		this.joinAttributeId = joinAttributeId;

		eventQueue = new LinkedList<Event>();

		this.falsePositiveRate = falsePositiveRate;

		int optimalBloomFilterSize =
		                             BloomFilterUtil.optimalBloomFilterSize(windowSize,
		                                                                    falsePositiveRate);
		
		filter =
		         new CountingBloomFilter(
		                                 optimalBloomFilterSize,
		                                 (int) BloomFilterUtil.optimalNoOfHash(optimalBloomFilterSize,
		                                                                       windowSize),
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
		boolean isMatch = false;

		if (filter.membershipTest(key)) {
			noOfMatches++;
			Iterator<Event> iterator = eventQueue.iterator();
			while (iterator.hasNext()) {
				Event evt = (Event) iterator.next();
				if ((double) evt.getData(joinWindowAttributeId) == (double) (stream2Event.getData(stream2EventJoinAttributeId))) {
					noOfMatchedEvents++;
					isMatch = true;
				}
			}
		}

		if (isMatch) {
			actualMatches++;
		}
	}

	/**
	 * @param stream2Event
	 * @param stream2EventJoinAttributeId
	 */
	public void joinEvent(Event stream2Event, int stream2EventJoinAttributeId) {
		joinEvent(stream2Event, stream2EventJoinAttributeId, joinAttributeId);
	}

}
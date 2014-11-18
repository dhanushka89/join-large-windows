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

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.wso2.siddhi.core.event.Event;

public class OBFBasedWindow extends CBFBasedWindow {

	private BloomFilter[] filters;
	private int noOfFilters;
	private int optimalBloomFilterSize;
	private int[] filterSize;
	private int nonOverLapEvents;
	private int noOfHash;
	private int addingFilters;

	public OBFBasedWindow(int windowSize, double falsePositiveRate, int overLapPrecentage,
	                      int windowFilterAttributeId, double filterValue, int joinAttributeId) {
		super(windowSize, falsePositiveRate, windowFilterAttributeId, filterValue, joinAttributeId);

		noOfFilters = (int) Math.ceil((100.0 / (100 - overLapPrecentage))) + 1;

		nonOverLapEvents = windowSize - (int) (overLapPrecentage / 100.0 * windowSize);

		optimalBloomFilterSize =
		                         BloomFilterUtil.optimalBloomFilterSize(windowSize,
		                                                                falsePositiveRate);
		noOfHash = (int) BloomFilterUtil.optimalNoOfHash(optimalBloomFilterSize, windowSize);

		filters = new BloomFilter[noOfFilters];
		filterSize = new int[noOfFilters];
		addingFilters = 0;

		for (int i = 0; i < noOfFilters; i++) {
			filters[i] = new BloomFilter(optimalBloomFilterSize, noOfHash, Hash.MURMUR_HASH);
		}

	}

	@Override
	public void addEvent(Event event) {
		if ((double) event.getData(windowFilterAttributeId) == filterValue) {

			if (eventQueue.size() == windowSize) {
				eventQueue.remove();
			}
			eventQueue.add(event);

			if (filterSize[1] % nonOverLapEvents == 0) {
				addingFilters++;
			}

			if (filterSize[1] == windowSize) {
				for (int i = 0; i < (noOfFilters - 1); i++) {
					filters[i] = filters[i + 1];
					filterSize[i] = filterSize[i + 1];
				}
				filters[noOfFilters - 1] =
				                           new BloomFilter(optimalBloomFilterSize, noOfHash,
				                                           Hash.MURMUR_HASH);
				filterSize[noOfFilters - 1] = 0;
				addingFilters--;
			}

			Key eventKey = new Key(event.getData(joinAttributeId).toString().getBytes());

			for (int i = 1; i <= addingFilters; i++) {
				filters[i].add(eventKey);
				filterSize[i]++;
			}

		}
	}

	@Override
	public void joinEvent(Event stream2Event, int stream2EventJoinAttributeId,
	                      int joinWindowAttributeId) {
		Key key = new Key(stream2Event.getData(stream2EventJoinAttributeId).toString().getBytes());
		boolean isMatch = false;

		if (filters[0].membershipTest(key) || filters[1].membershipTest(key)) {

			noOfMatches++;
			Iterator<Event> iterator = eventQueue.iterator();
			while (iterator.hasNext()) {
				Event evt = (Event) iterator.next();
				if ((double) evt.getData(joinWindowAttributeId) == (double) (stream2Event.getData(stream2EventJoinAttributeId))) {
					Event joinEvent =
					                  concatenateEvents(evt, stream2Event,
					                                    stream2EventJoinAttributeId);
					noOfMatchedEvents++;
					isMatch = true;
				}
			}
		}
		if (isMatch) {
			actualMatches++;
		}
	}

}
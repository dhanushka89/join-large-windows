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

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.Event;

/**
 * SimpleWindow class joins an incoming event with an existing event in the
 * Siddhi Window by comparing the join attribute.
 * 
 */
public class SimpleWindow {

	private static final Logger LOGGER = Logger.getLogger(SimpleWindow.class);

	protected int windowSize;
	protected int windowFilterAttributeId;
	protected double filterValue;

	Queue<Event> eventQueue;

	/**
	 * @param windowSize
	 *            No of events in the window
	 * @param windowFilterAttributeId
	 *            Filtering attribute location of the object array
	 * @param filterValue
	 *            Filtering condition value
	 */
	public SimpleWindow(int windowSize, int windowFilterAttributeId, double filterValue) {
		this.windowSize = windowSize;
		this.windowFilterAttributeId = windowFilterAttributeId;
		this.filterValue = filterValue;

		eventQueue = new LinkedList<Event>();
	}

	/**
	 * Add events to the Window by checking the windowFiterAttributeId
	 * 
	 * @param event
	 *            Siddhi Event
	 */
	public void addEvent(Event event) {
		if ((double) event.getData(windowFilterAttributeId) == filterValue) {
			if (eventQueue.size() == windowSize) {
				eventQueue.remove();
			}
			eventQueue.add(event);
		}

	}

	/**
	 * Join two events by comparing their joinAttributeId
	 * 
	 * @param stream2Event
	 *            Siddhi Event
	 * @param stream2EventJoinAttributeId
	 *            Join Attribute Id
	 * @param joinWindowAttributeId
	 *            Window attribute to join with the attId
	 */
	public void joinEvent(Event stream2Event, int stream2EventJoinAttributeId,
	                      int joinWindowAttributeId) {

		Iterator<Event> iterator = eventQueue.iterator();
		while (iterator.hasNext()) {
			Event evt = (Event) iterator.next();
			if ((double) evt.getData(joinWindowAttributeId) == (double) (stream2Event.getData(stream2EventJoinAttributeId))) {
				Event joinEvent = concatenateEvents(evt, stream2Event, stream2EventJoinAttributeId);
			}
		}
	}

	/**
	 * Concatenate two joined events and return a single event
	 * 
	 * @param event1
	 *            Siddhi Event
	 * @param event2
	 *            Siddhi Event
	 * @param event2JoinAttId
	 *            Joined attribute Id of Event 2
	 * @return Concatenated Event
	 */
	protected Event concatenateEvents(Event event1, Event event2, int event2JoinAttId) {

		Object[] event1Data = event1.getData();
		Object[] event2Data = event2.getData();

		int event1DataLength = event1Data.length;
		int event2DataLength = event2Data.length;

		// Reduce the size of the created array by one because of removing the
		// duplicated joining attribute
		Object[] joinEventData = new Object[event1DataLength + event2DataLength - 1];

		for (int i = 0; i < event1DataLength; i++) {
			joinEventData[i] = event1Data[i];
		}

		for (int i = 0; i < event2DataLength; i++) {
			if (i != event2JoinAttId) {
				joinEventData[event1DataLength] = event2Data[i];
				event1DataLength++;
			}
		}

		return new SiddhiEvent("3", event1.getTimeStamp(), joinEventData);

	}

}

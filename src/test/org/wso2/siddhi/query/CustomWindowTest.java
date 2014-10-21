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

import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * 
 *
 */
public class CustomWindowTest {

	private static final int SIMPLE_WINDOW = 1;
	private static final int CBFB_WINDOW = 2;

	private static final Logger LOGGER = Logger.getLogger(CustomWindowTest.class);

	public static void main(String[] args) {

		runTest(CBFB_WINDOW, 10000, 1000000, 1);
	}

	/**
	 * A specific Window will be created with a size.
	 * Two event streams will be generated with a defined matching rate which,
	 * one goes through the window and the other goes through the joining of the
	 * events in the window
	 * 
	 * @param windowType
	 *            Two types of windows, Simple and CountingBloomFilterBased
	 * @param windowSize
	 *            No of events contained in the Window
	 * @param noOfEvents
	 *            No of events generating through out the process
	 * @param matchRate
	 *            Matching rate between the two event streams
	 */
	private static void runTest(int windowType, int windowSize, int noOfEvents, int matchRate) {

		int randomNumRange = 100000;

		SimpleWindow window = null;
		String type = null;
		SiddhiEvent event1, event2;

		if (windowType == SIMPLE_WINDOW) {
			window = new SimpleWindow(windowSize, 1, 0.0);
			type = "SIMPLE WINDOW";
		} else if (windowType == CBFB_WINDOW) {
			window = new CBFBasedWindow(windowSize, 1, 0.0, 2);
			type = "CBF BASE WINDOW";
		}

		String[] brand = new String[10];
		brand[0] = "IBM";
		brand[1] = "Samsung";
		brand[2] = "Azus";
		brand[3] = "Toshiba";
		brand[4] = "Lenovo";
		brand[5] = "Apple";
		brand[6] = "Dell";
		brand[7] = "HP";
		brand[8] = "Singer";
		brand[9] = "LG";

		Random random = new Random();

		Date startDate = new Date();

		int position1 = 0, position2 = 0;
		double value1 = 0, value2 = 0;

		for (int i = 0; i < noOfEvents / 100 * matchRate; i++) {

			position1 = random.nextInt(100 / matchRate);
			position2 = random.nextInt(100 / matchRate);

			for (int j = 0; j < (100 / matchRate); j++) {

				if (j == position1) {
					value1 = getRandomDouble(random, randomNumRange, 2);
					Object[] ob1 = { brand[random.nextInt(10)], 0.0, value1, random.nextInt(20) };
					event1 = new SiddhiEvent("1", startDate.getTime(), ob1);

				} else {
					Object[] ob1 =
					               { brand[random.nextInt(10)], 0.0,
					                (double) getRandomDouble(random, randomNumRange, 2),
					                random.nextInt(20) };
					event1 = new SiddhiEvent("1", startDate.getTime(), ob1);
				}

				window.addEvent(event1);

			}

			for (int j = 0; j < (100 / matchRate); j++) {
				if (j == position2) {
					value2 = value1;
					Object[] ob2 = { brand[random.nextInt(10)], value2, random.nextInt(20) };
					event2 = new SiddhiEvent("2", startDate.getTime(), ob2);
				} else {
					Object[] ob2 =
					               {
					                brand[random.nextInt(10)],
					                (double) (getRandomDouble(random, randomNumRange, 2) +
					                          randomNumRange + 1), random.nextInt(20) };
					event2 = new SiddhiEvent("2", startDate.getTime(), ob2);
				}
				window.joinEvent(event2, 1, 2);

			}

		}

		Date endDate = new Date();

		long diff = endDate.getTime() - startDate.getTime();

		LOGGER.debug("----------------------------------");
		LOGGER.debug("Window type - " + type);
		LOGGER.debug("No Of Events - " + noOfEvents);
		LOGGER.debug("Windows Size - " + windowSize);
		LOGGER.debug("Match Rate - " + matchRate + "%");

		LOGGER.debug("Started - " + startDate.getTime());
		LOGGER.debug("Ended - " + endDate.getTime());

		long diffSeconds = diff / 1000 % 60;
		long diffMinutes = diff / (60 * 1000) % 60;
		long diffHours = diff / (60 * 60 * 1000) % 24;

		LOGGER.debug("Time duration - " + diffHours + ":" + diffMinutes + ":" + diffSeconds);

		LOGGER.debug("----------------------------------");

	}

	/**
	 * A random value will be generated according to a given parameters
	 * 
	 * @param random
	 *            util.Random object variable
	 * @param randomNumRange
	 *            Return value range will be varied according to this
	 * @param numOfDecimalPoints
	 *            number of decimal point in the generated random value
	 * @return random double value
	 */
	private static double getRandomDouble(Random random, int randomNumRange, int numOfDecimalPoints) {
		double p = Math.pow(10, numOfDecimalPoints);
		return Math.round(random.nextDouble() * randomNumRange * p) / p;
	}

}

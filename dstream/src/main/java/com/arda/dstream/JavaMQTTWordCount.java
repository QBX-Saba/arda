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

package com.arda.dstream;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * com.arda.dstream.JavaMQTTWordCount Consumes
 * messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: JavaKafkaWordCount
 * <zkQuorum> <group> <topics> <numThreads> <zkQuorum> is a list of one or more
 * zookeeper servers that make quorum <group> is the name of kafka consumer
 * group <topics> is a list of one or more kafka topics to consume from
 * <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example: `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \ zoo03
 * my-consumer-group topic1,topic2 1`
 */

public final class JavaMQTTWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	private JavaMQTTWordCount() {
	}

	public static void main(String[] args) {

		// StreamingExamples.setStreamingLogLevels();
		SparkConf sparkConf = new SparkConf().setAppName("JavaMQTTWordCount").setMaster("local[4]").
				set("spark.driver.allowMultipleContexts", "true");
		//sparkConf.setMaster("spark://asadkazmi-Inspiron-5548:7077");
//		sparkConf.set("spark.driver.allowMultipleContexts", "true");
			
		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
//		JavaReceiverInputDStream<String> messages = jssc.socketTextStream("ec2-54-93-53-98.eu-central-1.compute.amazonaws.com", 61616);
		
		
		String brokerUrl = "tcp://ec2-54-93-53-98.eu-central-1.compute.amazonaws.com:1883";
		String topic = "connectavo.devices.4000002707881.191238";
		
		JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, brokerUrl, topic,StorageLevels.MEMORY_AND_DISK_SER);
	
/*		JavaDStream<String> lines = messages.map(new Function<String, String>() {

			public String call(String arg0) throws Exception {
				return arg0;
			}
		});

		JavaDStream<String> words = messages.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {
				System.out.println(x);
				return Lists.newArrayList(SPACE.split(x));
			}
		});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			public Integer call(Integer i1, Integer i2) {

				return i1 + i2;
			}
		});*/

		messages.print();
		//messages.dstream().log();
		jssc.start();
		jssc.awaitTermination();
	}
}

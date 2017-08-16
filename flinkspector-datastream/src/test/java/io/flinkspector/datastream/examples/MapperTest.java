/*
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.flinkspector.datastream.examples;

import io.flinkspector.core.collection.ExpectedRecords;
import io.flinkspector.datastream.DataStreamTestBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;


/**
 * This example shows how to define input without time characteristics.
 * With the usage of {@link ExpectedRecords}.
 */
public class MapperTest extends DataStreamTestBase {

    /**
     * DataStream transformation to test.
     * Swaps the fields of a {@link Tuple2}
     *
     * @param stream input {@link DataStream}
     * @return {@link DataStream}
     */
    public static DataStream<Tuple2<String, Integer>> swap(DataStream<Tuple2<Integer, String>> stream) {
        return stream.map(new MapFunction<Tuple2<Integer, String>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<Integer, String> input) throws Exception {
                return input.swap();
            }
        });
    }

    /**
     * JUnit Test
     */
    @org.junit.Test
    public void testMap() {

		/*
         * Define the input DataStream:
		 * Get a SourceBuilder with .createTestStreamWith(record).
		 * Add data records to it and retrieve a DataStreamSource
		 * by calling .complete().
		 */
        DataStream<String> stream =
                createTimedTestStreamWith("hllo")
                        .close();

		/*
		 * Define the output you expect from the the transformation under test.
		 * Add the tuples you want to see with .expect(record).
		 */
        ExpectedRecords<String> expectedRecords = new ExpectedRecords<String>()
                .expect("hello");
        // refine your expectations by adding requirements
        expectedRecords.refine().only();

		/*
		 * Use assertStream to map DataStream to an OutputMatcher.
		 * ExpectedRecords extends OutputMatcher and thus can be used in this way.
		 * This means you're also able to combine ExpectedRecords with any
		 * OutputMatcher. E.g:
		 * assertStream(swap(stream), and(expectedRecords,outputWithSize(3))
		 * would additionally assert that the number of produced records is exactly 3.
		 */
        assertStream(stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return false;
            }
        }), expectedRecords);

    }

}

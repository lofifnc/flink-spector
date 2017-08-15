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

package io.flinkspector.datastream;

import io.flinkspector.core.Order;
import io.flinkspector.core.collection.ExpectedRecords;
import io.flinkspector.core.input.Input;
import io.flinkspector.core.input.InputBuilder;
import io.flinkspector.core.quantify.HamcrestVerifier;
import io.flinkspector.core.quantify.OutputMatcherFactory;
import io.flinkspector.core.runtime.OutputVerifier;
import io.flinkspector.core.trigger.VerifyFinishedTrigger;
import io.flinkspector.datastream.functions.TestSink;
import io.flinkspector.datastream.input.EventTimeInput;
import io.flinkspector.datastream.input.EventTimeInputBuilder;
import io.flinkspector.datastream.input.EventTimeSourceBuilder;
import io.flinkspector.datastream.input.SourceBuilder;
import io.flinkspector.datastream.input.time.After;
import io.flinkspector.datastream.input.time.Before;
import io.flinkspector.datastream.input.time.InWindow;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Offers a base for testing Flink streaming applications
 * To use, extend your JUnitTest with this class.
 */
public class DataStreamTestBase {

    public static final Order strict = Order.STRICT;
    public static final Order notStrict = Order.NONSTRICT;
    public static final TimeUnit seconds = TimeUnit.SECONDS;
    public static final TimeUnit minutes = TimeUnit.MINUTES;
    public static final TimeUnit hours = TimeUnit.HOURS;
    public static final String ignore = null;
    /**
     * Test Environment
     */
    DataStreamTestEnvironment testEnv;

    /**
     * Creates an {@link InWindow} object.
     *
     * @param time value.
     * @param unit of time.
     * @return {@link InWindow}
     */
    public static InWindow intoWindow(long time, TimeUnit unit) {
        return InWindow.to(time, unit);
    }

    /**
     * Creates an {@link After} object.
     *
     * @param span length of span.
     * @param unit of time.
     * @return {@link After}
     */
    public static After after(long span, TimeUnit unit) {
        return After.period(span, unit);
    }

    /**
     * Creates an {@link Before} object.
     *
     * @param span length of span.
     * @param unit of time.
     * @return {@link Before}
     */
    public static Before before(long span, TimeUnit unit) {
        return Before.period(span, unit);
    }

    public static <T> EventTimeInputBuilder<T> startWith(T record) {
        return EventTimeInputBuilder.startWith(record);
    }

    public static <T> InputBuilder<T> emit(T elem) {
        return InputBuilder.startWith(elem);
    }

    public static <T> ExpectedRecords<T> expectOutput(T record) {
        return ExpectedRecords.create(record);
    }

    public static int times(int n) {
        return n;
    }

    /**
     * Creates a new {@link DataStreamTestEnvironment}
     */
    @org.junit.Before
    public void initialize() throws Exception {
        testEnv = DataStreamTestEnvironment.createTestEnvironment(1);
        testEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    /**
     * Gets the parallelism with which operation are executed by default.
     * Operations can individually override this value to use a specific
     * parallelism.
     *
     * @return The parallelism used by operations, unless they override that
     * value.
     */
    public Integer getParrallelism() {
        return testEnv.getParallelism();
    }

    /**
     * Sets the parallelism for operations executed through this environment.
     * Setting a parallelism of x here will cause all operators (such as map,
     * batchReduce) to run with x parallel instances. This method overrides the
     * default parallelism for this environment. The
     * {@link LocalStreamEnvironment} uses by default a value equal to the
     * number of hardware contexts (CPU cores / threads). When executing the
     * program via the command line client from a JAR file, the default degree
     * of parallelism is the one configured for that setup.
     *
     * @param parallelism The parallelism
     */
    public void setParrallelism(Integer parallelism) {
        testEnv.setParallelism(parallelism);
    }

    /**
     * Creates a DataStreamSource from an EventTimeInput object.
     * The DataStreamSource will emit the records with the specified EventTime.
     *
     * @param input to emit.
     * @param <OUT> type of the emitted records.
     * @return a DataStreamSource generating the input.
     */
    public <OUT> DataStreamSource<OUT> createTestStream(EventTimeInput<OUT> input) {
        return testEnv.fromInput(input);
    }

    //================================================================================
    // Syntactic sugar stuff
    //================================================================================

    /**
     * Provides an {@link SourceBuilder} to startWith an {@link DataStreamSource}
     *
     * @param record the first record to emit.
     * @param <OUT>  type of the stream.
     * @return {@link SourceBuilder} to work with.
     */
    public <OUT> SourceBuilder<OUT> createTestStreamWith(OUT record) {
        return SourceBuilder.createBuilder(record, testEnv);
    }

    /**
     * Provides an {@link EventTimeSourceBuilder} to startWith an {@link DataStreamSource}
     *
     * @param record the first record to emit.
     * @param <OUT>  type of the stream.
     * @return {@link EventTimeSourceBuilder} to work with.
     */
    public <OUT> EventTimeSourceBuilder<OUT> createTimedTestStreamWith(OUT record) {
        return EventTimeSourceBuilder.createBuilder(record, testEnv);
    }

    /**
     * Creates a DataStreamSource from an EventTimeInput object.
     * The DataStreamSource will emit the records with the specified EventTime.
     *
     * @param input to emit.
     * @param <OUT> type of the emitted records.
     * @return a DataStreamSource generating the input.
     */
    public <OUT> DataStreamSource<OUT> createTestStream(Collection<OUT> input) {
        return testEnv.fromInput(input);
    }

    /**
     * Creates a DataStreamSource from an Input object.
     *
     * @param input to emit.
     * @param <OUT> type of the emitted records.
     * @return a DataStream generating the input.
     */
    public <OUT> DataStreamSource<OUT> createTestStream(Input<OUT> input) {
        return testEnv.fromInput(input);
    }

    /**
     * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
     *
     * @param matcher of type Iterable
     * @param <IN>
     * @return the created sink.
     */
    public <IN> TestSink<IN> createTestSink(org.hamcrest.Matcher<Iterable<IN>> matcher) {
        OutputVerifier<IN> verifier = new HamcrestVerifier<IN>(matcher);
        return createTestSink(verifier);
    }

    /**
     * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
     *
     * @param verifier of generic type IN
     * @param <IN>
     * @return the created sink.
     */
    public <IN> TestSink<IN> createTestSink(OutputVerifier<IN> verifier,
                                            VerifyFinishedTrigger trigger) {
        return testEnv.createTestSink(verifier, trigger);
    }

    /**
     * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
     *
     * @param verifier of generic type IN
     * @param <IN>
     * @return the created sink.
     */
    public <IN> TestSink<IN> createTestSink(OutputVerifier<IN> verifier) {
        return testEnv.createTestSink(verifier);
    }

    /**
     * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
     *
     * @param matcher of type Iterable
     * @param <IN>
     * @return the created sink.
     */
    public <IN> TestSink<IN> createTestSink(org.hamcrest.Matcher<Iterable<IN>> matcher,
                                            VerifyFinishedTrigger trigger) {
        OutputVerifier<IN> verifier = new HamcrestVerifier<>(matcher);
        return createTestSink(verifier, trigger);
    }

    /**
     * Inspect a {@link DataStream} using a {@link OutputMatcherFactory}.
     *
     * @param stream  {@link DataStream} to test.
     * @param matcher {@link OutputMatcherFactory} to use.
     * @param <T>     type of the stream.
     */
    public <T> void assertStream(DataStream<T> stream, Matcher<Iterable<T>> matcher) {
        stream.addSink(createTestSink(matcher));
    }

    /**
     * Inspect a {@link DataStream} using a {@link OutputMatcherFactory}.
     *
     * @param stream  {@link DataStream} to test.
     * @param matcher {@link OutputMatcherFactory} to use.
     * @param trigger {@link VerifyFinishedTrigger}
     *                to finish the assertion early.
     * @param <T>     type of the stream.
     */
    public <T> void assertStream(DataStream<T> stream,
                                 Matcher<Iterable<T>> matcher,
                                 VerifyFinishedTrigger trigger) {
        stream.addSink(createTestSink(matcher, trigger));
    }

    /**
     * Sets the parallelism for operations executed through this environment.
     * Setting a parallelism of x here will cause all operators (such as map,
     * batchReduce) to run with x parallel instances. This method overrides the
     * default parallelism for this environment. The
     * {@link LocalStreamEnvironment} uses by default a value equal to the
     * number of hardware contexts (CPU cores / threads). When executing the
     * program via the command line client from a JAR file, the default degree
     * of parallelism is the one configured for that setup.
     *
     * @param parallelism The parallelism
     */
    public void setParallelism(int parallelism) {
        testEnv.setParallelism(parallelism);
    }

    /**
     * Executes the test and verifies the output received by the sinks.
     */
    @org.junit.After
    public void executeTest() throws Throwable {
        try {
            testEnv.executeTest();
        } catch (AssertionError assertionError) {
            System.out.println(assertionError.getMessage());

//			TODO: find a better way to flag internal errors
//			if (testEnv.hasBeenStopped()) {
//			//	the execution has been forcefully stopped inform the user!
//				throw new AssertionError("Test terminated due timeout!" +
//						assertionError.getMessage());
//			}
            throw assertionError;
        }
    }

    /**
     * Setter for the timeout interval
     * after the test execution gets stopped.
     *
     * @param interval in milliseconds.
     */
    public void setTimeout(long interval) {
        testEnv.setTimeoutInterval(interval);
    }

}

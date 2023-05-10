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

package nl.tudelft;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
// import org.apache.flink.formats.json;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSource.FileSourceBuilder;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStream.Collector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	private static FileSource<String> read() {
		return FileSource
			.forRecordStreamFormat(new TextLineInputFormat(), new Path("../traces-1"))
			.setFileEnumerator(() -> new NonSplittingRecursiveEnumerator(path -> {
				return new File(path.getPath()).isDirectory() || (path.getName().equals("execution.txt"));
			}))
			.build();
	}

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000);

		FileSink<String> sink = FileSink.forRowFormat(new Path("./data"), new SimpleStringEncoder<String>("UTF-8")).build();
		// env.fromElements(0, 1, 2, 3, 4).sinkTo(sink);
		DataStream<String> stream = env.fromSource(read(), WatermarkStrategy.noWatermarks(), "file-source");
		// stream.map(text -> "hello: " + text).sinkTo(sink);

		// Pattern<String, ?> pattern = Pattern.<String>begin("start")
		// 	.where(SimpleCondition.of(event -> event.contains("Validation")))
		// 	// .next("middle")
		// 	// // .subtype(SubEvent.class)
		// 	// .where(SimpleCondition.of(subEvent -> subEvent.contains("ProposeSet")))
		// 	.next("end")
		// 	.where(SimpleCondition.of(event -> event.contains("Ping")))
		// 	;

		// PatternStream<String> patternStream = CEP.pattern(stream.filter(line -> !line.startsWith("--")), pattern).inProcessingTime();

		// final class Alert {
		// 	String text;
		// 	public Alert(String text) {
		// 		this.text = text;
		// 	}
		// }

		// DataStream<Alert> result = patternStream.process(
		// 	new PatternProcessFunction<String, Alert>() {

		// 		@Override
		// 		public void processMatch(Map<String, List<String>> match, Context ctx,
		// 				org.apache.flink.util.Collector<Alert> out) throws Exception {
		// 			out.collect(new Alert(match.toString()));
		// 		}
		// 	});

		// // stream
		// // 	.filter(line -> !line.startsWith("--"))
		// // 	.sinkTo(sink);

		// result.map(alert -> alert.text).sinkTo(sink);

		final class Deduplicate extends RichFlatMapFunction<String, String> {
			ValueState<Boolean> seen;
		  
			@Override
			public void open(Configuration conf) {
			  ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
			  seen = getRuntimeContext().getState(desc);
			}

			@Override
			public void flatMap(String value, org.apache.flink.util.Collector<String> out) throws Exception {
				if (seen.value() == null) {
					out.collect(value);
					seen.update(true);
				  }
			}
		  }

		stream
			.filter(line -> !line.startsWith("--"))
			.map(line -> line.substring(line.indexOf("sent") + 5).split(":")[0])
			.keyBy(x -> x)
			.flatMap(new Deduplicate())
			.sinkTo(sink);

		// stream.sinkTo(sink);

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}

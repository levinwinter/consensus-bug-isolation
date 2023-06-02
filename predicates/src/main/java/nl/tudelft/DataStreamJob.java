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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.Encoder;
// import org.apache.flink.formats.json;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStream.Collector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>
 * For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink
 * Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for
 * 'mainClass').
 */
public class DataStreamJob {

	static class MyTimestampAssigner implements TimestampAssigner<Tuple2<String, String>> {

		@Override
		public long extractTimestamp(Tuple2<String, String> element, long recordTimestamp) {
			if (element.f1.startsWith("--")) {
				// System.out.println(element.f1.split(" ")[1]);
				return (Long.parseLong(element.f1.split(" ")[1]) + 946684800) * 1000;
			} else {
				// System.out.println(element.f1.split(" ")[0]);
				return (Long.parseLong(element.f1.split(" ")[0]) + 946684800) * 1000;
			}
		}

	}

	private static FileSource<String> read(Path directory) {
		return FileSource
				.forRecordStreamFormat(new TextLineInputFormat(), directory)
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
		env.getCheckpointConfig().setCheckpointStorage(
  new FileSystemCheckpointStorage("file:///tmp/flink-checkpoints"));

		final class TupleEncoder implements Encoder<Tuple2<String, String>> {
			@Override
			public void encode(Tuple2<String, String> stringStringTuple2, OutputStream outputStream)
					throws IOException {
				outputStream.write((stringStringTuple2.f1.toString() + System.lineSeparator()).getBytes());
			}
		}

		FileSink<Tuple2<String, String>> sink = FileSink
				.forRowFormat(new Path("../data"), new TupleEncoder())
				.withBucketAssigner(new BucketAssigner<Tuple2<String, String>, String>() {

					@Override
					public String getBucketId(Tuple2<String, String> stringStringTuple2, Context context) {
						return stringStringTuple2.f0;
					}

					@Override
					public SimpleVersionedSerializer<String> getSerializer() {
						return SimpleVersionedStringSerializer.INSTANCE;
					}

				})
				.build();

		String dir = "/traces/buggy-7-2-0-6-small-scope-0.2.4";
		// String dir = "/workspaces/consensus-bug-isolation/traces";
		List<String> paths = new ArrayList<>();
		for (String runPath : new File(dir).list()) {
			if (runPath.startsWith("."))
				continue;
			paths.add(new Path(dir, runPath).toString());
		}

		class MyFlatMap extends ProcessFunction<String, Tuple2<String, String>> {

			@Override
			public void processElement(String value, ProcessFunction<String, Tuple2<String, String>>.Context arg1,
					org.apache.flink.util.Collector<Tuple2<String, String>> out) throws Exception {
				try (Stream<String> stream = Files.lines(Paths.get(value, "execution.txt"))) {
					stream.forEach(line -> {
						if (line.contains("Validation ")) out.collect(Tuple2.of(new Path(value).getName(), Utils.sendToSelf(line)));
						out.collect(Tuple2.of(new Path(value).getName(), line));
					});
				}
			}
		}

		DataStream<Tuple2<String, String>> stream = env
				.fromCollection(paths)
				.process(new MyFlatMap())
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<Tuple2<String, String>>forMonotonousTimestamps()
						.withTimestampAssigner((x) -> new MyTimestampAssigner()))
				.keyBy(pair -> pair.f0)
				// .process(new KeyedProcessFunction<String, Tuple2<String, String>,
				// Tuple2<String, String>>() {

				// @Override
				// public void processElement(Tuple2<String, String> arg0,
				// KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String,
				// String>>.Context arg1,
				// org.apache.flink.util.Collector<Tuple2<String, String>> arg2) throws
				// Exception {
				// arg2.collect(Tuple2.of(arg1.getCurrentKey(), arg1.timestamp().toString()));
				// }

				// })
				// .keyBy(pair -> pair.f0)
				.process(new DropBootstrapping())
				.returns(new TypeHint<Tuple2<String, String>>() {
				})
				// .keyBy(pair -> pair.f0)
				.filter(line -> (line.f1.startsWith("--")
						|| (!line.f1.contains("Validation") && !line.f1.contains("ProposeSet")))
						&& Utils.to(line.f1) != 3)
				.map(line -> Tuple2.of(line.f0, line.f1.startsWith("--") ? line.f1.substring(3) : line.f1))
				.returns(new TypeHint<Tuple2<String, String>>() {
				})
		// .sinkTo(sink)
		;

		// env.fromElements(0, 1, 2, 3, 4).sinkTo(sink);
		// DataStream<String> stream = env.fromSource(read(),
		// WatermarkStrategy.noWatermarks(), "file-source");
		// stream.map(text -> "hello: " + text).sinkTo(sink);

		// Pattern<Tuple2<String, String>, ?> pattern = Pattern.<Tuple2<String, String>>begin("start")
		// 		.where(SimpleCondition.of(event -> event.f1.contains("Validation")))
		// 		// .next("middle")
		// 		// // .subtype(SubEvent.class)
		// 		// .where(SimpleCondition.of(subEvent -> subEvent.contains("ProposeSet")))
		// 		.next("end")
		// 		.where(SimpleCondition.of(event -> event.f1.contains("ProposeSet")));

		// PatternStream<Tuple2<String, String>> patternStream = CEP
		// 		.pattern(stream.filter(line -> !line.f1.startsWith("--")), pattern).inProcessingTime();

		// final class Alert {
		// 	String text;

		// 	public Alert(String text) {
		// 		this.text = text;
		// 	}
		// }

		// DataStream<Tuple2<String, String>> result = patternStream.process(
		// 		new PatternProcessFunction<Tuple2<String, String>, Tuple2<String, String>>() {

		// 			@Override
		// 			public void processMatch(Map<String, List<Tuple2<String, String>>> match, Context ctx,
		// 					org.apache.flink.util.Collector<Tuple2<String, String>> out) throws Exception {
		// 				ArrayList<String> runs = new ArrayList<>();
		// 				for (List<Tuple2<String, String>> x : match.values()) {
		// 					for (Tuple2<String, String> y : x) {
		// 						if (!runs.contains(y.f0))
		// 							runs.add(y.f0);
		// 					}
		// 				}
		// 				out.collect(Tuple2.of(runs.get(0), match.toString()));
		// 			}
		// 		});

		// result.sinkTo(sink);

		// Predicate p1 = new Predicate("Validation", "Validation", (pair) -> Utils.seq(pair.f0) + 1 == Utils.seq(pair.f1),
		// 		"Validation seq -- +1, >3 --> Validation seq", 4);
		IncompatibleLedgerPredicate p1 = new IncompatibleLedgerPredicate();
		p1.apply(stream
		.keyBy(new KeySelector<Tuple2<String,String>,Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Integer> getKey(Tuple2<String, String> pair) throws Exception {
				return Tuple2.of(pair.f0, Utils.to(pair.f1));
			}
			
		})
		)
		.sinkTo(sink);
		// stream.sinkTo(sink);

		// stream
		// .filter(line -> !line.startsWith("--"))
		// .sinkTo(sink);

		// result.map(alert -> alert.text).sinkTo(sink);

		// stream
		// .filter(line -> !line.startsWith("--"))
		// .map(line -> line.substring(line.indexOf("sent") + 5).split(":")[0])
		// .keyBy(x -> x)
		// .flatMap(new Deduplicate())
		// .sinkTo(sink);

		final class MyWatermarkGenerator implements WatermarkGenerator<String> {

			private final long maxOutOfOrderness = 0; // 3.5 seconds

			private long currentMaxTimestamp;

			@Override
			public void onEvent(String element, long eventTimestamp, WatermarkOutput output) {
				if (element.startsWith("--")) {
					eventTimestamp = Long.parseLong(element.split(" ")[1]) + 946684800;
				} else {
					eventTimestamp = Long.parseLong(element.split(" ")[0]) + 946684800;
				}
				currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
			}

			@Override
			public void onPeriodicEmit(WatermarkOutput output) {
				// emit the watermark as current highest timestamp minus the out-of-orderness
				// bound
				output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
			}

		}

		// File dir = new File("../traces");
		// File dir = new File("/traces/buggy-7-0-0-6-small-scope-0.2.4");
		// int count = 0;
		// for (String runPath : dir.list()) {
		// count++;
		// if (count > 1600) {
		// break;
		// }
		// WatermarkStrategy<String> watermarks = WatermarkStrategy
		// .<String>forMonotonousTimestamps()
		// // .forGenerator((x) -> new MyWatermarkGenerator())
		// .withTimestampAssigner((x) -> new MyTimestampAssigner());
		// env.fromSource(read(new Path(dir.getPath(), runPath)), watermarks, runPath)
		// .keyBy((x) -> 0)
		// .process(new DropBootstrapping<Integer, String>())
		// .returns(String.class)
		// .filter(line -> line.startsWith("--") || (!line.contains("Validation") &&
		// !line.contains("ProposeSet")))
		// .map(line -> line.startsWith("--") ? line.substring(3) : line)
		// // .map(line -> line.substring(line.indexOf("sent") + 5).split(":")[0])
		// // .map(line -> line.substring(line.indexOf("sent") + 5))
		// // .map(str -> 1)
		// // .process(new ProcessFunction<String, String>() {

		// // @Override
		// // public void processElement(String arg0, ProcessFunction<String,
		// // String>.Context arg1,
		// // org.apache.flink.util.Collector<String> arg2) throws Exception {
		// // arg2.collect("(time: " + new Date(arg1.timestamp() * 1000) + ") - " +
		// arg0);
		// // }

		// // })
		// // .process(new ProcessFunction<String, Long>() {

		// // @Override
		// // public void processElement(String arg0, ProcessFunction<String,
		// Long>.Context arg1,
		// // org.apache.flink.util.Collector<Long> arg2) throws Exception {
		// // arg2.collect(arg1.timestamp());
		// // }

		// // })
		// // .keyBy(x -> x)
		// // // .window(TumblingEventTimeWindows.of(Time.seconds(2)))
		// // // .sum(0)
		// // .flatMap(new Deduplicate<Long>())
		// // .map(integer -> integer.toString())
		// .sinkTo(sink);
		// // env.fromElements("hello: " + runPath, runPath).sinkTo(sink);
		// }

		// stream.sinkTo(sink);

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * .filter()
		 * .flatMap()
		 * .window()
		 * .process()
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

	static <T> DataStream<T> removeBootstrapping(DataStream<T> stream) throws Exception {
		List<Long> timestamps = stream.process(new ProcessFunction<T, Long>() {

			@Override
			public void processElement(T arg0, ProcessFunction<T, Long>.Context arg1,
					org.apache.flink.util.Collector<Long> arg2) throws Exception {
				arg2.collect(arg1.timestamp());
			}

		})
				.keyBy(x -> x)
				// .window(TumblingEventTimeWindows.of(Time.seconds(2)))
				// .sum(0)
				.flatMap(new Deduplicate<Long>())
				.executeAndCollect(100);
		Long current = timestamps.get(0);
		for (long t : timestamps) {
			if (t - current > 15) {
				current = t;
				break;
			}
			current = t;
		}
		final long currentF = current;
		return stream.process(new ProcessFunction<T, T>() {

			@Override
			public void processElement(T arg0, ProcessFunction<T, T>.Context arg1,
					org.apache.flink.util.Collector<T> arg2) throws Exception {
				if (currentF <= arg1.timestamp()) {
					arg2.collect(arg0);
				}
			}

		});
	}

	static final class DropBootstrapping
			extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>>
			implements ResultTypeQueryable {

		ValueState<Long> time;
		ValueState<Boolean> drop;

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Long> time = new ValueStateDescriptor<>("time", Types.LONG);
			ValueStateDescriptor<Boolean> drop = new ValueStateDescriptor<>("drop", Types.BOOLEAN);
			this.time = getRuntimeContext().getState(time);
			this.drop = getRuntimeContext().getState(drop);
		}

		@Override
		public void processElement(Tuple2<String, String> elem,
				KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>>.Context ctx,
				org.apache.flink.util.Collector<Tuple2<String, String>> out)
				throws Exception {
			if (drop.value() != null) {
				out.collect(elem);
			} else if (time.value() == null) {
				time.update(ctx.timestamp());
			} else if (ctx.timestamp() - time.value() > (15 * 1000)) {
				drop.update(false);
				// System.out.println("error:" + elem);
				out.collect(elem);
			} else {
				time.update(ctx.timestamp());
			}
		}

		@Override
		public TypeInformation getProducedType() {
			return TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {
			});
		}

	}

	static final class MapToEventTime<T> extends ProcessFunction<T, Long> {
		@Override
		public void processElement(T elem, ProcessFunction<T, Long>.Context ctx,
				org.apache.flink.util.Collector<Long> out)
				throws Exception {
			out.collect(ctx.timestamp());
		}
	}

	static final class FilterAfter<T> extends ProcessFunction<T, T> {
		long t;

		public FilterAfter(long t) {
			this.t = t;
		}

		@Override
		public void processElement(T elem, ProcessFunction<T, T>.Context ctx, org.apache.flink.util.Collector<T> out)
				throws Exception {
			if (t <= ctx.timestamp()) {
				out.collect(elem);
			}
		}
	}

	static final class Deduplicate<T> extends RichFlatMapFunction<T, T> {
		ValueState<Boolean> seen;

		@Override
		public void open(Configuration conf) {
			ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
			seen = getRuntimeContext().getState(desc);
		}

		@Override
		public void flatMap(T value, org.apache.flink.util.Collector<T> out) throws Exception {
			if (seen.value() == null) {
				out.collect(value);
				seen.update(true);
			}
		}
	}
}

package nl.tudelft;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.SerializableFunction;

public class Predicate {

    static final int WINDOW = 3;

    int threshold;
    String verbL, verbR, text;
    SerializableFunction<Tuple2<String, String>, Boolean> pred;

    public Predicate(String verbL, String verbR, SerializableFunction<Tuple2<String, String>, Boolean> pred, String text, int threshold) {
        this.pred = pred;
        this.text = text;
        this.verbL = verbL;
        this.verbR = verbR;
        this.threshold = threshold;
    }

    public DataStream<Tuple2<String, String>> apply(DataStream<Tuple2<String, String>> stream) {
        return CEP.pattern(stream, pattern()).inEventTime().process(process());
    }

    private Pattern<Tuple2<String, String>, ?> pattern() {
        final String verbLSer = verbL;
        final String verbRSer = verbR;
        return Pattern
            .<Tuple2<String, String>>begin("verbL", AfterMatchSkipStrategy.skipToNext())
            .where(SimpleCondition.of(event -> event.f1.contains(verbLSer)))
            .followedBy("middle")
            .where(new IterativeCondition<Tuple2<String,String>>() {

                @Override
                public boolean filter(Tuple2<String, String> value, Context<Tuple2<String, String>> ctx)
                        throws Exception {
                    if (!value.f1.contains(verbLSer)) return false;
                    Tuple2<String, String> left = ctx.getEventsForPattern("verbL").iterator().next();
                    return Utils.seq(left.f1) == Utils.seq(value.f1);
                }
                
            })
            .timesOrMore(threshold - 1)
            .followedBy("verbR")
            .where(SimpleCondition.of(event -> event.f1.contains(verbRSer)))
            .within(Time.seconds(WINDOW));
    }

    private PatternProcessFunction<Tuple2<String, String>, Tuple2<String, String>> process() {
        return new PatternProcessor(text, pred);
    }

    static class PatternProcessor extends PatternProcessFunction<Tuple2<String,String>,Tuple2<String,String>> {

        String text;
        SerializableFunction<Tuple2<String, String>, Boolean> pred;

        public PatternProcessor(String text, SerializableFunction<Tuple2<String, String>, Boolean> pred) {
            super();
            this.text = text;
            this.pred = pred;
        }

        @Override
        public void processMatch(Map<String, List<Tuple2<String, String>>> match, Context ctx,
                Collector<Tuple2<String, String>> out) throws Exception {
                    Tuple2<String, String> right = match.get("verbR").get(0);
                    // for (Tuple2<String, String> left : match.get("verbL")) {
                    //     if (!pred.apply(Tuple2.of(left.f1, right.f1))); //out.collect(Tuple2.of(right.f0, "False " + text + " " + match.toString()));
                    // }
                    if (Utils.seq(match.get("verbL").get(0).f1) + 1 == Utils.seq(right.f1)) {
                        out.collect(Tuple2.of(right.f0, "True " + text + Utils.seq(right.f1) + "," + Utils.seq(match.get("verbL").get(0).f1)+ " " + match.toString()));
            
                    } else {
                        out.collect(Tuple2.of(right.f0, "False " + text + Utils.seq(right.f1) + "," + Utils.seq(match.get("verbL").get(0).f1)+ " " + match.toString()));
                    
                    }
                    // out.collect(Tuple2.of(right.f0, "True " + text + Utils.seq(right.f1) + "," + Utils.seq(match.get("verbL").get(0).f1)+ " " + match.toString()));
                    // out.collect(Tuple2.of(right.f0, "True " + text + " " + match.toString()));
        }}

}

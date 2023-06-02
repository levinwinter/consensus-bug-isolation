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

public class IncompatibleLedgerPredicate {

    public IncompatibleLedgerPredicate() {
    }

    public DataStream<Tuple2<String, String>> apply(DataStream<Tuple2<String, String>> stream) {
        return CEP.pattern(stream, pattern()).inEventTime().process(process());
    }

    private Pattern<Tuple2<String, String>, ?> pattern() {
        return Pattern
            .<Tuple2<String, String>>begin("start", AfterMatchSkipStrategy.skipToNext())
            .where(SimpleCondition.of(event -> event.f1.contains("Validation") && ((Utils.to(event.f1) > 3) ? Utils.validatorNumber(event.f1) > 1 : Utils.validatorNumber(event.f1) < 5)))
            .followedBy("middle")
            .where(new IterativeCondition<Tuple2<String,String>>() {

                @Override
                public boolean filter(Tuple2<String, String> value, Context<Tuple2<String, String>> ctx)
                        throws Exception {
                    if (!value.f1.contains("Validation")) return false;
                    if (!(Utils.to(value.f1) > 3 ? Utils.validatorNumber(value.f1) > 1 : Utils.validatorNumber(value.f1) < 5)) return false;
                    Tuple2<String, String> left = ctx.getEventsForPattern("start").iterator().next();
                    if (Utils.seq(left.f1) != Utils.seq(value.f1)) return false;
                    if (!Utils.hash(left.f1).equals(Utils.hash(value.f1))) return false;
                    if (Utils.validatorNumber(left.f1) == Utils.validatorNumber(value.f1)) return false;
                    for (Tuple2<String, String> pair : ctx.getEventsForPattern("middle")) {
                        if (Utils.seq(pair.f1) != Utils.seq(value.f1)) return false;
                        if (!Utils.hash(pair.f1).equals(Utils.hash(value.f1))) return false;
                        if (Utils.validatorNumber(pair.f1) == Utils.validatorNumber(value.f1)) return false;
                    }
                    return true;
                }
                
            })
            .timesOrMore(3)
            .notFollowedBy("not")
            .where(new IterativeCondition<Tuple2<String,String>>() {

                @Override
                public boolean filter(Tuple2<String, String> value, Context<Tuple2<String, String>> ctx)
                        throws Exception {
                    if (!value.f1.contains("Validation")) return false;
                    Tuple2<String, String> left = ctx.getEventsForPattern("start").iterator().next();
                    return Utils.seq(left.f1) + 1 == Utils.seq(value.f1) && !Utils.hash(left.f1).equals(Utils.hash(value.f1));
                }
                
            })
            .followedBy("end")
            .where(new IterativeCondition<Tuple2<String,String>>() {

                @Override
                public boolean filter(Tuple2<String, String> value, Context<Tuple2<String, String>> ctx)
                        throws Exception {
                    if (!value.f1.contains("Validation")) return false;
                    Tuple2<String, String> left = ctx.getEventsForPattern("start").iterator().next();
                    return Utils.seq(left.f1) + 1 == Utils.seq(value.f1) && Utils.hash(left.f1).equals(Utils.hash(value.f1));
                }
                
            })
            .within(Time.seconds(3));
    }

    private PatternProcessFunction<Tuple2<String, String>, Tuple2<String, String>> process() {
        return new PatternProcessor();
    }

    static class PatternProcessor extends PatternProcessFunction<Tuple2<String,String>,Tuple2<String,String>> {

        @Override
        public void processMatch(Map<String, List<Tuple2<String, String>>> match, Context ctx,
                Collector<Tuple2<String, String>> out) throws Exception {
                    Tuple2<String, String> right = match.get("end").get(0);
                    // for (Tuple2<String, String> left : match.get("verbL")) {
                    //     if (!pred.apply(Tuple2.of(left.f1, right.f1))); //out.collect(Tuple2.of(right.f0, "False " + text + " " + match.toString()));
                    // }
                    // if (Utils.seq(match.get("verbL").get(0).f1) + 1 == Utils.seq(right.f1)) {
                    //     out.collect(Tuple2.of(right.f0, "True " + text + Utils.seq(right.f1) + "," + Utils.seq(match.get("verbL").get(0).f1)+ " " + match.toString()));
            
                    // } else {
                    //     out.collect(Tuple2.of(right.f0, "False " + text + Utils.seq(right.f1) + "," + Utils.seq(match.get("verbL").get(0).f1)+ " " + match.toString()));
                    
                    // }
                    out.collect(Tuple2.of(right.f0, match.toString()));
                    // out.collect(Tuple2.of(right.f0, "True " + text + Utils.seq(right.f1) + "," + Utils.seq(match.get("verbL").get(0).f1)+ " " + match.toString()));
                    // out.collect(Tuple2.of(right.f0, "True " + text + " " + match.toString()));
        }}

}

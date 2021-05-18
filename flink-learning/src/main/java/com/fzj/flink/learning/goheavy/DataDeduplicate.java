//package com.fzj.flink.learning.goheavy;
//
//import com.fzj.flink.learning.domain.UserAction;
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.state.StateTtlConfig;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
//import java.time.Duration;
//import java.util.Arrays;
//
///**
// * @Author: aze
// * @Date: 2020-09-16 14:45
// */
//public class DataDeduplicate {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(1);
//
//        DataStreamSource<UserAction> dataStream = env.fromCollection(Arrays.asList(
//                new UserAction("userID1", 1293984000, "click", "productID1", 10),
//                new UserAction("userID2", 1293984001, "browse", "productID2", 8),
//                new UserAction("userID2", 1293984002, "browse", "productID2", 8),
//                new UserAction("userID2", 1293984003, "browse", "productID2", 8),
//                new UserAction("userID1", 1293984002, "click", "productID1", 10),
//                new UserAction("userID1", 1293984003, "click", "productID3", 10),
//                new UserAction("userID1", 1293984004, "click", "productID1", 10)
//        ));
//         dataStream
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//                    @Override
//                    public void flatMap(String s, Collector<Tuple2<String, Long>> out) throws Exception {
//                        String[] split = s.split(",");
//                        if ("pv".equals(split[3])) {
//                            Tuple2 res = new Tuple2<>(split[0] + "-" + split[1], Long.parseLong(split[4]));
//                            out.collect(res);
//                        }
//                    }
//                })
//                 .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(1000))
//                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>)
//                                (s, l) -> s.f1))
//                .keyBy(s -> s)
//                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Object>() {
//
//                    private ValueState<UserAction> state;
//
//                    @Override
//                    public void open(Configuration parameters) {
//                        ValueStateDescriptor stateDescriptor = new ValueStateDescriptor<>("mystate", UserAction.class);
//                        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(60)).build());
//                        state = getRuntimeContext().getState(stateDescriptor);
//                    }
//
//                    @Override
//                    public void processElement(Tuple2<String, Long> in,
//                                               Context ctx,
//                                               Collector<Object> out) throws Exception {
//                        UserAction cur = state.value();
//                        if (cur == null) {
//                            cur = new UserAction(in.f0, in.f1, in.f0, in.f0, in.f1);
//                            state.update(cur);
//                            ctx.timerService().registerEventTimeTimer(cur.getTime() + 60000);
//                            out.collect(cur);
//                        } else {
//                            System.out.println("[Duplicate Data] " + in.f0 + " " + in.f1);
//                        }
//                    }
//
//                    @Override
//                    public void onTimer(long timestamp,
//                                        OnTimerContext ctx,
//                                        Collector<Object> out) throws Exception {
//                        UserAction cur = state.value();
//                        if (cur.getTime() + 1000 <= timestamp) {
//                            System.out.printf("[Overdue] now: %d obj_time: %d Date: %s%n",
//                                    timestamp, cur.getTime(), cur.getUserId());
//                            state.clear();
//                        }
//                    }
//                });
//        dataStream.print();
//        env.execute("flink");
//
//    }
//}

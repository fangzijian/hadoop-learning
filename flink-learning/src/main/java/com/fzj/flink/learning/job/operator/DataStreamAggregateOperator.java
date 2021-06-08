package com.fzj.flink.learning.job.operator;

import com.fzj.flink.learning.domain.UserAction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Summary:
 *     Aggregate: min()、minBy()、max()、maxBy() 滚动聚合并输出每次滚动聚合后的结果
 */
class DataStreamAggregateOperator {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入: 用户行为。某个用户在某个时刻点击或浏览了某个商品，以及商品的价格。
        ArrayList<UserAction> userActionLogs = new ArrayList<>();
        UserAction userActionLog1 = new UserAction();
        userActionLog1.setUserId("userID1");
        userActionLog1.setProduce("productID3");
        userActionLog1.setTime(10);
        userActionLogs.add(userActionLog1);

        UserAction userActionLog2 = new UserAction();
        userActionLog2.setUserId("userID2");
        userActionLog2.setTime(10);
        userActionLogs.add(userActionLog2);

        UserAction userActionLog3 = new UserAction();
        userActionLog3.setUserId("userID1");
        userActionLog3.setProduce("productID5");
        userActionLog3.setTime(30);
        userActionLogs.add(userActionLog3);

        DataStreamSource<UserAction> source = env.fromCollection(userActionLogs);

        // 转换: KeyBy对数据重分区
        // 这里, UserActionLog是POJO类型,也可通过keyBy("userID")进行分区
        KeyedStream<UserAction, String> keyedStream = source.keyBy(new KeySelector<UserAction, String>() {
            @Override
            public String getKey(UserAction value) throws Exception {
                return value.getUserId();
            }
        });

        // 转换: Aggregate并输出
        // 滚动求和并输出
        //keyedStream.sum("productPrice").print();
        // 滚动求最大值并输出
        keyedStream.max("productPrice").print();
        // 滚动求最大值并输出
        keyedStream.maxBy("productPrice").print();
        // 滚动求最小值并输出
        //keyedStream.min("productPrice").print();
        // 滚动求最小值并输出
        //keyedStream.minBy("productPrice").print();

        env.execute();
    }
}

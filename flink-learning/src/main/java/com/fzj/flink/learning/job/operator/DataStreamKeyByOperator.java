package com.fzj.flink.learning.job.operator;

import com.fzj.flink.learning.domain.UserAction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class DataStreamKeyByOperator {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 输入: 用户行为。某个用户在某个时刻点击或浏览了某个商品，以及商品的价格。
        DataStreamSource<UserAction> source = env.fromCollection(Arrays.asList(
                new UserAction("userID1", 1293984000, "click", "productID1", 10),
                new UserAction("userID2", 1293984001, "browse", "productID2", 8),
                new UserAction("userID1", 1293984002, "click", "productID1", 10)
        ));

        // 转换: 按指定的Key(这里,用户ID)对数据重分区，将相同Key(用户ID)的数据分到同一个分区
        KeyedStream<UserAction, String> result = source.keyBy((KeySelector<UserAction, String>) UserAction::getUserId);

        // 输出: 输出到控制台
        //3> UserAction(userID=userID1, eventTime=1293984000, eventType=click, productID=productID1, productPrice=10)
        //3> UserAction(userID=userID1, eventTime=1293984002, eventType=click, productID=productID1, productPrice=10)
        //2> UserAction(userID=userID2, eventTime=1293984001, eventType=browse, productID=productID2, productPrice=8)
        result.print().setParallelism(3);

        env.execute();

    }
}
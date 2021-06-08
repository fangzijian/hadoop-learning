package com.fzj.flink.learning.job.goheavy;

import com.fzj.flink.learning.domain.UserAction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import static org.apache.flink.table.api.Expressions.$;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

public class SQLProcess {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 输入: 用户行为。某个用户在某个时刻点击或浏览了某个商品，以及商品的价格。
        DataStreamSource<UserAction> ds = env.fromCollection(Arrays.asList(
                new UserAction("userID1", 1293984000, "click", "productID1", 10),
                new UserAction("userID2", 1293984001, "browse", "productID2", 8),
                new UserAction("userID2", 1293984001, "browse", "productID1", 9),
                new UserAction("userID2", 1293984001, "browse", "productID2", 7),
                new UserAction("userID1", 1293984002, "click", "productID1", 11)
        ));
        // 注册名为 “heavy” 的 DataStream
        tableEnv.createTemporaryView("heavy", ds, $("userId"), $("date"), $("action"), $("produce"), $("time").proctime());

        Table table = tableEnv.sqlQuery(
                "SELECT userId, action, product, time " +
                        "FROM (" +
                        "   SELECT *," +
                        "       ROW_NUMBER() OVER (PARTITION BY userId ORDER BY time ASC) as row_num" +
                        "   FROM heavy)" +
                        "WHERE row_num = 1");
        //TODO xxx
    }
}

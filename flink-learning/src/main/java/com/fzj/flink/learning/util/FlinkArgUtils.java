package com.fzj.flink.learning.util;


import com.fzj.flink.learning.constant.TaskParamName;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 参数工具
 */
public class FlinkArgUtils {

    public static List<String> topicList;
    public static String servers_host = "127.0.0.1:9092";
    public static String hdfsPath = "hdfs://127.0.0.1:9000/fzj/kafkaArchive";

    public static int checkpointing;
    public static int checkpointTimeout;
    public static String taskName;

    public static void convertArg(ParameterTool parameterTool) {
        String servers = parameterTool.get(TaskParamName.KAFKA_BOOTSTRAP_SERVERS);
        if (StringUtils.isNotBlank(servers)) {
            servers_host = servers;
        }
        String hdfs_Path = parameterTool.get(TaskParamName.HDFS_PATH);
        if (StringUtils.isNotBlank(hdfs_Path)) {
            hdfsPath = hdfs_Path;
        }
        String kafka_topic = parameterTool.get(TaskParamName.KAFKA_TOPIC);
        if (StringUtils.isNotBlank(kafka_topic)) {
            String[] split = kafka_topic.split(",");
            topicList = Arrays.asList(split);
        } else {
            topicList = Collections.singletonList("PUSH_MESSAGE_MQ");
        }
        //taskName 初始化
        taskName = Optional.ofNullable(parameterTool.get(TaskParamName.TASK_NAME))
                .orElse("flinkTaskName");
        checkpointing = Integer.parseInt(Optional.ofNullable(
                parameterTool.get(TaskParamName.CHECKPOINTING)).orElse("60000")); //默认一分钟
        checkpointTimeout = Integer.parseInt(Optional.ofNullable(
                parameterTool.get(TaskParamName.CHECKPOINT_TIMEOUT)).orElse("600000"));//默认十分钟
    }
}

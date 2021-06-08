package com.fzj.flink.learning.job.kafka;

import com.fzj.flink.learning.constant.Constant;
import com.fzj.flink.learning.constant.TaskParamName;
import com.fzj.flink.learning.util.FlinkArgUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 读取kafka ，用orc 格式写hdfs，按天分桶
 */
public class KafkaToHdfs {

    public static final String FORMAT_DAY_NO = "yyyyMMdd";

    static Logger log = LoggerFactory.getLogger(KafkaToHdfs.class);

    public static void main(String[] args) throws Exception {
        log.info("args:{}", (Object) args);
        /* 初始化运行参数 */
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        FlinkArgUtils.convertArg(parameterTool);
        //初始化
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(FlinkArgUtils.checkpointing);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(50000);
        env.getCheckpointConfig().setCheckpointTimeout(FlinkArgUtils.checkpointTimeout);
        env.getConfig().setGlobalJobParameters(parameterTool);

        //读取kafka
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkArgUtils.servers_host);
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, TaskParamName.KAFKA_GROUP_ID);
        DataStream<RowData> flatMap;
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(FlinkArgUtils.topicList, new SimpleStringSchema(), kafkaProperties);
        kafkaConsumer.setStartFromLatest();
        DataStream<String> kafkaInitStream = env.addSource(kafkaConsumer);
        flatMap = kafkaInitStream.flatMap((FlatMapFunction<String, RowData>) (message, collector) -> {
            String[] split = message.split(",");
            for (String str : split) {
                GenericRowData rowData = new GenericRowData(4);
                rowData.setField(0, Integer.parseInt(str));
                rowData.setField(1, str);
                rowData.setField(2, str);
                rowData.setField(3, str);
                collector.collect(rowData);
            }
        }).returns(RowData.class);
        flatMap.print();

        /* 添加目标数据源 */
        flatMap.addSink(getOrcSink());
        env.execute(FlinkArgUtils.taskName);
    }

    /**
     * 以 orc 格式写入
     */
    private static StreamingFileSink<RowData> getOrcSink() {
        //写hive
        final Properties orcProperties = new Properties();
        orcProperties.setProperty("orc.compress", "LZ4");
        LogicalType[] orcTypes = new LogicalType[] {
                new IntType(), new VarCharType(255), new VarCharType(255), new VarCharType(50)
        };
        TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(RowType.of(
                orcTypes, Constant.ROW_TYPE
        ));
        final OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
                new RowDataVectorizer(typeDescription.toString(), orcTypes),
                orcProperties,
                new Configuration()
        );
        return StreamingFileSink
                .forBulkFormat(new Path(FlinkArgUtils.hdfsPath), factory)
                .withBucketAssigner(new DateTimeBucketAssigner<>(FORMAT_DAY_NO))
                .build();
    }

}

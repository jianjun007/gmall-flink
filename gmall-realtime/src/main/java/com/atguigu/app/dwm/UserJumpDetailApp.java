package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//数据流：web/app -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)
//程  序：Mock    -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> UserJumpDetailApp -> Kafka(ZK)
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境应该设置为Kafka主题的分区数

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取Kafka dwd_page_log 主题数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_app_210726";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.提取事件时间生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO 5.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 6.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                return value.getJSONObject("page").getString("last_page_id") == null;
//            }
//        }).times(2).consecutive().within(Time.seconds(10));

        //TODO 7.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 8.提取匹配上的事件以及超时事件
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });
        selectDS.print("Select>>>>");
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(outputTag);
        timeOutDS.print("TimeOut>>>>>");

        //TODO 9.Union
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        //TODO 10.将数据写入Kafka
        unionDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        unionDS.print("UnionDS>>>>>>");

        //TODO 11.启动任务
        env.execute("UserJumpDetailApp");

    }

}

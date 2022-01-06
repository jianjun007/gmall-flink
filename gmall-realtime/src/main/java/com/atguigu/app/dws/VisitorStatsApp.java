package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {

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

        //TODO 2.读取Kafka数据
        String groupId = "visitor_stats_app_210726";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);

        //TODO 3.统一数据格式
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPVDS = pageViewDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            long sv = 0L;
            if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUVDS = uniqueVisitDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUJDS = userJumpDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //TODO 4.Union多个流
        DataStream<VisitorStats> unionDS = visitorStatsWithPVDS.union(visitorStatsWithUVDS, visitorStatsWithUJDS);

        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(14)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
            @Override
            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 6.分组、开窗、聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = visitorStatsWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(),
                        value.getCh(),
                        value.getVc(),
                        value.getIs_new());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));


//        windowedStream.reduce(new ReduceFunction<VisitorStats>() {
//            @Override
//            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
//                return null;
//            }
//        });
//        windowedStream.apply(new WindowFunction<VisitorStats, Object, Tuple4<String, String, String, String>, TimeWindow>() {
//            @Override
//            public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<Object> collector) throws Exception {
//            }
//        });
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {

                //取出数据
                VisitorStats visitorStats = iterable.iterator().next();

                //补充窗口信息
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(timeWindow.getStart())));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(timeWindow.getEnd())));

                //输出数据
                collector.collect(visitorStats);
            }
        });

        //TODO 7.将数据写入ClickHouse
        result.print(">>>>>>>");
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats_210726 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("VisitorStatsApp");

    }

}

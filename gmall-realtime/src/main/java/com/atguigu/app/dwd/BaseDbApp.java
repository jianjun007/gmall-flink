package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.func.DimSinkFunction;
import com.atguigu.func.MyCustomDeserializer;
import com.atguigu.func.TableProcessFunction;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author JianJun
 * @create 2021/12/27 0:50
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境应该设置为Kafka主题的分区数


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

        //TODO 2.读取Kafka主题数据,创建对应的流
        String topic = "ods_base_db";
        String groupId = "base_db_app_210726";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));
        kafkaDS.print("kafkaDS>>>>>>");

        //TODO 3.将数据转换为为JSON格式,过滤掉删除数据
        OutputTag<String> outputTag = new OutputTag<String>("DirtyData") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                //过滤掉非JSON格式的数据,并将脏数据放入侧输出流中
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        }).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        //TODO 4.使用FlinkCDC读取配置表创建配置流
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink_realtime")
                .tableList("gmall_flink_realtime.table_process")
                .deserializer(new MyCustomDeserializer())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> flinkCdcDS = env.addSource(sourceFunction);
        flinkCdcDS.print("flinkCdcDS>>>>>>");

        //TODO 5.将配置流转换为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = flinkCdcDS.broadcast(mapStateDescriptor);

        //TODO 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);


        //TODO 7.根据广播流处理主流数据
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaMainDS = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        //TODO 8.将维度数据写入Phoenix
        DataStream<JSONObject> hbaseDS = kafkaMainDS.getSideOutput(hbaseTag);
        hbaseDS.print("Hbase>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        //TODO 9.将事实数据写入Kafka
        kafkaMainDS.print("Kafka>>>>>>");
        kafkaMainDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            //jsonObject:{"database":"gmall-210726-flink","before":{},"after":{"user_id":"1001","id":17},"type":"insert","tableName":"order_info","sinkTable":"dwd_order_info"}
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {

                return new ProducerRecord<>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));

        //TODO 10.启动任务
        env.execute("BaseDBApp");

    }
}

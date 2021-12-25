package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author JianJun
 * @create 2021/12/24 11:27
 */
public class FlinkCDC_DataStream_Deserializer {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2. 设置FlinkCDC参数
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_flink")
                .username("root")
                .password("123456")
//                .tableList("gmall_flink.base_region")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute();


    }

    /**
     * 发送到Kafka中的数据封装成JSON格式
     * {
     *     "database":"gmall_210726",
     *     "tableName":"base_trademark",
     *     "type":"insert",
     *     "before":{"id":"101","name":"zs"...}
     *     "after":{"id":"102","name":"zs"...}
     * }
     */

    //自定义反序列化器
    private static class MyDeserial implements com.ververica.cdc.debezium.DebeziumDeserializationSchema<String> {


        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //1.创建一个JSONObject用来存放最终封装的数据
            JSONObject jsonObject = new JSONObject();

            //TODO 2.提取数据库名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String database = split[1];

            //TODO 3.提取表名
            String tableName = split[2];

            Struct value = (Struct) sourceRecord.value();
            //TODO 4.获取after数据
            Struct afterStruct = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            //判断是否有after数据
            if (afterStruct != null) {
                List<Field> fields = afterStruct.schema().fields();
                for (Field field : fields) {
                    afterJson.put(field.name(), afterStruct.get(field));
                }
            }

            //TODO 5.获取beford数据
            Struct beforeStruct = value.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            //判断是否有after数据
            if (beforeStruct != null) {
                List<Field> fields = beforeStruct.schema().fields();
                for (Field field : fields) {
                    beforeJson.put(field.name(), beforeStruct.get(field));
                }
            }

            //TODO 6.获取操作类型 READ DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            //将获取的类型转换成小写
            String type = operation.toString().toLowerCase();
            //将create替换为insert
            if ("create".equals(type)) {
                type = "insert";
            }

            //TODO 7.封装数据
            jsonObject.put("database", database);
            jsonObject.put("tableName", tableName);
            jsonObject.put("after", afterJson);
            jsonObject.put("before", beforeJson);
            jsonObject.put("type", type);

            collector.collect(jsonObject.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}

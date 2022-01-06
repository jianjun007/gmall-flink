package com.atguigu.func;

import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author JianJun
 * @create 2021/12/24 19:38
 */
//自定义反序列化器
public class MyCustomDeserializer implements com.ververica.cdc.debezium.DebeziumDeserializationSchema<String> {


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

package com.atguigu.app;

import com.atguigu.utils.MyDeserial;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JianJun
 * @create 2021/12/24 19:36
 */
public class Flink_CDCWithCustomerSchema {
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

        //TODO 3.将数据发送至Kafka
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));


        streamSource.print();

        env.execute();


    }
}

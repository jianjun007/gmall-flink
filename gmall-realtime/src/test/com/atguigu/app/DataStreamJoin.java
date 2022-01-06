package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JianJun
 * @create 2021/12/29 9:25
 */
public class DataStreamJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Bean1> ds1 = env.socketTextStream("localhost", 9991)
                .map(value -> {
                    String[] split = value.split(",");
                    return new Bean1(split[0], split[1], Long.parseLong(split[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                            @Override
                            public long extractTimestamp(Bean1 element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        }));

        SingleOutputStreamOperator<Bean2> ds2 = env.socketTextStream("localhost", 9992)
                .map(value -> {
                    String[] split = value.split(",");
                    return new Bean2(split[0], split[1], Long.parseLong(split[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                            @Override
                            public long extractTimestamp(Bean2 element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        }));
    }
}

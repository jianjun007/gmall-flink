package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * for (Field declaredField : declaredFields) {
 * Object valule = declaredField.get(t);
 * }
 * Method[] methods = clz.getMethods();
 * for (Method method : methods) {
 * method.invoke(t);
 * }
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //通过反射的方式获取T对象所有的属性
                        Class<?> clz = t.getClass();
                        Field[] declaredFields = clz.getDeclaredFields();

                        //遍历属性,取出值给预编译SQL对象占位符赋值
                        int j = 0;
                        for (int i = 0; i < declaredFields.length; i++) {

                            Field field = declaredFields[i];

                            //获取当前字段的注解信息
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                j++;
                                continue;
                            }

                            //设置可访问
                            field.setAccessible(true);
                            Object value = field.get(t);

                            //给占位符赋值
                            preparedStatement.setObject(i + 1 - j, value);
                        }

                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}

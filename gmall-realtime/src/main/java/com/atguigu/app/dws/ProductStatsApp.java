package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.func.DimAsyncFunction;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {

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

        //TODO 2.消费Kafka数据创建流
        String groupId = "product_stats_app_210726";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 3.统一数据格式
        // 3.1 处理点击&曝光数据
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {

                //提取时间戳
                JSONObject jsonObject = JSON.parseObject(value);
                Long ts = jsonObject.getLong("ts");

                //点击数据
                JSONObject page = jsonObject.getJSONObject("page");
                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {

                    //遍历曝光数据
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        // 3.2 处理收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.3 处理加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.4 处理下单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        // 3.5 处理支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = paymentWideDStream.map(line -> {

            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .paidOrderIdSet(orderIds)
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        // 3.6 处理退单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refundOrderIdSet(orderIds)
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.7 处理评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            String appraise = jsonObject.getString("appraise");
            Long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //TODO 4.合并多个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 6.分组开窗&聚合
        SingleOutputStreamOperator<ProductStats> productStatsDS = productStatsWithWMDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());

                        //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L); //
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L); //
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);//

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;

                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {

                        //提取数据
                        ProductStats productStats = iterable.iterator().next();

                        //补充订单次数
                        productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                        productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());
                        productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());

                        //补充窗口信息
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(timeWindow.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(timeWindow.getEnd())));

                        //输出数据
                        collector.collect(productStats);

                    }
                });

        //TODO 7.关联维度信息

        //7.1 关联SKU
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(productStatsDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        //7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //7.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //7.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //TODO 8.将数据写入ClickHouse
        productStatsWithTmDstream.print(">>>>>");
        productStatsWithTmDstream.addSink(ClickHouseUtil.getSink("insert into product_stats_210726  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("ProductStatsApp");

    }

}

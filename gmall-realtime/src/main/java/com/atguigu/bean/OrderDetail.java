package com.atguigu.bean;

/**
 * @author JianJun
 * @create 2021/12/29 8:45
 */

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderDetail {
    Long id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}
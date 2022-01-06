package com.atguigu.app;

import cn.hutool.core.date.DateUtil;

/**
 * @author JianJun
 * @create 2021/12/30 14:52
 */
public class Test01 {
    public static void main(String[] args) {
        String s = " 2000-09-09";
        System.out.println(DateUtil.ageOfNow(s));
    }
}

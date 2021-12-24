package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author JianJun
 * @create 2021/12/22 16:23
 */
@RestController
@Slf4j
public class LoggerController {

    @RequestMapping("test01")
    public String test01() {
        System.out.println("111111");
        return "success";
    }

    @RequestMapping("test02")
    public String test02(@RequestParam("name") String name,
                         @RequestParam Integer age) {
        System.out.println(name + ":" + age);
        return "success";
    }

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        //落盘
        log.info(jsonStr);

        //写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }

}

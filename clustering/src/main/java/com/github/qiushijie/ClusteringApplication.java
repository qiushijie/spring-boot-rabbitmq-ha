package com.github.qiushijie;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class ClusteringApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClusteringApplication.class, args);
    }

    private static Logger logger = LoggerFactory.getLogger(ClusteringApplication.class);

    @Component
    @RabbitListener(queues = "hello")
    public class HelloListener {

        @RabbitHandler
        public void process(String name) {
            logger.info("hello queue receiver: " + name);
        }

    }

    @Component
    @RabbitListener(queues = "ha_hello")
    public class HAHelloListener {

        @RabbitHandler
        public void process(String name) {
            logger.info("ha_hello queue receiver: " + name);
        }

    }

    @Component
    @RabbitListener(queues = "tmp")
    public class TmpListener {

        @RabbitHandler
        public void process(String hello) {
            logger.info("tmp receiver: " + hello);
        }

    }

    @Component
    @RabbitListener(queues = "quorum")
    public class QuorumQueueListener {

        @RabbitHandler
        public void process(String hello) {
            logger.info("quorum queue receiver: " + hello);
        }

    }

    @Bean
    public Queue helloQueue() {
        return new Queue("hello");
    }

    @Bean
    public Queue tmpQueue() {
        return QueueBuilder.nonDurable("tmp").build();
    }

    @Bean
    public Queue haHelloQueue() {
        return new Queue("ha_hello");
    }

    @Bean
    public Queue haAllQueue() {
        return QueueBuilder.durable("ha_all")
                .withArgument("x-ha-policy", "all")
                .build();
    }

    @Bean
    public Queue quorumQueue() {
        return QueueBuilder.durable("quorum")
                .withArgument("x-queue-type", "quorum")
                .build();
    }

    @Bean
    public RabbitAdmin rabbitAdmin(RabbitTemplate rabbitTemplate) {
        return new RabbitAdmin(rabbitTemplate);
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;

    @GetMapping("/hi")
    public String hi(String name) {
        logger.info("hello queue send:" + name);
        rabbitTemplate.convertAndSend("hello", name);
        return "ok";
    }

    @GetMapping("/tmp")
    public String tmp(String name) {
        logger.info("tmp queue send:" + name);
        rabbitTemplate.convertAndSend("tmp", name);
        return "ok";
    }

    @GetMapping("/ha_hi")
    public String haHi(String name) {
        logger.info("ha_hello queue send:" + name);
        rabbitTemplate.convertAndSend("ha_hello", name);
        return "ok";
    }

    @GetMapping("/quorum_hi")
    public String hiQuorum(String name) {
        logger.info("quorum queue send:" + name);
        rabbitTemplate.convertAndSend("quorum", name);
        return "ok";
    }

}

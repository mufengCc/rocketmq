package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SyncProducer {
    public static void main(String[] args) throws Exception {

        // 如果不再发送消息，关闭Producer实例。
        //producer.shutdown();

        product();

        System.out.println("开始注册消费者");

//        consumer();
//        consumer();
//        consumer2();

        System.out.println("消费者注册成功");

    }

    public static void product() throws UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException, MQClientException {

        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("my_xxx_group_name");

        // 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");

        // 启动Producer实例、初始化topic信息等等
        System.out.println("开始注册生产者");

        producer.start();

        System.out.println("生成者注册成功");

        for (int i = 0; i < 10; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 发送消息到一个Broker
            //SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            //System.out.printf("%s%n", sendResult);
        }

        for (int i = 0; i < 1; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );

            msg.setDelayTimeLevel(2);
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg);

//            producer.send(msg, new SendCallback() {
//                @Override
//                public void onSuccess(SendResult sendResult) {
//                    System.out.println("发送消息成功");
//                }
//
//                @Override
//                public void onException(Throwable e) {
//                    System.out.println("发送消息失败");
//
//                }
//            });

            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }
    }


    public static void consumer() throws InterruptedException, MQClientException {

        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("topic_test_consumer_group");

        // 设置NameServer的地址
        consumer.setNamesrvAddr("localhost:9876");

        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe("TopicTest", "*");
        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    public static void consumer2() throws InterruptedException, MQClientException {

        // 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("My_topic_consumer_group");

        // 设置NameServer的地址
        consumer.setNamesrvAddr("localhost:9876");

        // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe("My_topic", "*");
        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }


    /**
     * DefaultLitePullConsumer 手动提交消费位点
     */
    public void noAutoCommit() throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("topic_test_consumer_group");
        litePullConsumer.setNamesrvAddr("localhost:9876");
        // 关闭自动提交消费位点，默认为true
        litePullConsumer.setAutoCommit(false);
        litePullConsumer.start();
        Collection<MessageQueue> mqSet = litePullConsumer.fetchMessageQueues("order-topic");
        List<MessageQueue> list = new ArrayList<>(mqSet);
        List<MessageQueue> assignList = new ArrayList<>();
        for (int i = 0; i < list.size() / 2; i++) {
            assignList.add(list.get(i));
        }
        litePullConsumer.assign(assignList);
        litePullConsumer.seek(assignList.get(0), 10);
        try {
            while (true) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                // 开始消费消息
                System.out.printf("%s %n", messageExts);
                // 关闭提交时，需手动提交
                litePullConsumer.commit();
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }

    /**
     * DefaultLitePullConsumer 自动提交消费位点
     */
    public void autoCommit() throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("topic_test_consumer_group");
        litePullConsumer.setNamesrvAddr("localhost:9876");
        litePullConsumer.setAutoCommit(true);
        litePullConsumer.start();
        Collection<MessageQueue> mqSet = litePullConsumer.fetchMessageQueues("order-topic");
        List<MessageQueue> list = new ArrayList<>(mqSet);
        List<MessageQueue> assignList = new ArrayList<>();
        for (int i = 0; i < list.size() / 2; i++) {
            assignList.add(list.get(i));
        }
        litePullConsumer.assign(assignList);
        litePullConsumer.seek(assignList.get(0), 10);
        try {
            while (true) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                // 开始消费消息
                System.out.printf("%s %n", messageExts);
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }


}
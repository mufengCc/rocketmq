/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 3000;
    /**
     * Flow control interval when message cache is full
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL = 50;
    /**
     * Flow control interval when broker return flow control
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL = 20;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
    private static final Logger log = LoggerFactory.getLogger(DefaultMQPushConsumerImpl.class);
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();
    private final long consumerStartTimestamp = System.currentTimeMillis();
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    private final RPCHook rpcHook;
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    private volatile boolean pause = false;
    private boolean consumeOrderly = false;
    private MessageListener messageListenerInner;
    private OffsetStore offsetStore;
    private ConsumeMessageService consumeMessageService;
    private ConsumeMessageService consumeMessagePopService;
    private long queueFlowControlTimes = 0;
    private long queueMaxSpanFlowControlTimes = 0;

    //10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
    private int[] popDelayLevel = new int[]{10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};

    private static final int MAX_POP_INVISIBLE_TIME = 300000;
    private static final int MIN_POP_INVISIBLE_TIME = 5000;
    private static final int ASYNC_TIMEOUT = 3000;

    // only for test purpose, will be modified by reflection in unit test.
    @SuppressWarnings("FieldMayBeFinal")
    private static boolean doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged = false;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("consumeMessageHook {} executeHookBefore exception", hook.hookName(), e);
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("consumeMessageHook {} executeHookAfter exception", hook.hookName(), e);
                }
            }
        }
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return parseSubscribeMessageQueues(result);
    }

    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
        Set<MessageQueue> resultQueues = new HashSet<>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public void pullMessage(final PullRequest pullRequest) {
        // 获取当前消息队列对应的处理队列
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        // 是否已经被移除。负载均衡时会将dropped修改为true
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }

        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        // 确保此consumer的服务状态正常，服务状态为RUNNING才为正常，如果服务状态不是RUNNING，那么抛出异常
        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            // 重新放入队列中，进行重试
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            return;
        }

        // 如果消费者暂停了，那么延迟1s后再发送拉取消息请求（默认是false）
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        // ------------------- 1.流控校验

        /*
         * 处理队列已缓存的消息总量（还未消费的）。在下面的PullRequest中的Found中，有针对该count的操作
         * 关联操作逻辑：
         *   1.当拉取到消息后，会将消息保存到processQueue中的msgTreeMap中，并记录大小msgCount
         *   2.当消息成功消费后，将消息从msgTreeMap中移除，并减少msgCount值
         *
         * 第一相关代码：在本函数中的PullCallback();中的processQueue.putMessage(pullResult.getMsgFoundList());实现。
         *              内部则是由ProcessQueue.putMessage();实现
         *
         * 第二步相关代码：由consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());触发，
         *                内部则是由ProcessQueue.removeMessage();实现
         */
        long cachedMessageCount = processQueue.getMsgCount().get();
        // 处理队列已缓存的消息总大小（还未消费的）
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

        // 判断是否出发流控？目的：流控主要是保护消费者。当消费者消费能力不够时，拉取速度太快会导致大量消息积压，很可能导致内存溢出。
        /*
         * 如果消息数量大于配置，则从消息数量进行流控
         * 1. 判断queue缓存的消息数量是否超过1000（可以根据pullThresholdForQueue参数配置）
         *    如果超过了1000，则先不去broker拉取消息，而是先暂停50ms，然后重新将对象放入队列(this.pullRequestQueue.put(pullRequest))，然后重新拉取
         */
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            // 延迟执行拉取消息请求
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                        "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        /*
         * 如果消息大小超过配置，则从消息大小进行流控
         * 2. 判断queue缓存的消息大小是否超过100M（可以根据pullThresholdSizeForQueue参数配置）
         *    如果超过100M，则先不去broker拉取消息，而是先暂停50ms，然后重新将对象放入队列中(this.pullRequestQueue.put(pullRequest))，然后重新拉取
         */
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            // 延迟执行拉取消息请求
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                        "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        /*
         * 3 顺序消费和并发消费的校验
         */
        if (!this.consumeOrderly) {
            // 如果内存中消息的offset的最大跨度大于设置的阈值，默认2000
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                // 延迟执行拉取消息请求
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                            "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                            pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        } else {
            // 顺序消费校验，如果已经锁定。注意该locked，是在RebalanceImpl.lock(final MessageQueue mq);中向broker申请锁成功后加锁。
            if (processQueue.isLocked()) {
                // 如果此前没有锁定过，那么需要设置消费点位(默认false）
                if (!pullRequest.isPreviouslyLocked()) {
                    long offset = -1L;
                    try {
                        // 计算获取该MessageQueue的下一个消息的消费偏移量offset
                        offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                        if (offset < 0) {
                            throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Unexpected offset " + offset);
                        }
                    } catch (Exception e) {
                        // 延迟执行拉取消息请求
                        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                        log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                        return;
                    }
                    // 第一次拉取消息，所以修复了来自broker的偏移量
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                            pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        // 第一次拉取消息，但拉取请求偏移量大于代理消耗偏移量
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                                pullRequest, offset);
                    }

                    // 设置previouslyLocked为true
                    pullRequest.setPreviouslyLocked(true);
                    // 重设消费点位
                    pullRequest.setNextOffset(offset);
                }
            } else {
                // 如果没有被锁住，那么延迟发送拉取消息请求
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }

        // 获取topic对应的SubscriptionData订阅关系
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            // 如果订阅信息为null，则表示发现消费者的订阅失败
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        /*
         * 4. 创建拉取消息的回调函数对象，当拉取消息的请求返回之后，将会指定回调函数
         * 拉取消息成功逻辑， 拉取成功/异常 逻辑处理
         */
        PullCallback pullCallback = new PullCallback() {

            /**
             * PullResult：拉取的消息结果。 内部保存了MessageExt信息
             */
            @Override
            public void onSuccess(PullResult pullResult) {
                log.info("拉取消息时间:{}", System.currentTimeMillis() - beginTimestamp);
                if (pullResult != null) {

                    // 拉取成功，开始处理从broker读取到的消息，并设置建议下一次拉取消息的broker地址
                    // 注意：此时List<MessageExt> msgFoundList为空
                    // 需将pullResult.getMessageBinary()字节数组转换成MessageExt消息，并根据TAG的值进行过滤
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                            subscriptionData);

                    switch (pullResult.getPullStatus()) {
                        // 发现了消息
                        case FOUND:
                            long prevRequestOffset = pullRequest.getNextOffset();
                            // 设置下次拉取消息的偏移量,之后会将该pullRequest再次丢入拉取消息队列中。就是根据这个offset去broker中拉取的
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullRT);

                            long firstMsgOffset = Long.MAX_VALUE;

                            // 已经找到消息了，为什么这里还判空？
                            // 因为这里进行了TAG/SQL92过滤
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                // 如果拉取到的消息为空，则再次投递，延迟拉取
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            } else {
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                        pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());

                                // 将从pullRequest获取到的消息，放入treeMap中。 key为offset偏移量，value是消息对象。当消费成功之后，会移除该数据
                                // 关联业务：移除代码如下：ConsumeMessageConcurrentlyService.processConsumeResult()
                                //   更新偏移量,将消息从ProcessQueue中移除，并向Broker汇报消息进度
                                //   long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs())
                                boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());

                                /**
                                 * 异步消费消息，消费成功之后，将消息从ProcessQueue中移除，并向Broker汇报消息进度
                                 *   构建consumeRequest，将消息提交到线程池中，由ConsumerMessageService 进行消费
                                 *   由于我们的是普通消息（不是顺序消息），所以由ConsumeMessageConcurrentlyService类来消费消息
                                 *   ConsumeMessageConcurrentlyService内部会创建一个线程池ThreadPoolExecutor，这个xcc非常重要，消息最终将提交到这个线程池中
                                 */
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                        pullResult.getMsgFoundList(),
                                        processQueue,
                                        pullRequest.getMessageQueue(),
                                        dispatchToConsume);

                                /*
                                 * 上面是异步消费，然后这里是将PullRequest放入队列中，这样take()方法将不会阻塞
                                 * 然后继续从broker中拉取消息，从而到达持续从broker中拉取消息
                                 * 延迟pullInterval 时间再去拉取消息：
                                 *      这里有一个 pullInterval参数，表示间隔多长时间在放入队列中（实际上就是间隔多长时间再去broker拉取消息)。当消费者消费速度比生产者快的时候，可以考虑设置这个值，这样可以避免大概率拉取到空消息。
                                 *
                                 */
                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                            DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    // 如果pullInterval间隔值为0，代表没有间隔，则再次立即拉取消息
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            if (pullResult.getNextBeginOffset() < prevRequestOffset
                                    || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                        pullResult.getNextBeginOffset(),
                                        firstMsgOffset,
                                        prevRequestOffset);
                            }

                            break;
                            // 没有找到消息，或被tag过滤，将再次将请求放入messageRequestQueue在，再次立即发起拉取消息请求
                        case NO_NEW_MSG:
                        case NO_MATCHED_MSG:
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            // 更新偏移量
                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);
                            // 再次拉取消息
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                            // 处理拉取请求中偏移量非法的情况，并进行相关的修复和记录
                        case OFFSET_ILLEGAL:
                            log.warn("the pull request offset illegal, {} {}",
                                    pullRequest.toString(), pullResult.toString());
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            pullRequest.getProcessQueue().setDropped(true);
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        // 更新偏移量
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                                pullRequest.getNextOffset(), false);

                                        // 持久化偏移量存储，向Broker发起请求
                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());

                                        //从负载均衡实现中移除pullRequest.getMessageQueue()对应的ProcessQueue。
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            }, 10000);
                            break;
                        default:
                            break;
                    }
                }
            }

            /**
             * 处理消息异常
             */
            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }

                if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.FLOW_CONTROL) {
                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL);
                } else {
                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                }
            }
        };

        /*
         * 5. 是否允许上报消费点位
         */
        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            // 从broker中拉取偏移量，并更新本地偏移量
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }

        int sysFlag = PullSysFlag.buildSysFlag(
                commitOffsetEnable, // commitOffset
                true, // suspend
                subExpression != null, // subscription
                classFilter // class filter
        );

        /*
         * 6. 真正的开始拉取消息，从broker服务端拉取消息
         */
        try {
            this.pullAPIWrapper.pullKernelImpl(
                    // 指定去那个queue去拉取消息
                    pullRequest.getMessageQueue(),
                    // 过滤表达式，就是tag/sql
                    subExpression,
                    // 过滤类型 TAG/SQL
                    subscriptionData.getExpressionType(),
                    subscriptionData.getSubVersion(),
                    // 拉取的偏移量
                    pullRequest.getNextOffset(),
                    // 一次最多拉取多少消息数量。默认为32条
                    this.defaultMQPushConsumer.getPullBatchSize(),
                    // 一次最多拉取多少消息大小。默认为256kb
                    this.defaultMQPushConsumer.getPullBatchSizeInBytes(),
                    sysFlag,
                    commitOffsetValue,
                    BROKER_SUSPEND_MAX_TIME_MILLIS,
                    CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                    // 异步拉取
                    CommunicationMode.ASYNC,
                    // broker响应后要执行的回调函数
                    pullCallback
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        }
    }

    void popMessage(final PopRequest popRequest) {
        final PopProcessQueue processQueue = popRequest.getPopProcessQueue();
        if (processQueue.isDropped()) {
            log.info("the pop request[{}] is dropped.", popRequest.toString());
            return;
        }

        processQueue.setLastPopTimestamp(System.currentTimeMillis());

        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
            return;
        }

        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        if (processQueue.getWaiAckMsgCount() > this.defaultMQPushConsumer.getPopThresholdForQueue()) {
            this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn("the messages waiting to ack exceeds the threshold {}, so do flow control, popRequest={}, flowControlTimes={}, wait count={}",
                        this.defaultMQPushConsumer.getPopThresholdForQueue(), popRequest, queueFlowControlTimes, processQueue.getWaiAckMsgCount());
            }
            return;
        }

        //POPTODO think of pop mode orderly implementation later.
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(popRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", popRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        PopCallback popCallback = new PopCallback() {
            @Override
            public void onSuccess(PopResult popResult) {
                if (popResult == null) {
                    log.error("pop callback popResult is null");
                    DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                    return;
                }

                processPopResult(popResult, subscriptionData);

                switch (popResult.getPopStatus()) {
                    case FOUND:
                        long pullRT = System.currentTimeMillis() - beginTimestamp;
                        DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(popRequest.getConsumerGroup(),
                                popRequest.getMessageQueue().getTopic(), pullRT);
                        if (popResult.getMsgFoundList() == null || popResult.getMsgFoundList().isEmpty()) {
                            DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                        } else {
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(popRequest.getConsumerGroup(),
                                    popRequest.getMessageQueue().getTopic(), popResult.getMsgFoundList().size());
                            popRequest.getPopProcessQueue().incFoundMsg(popResult.getMsgFoundList().size());

                            DefaultMQPushConsumerImpl.this.consumeMessagePopService.submitPopConsumeRequest(
                                    popResult.getMsgFoundList(),
                                    processQueue,
                                    popRequest.getMessageQueue());

                            if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest,
                                        DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                            } else {
                                DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                            }
                        }
                        break;
                    case NO_NEW_MSG:
                    case POLLING_NOT_FOUND:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                        break;
                    case POLLING_FULL:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
                        break;
                    default:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
                        break;
                }

            }

            @Override
            public void onException(Throwable e) {
                if (!popRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception: {}", e);
                }

                if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.FLOW_CONTROL) {
                    DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL);
                } else {
                    DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
                }
            }
        };


        try {

            long invisibleTime = this.defaultMQPushConsumer.getPopInvisibleTime();
            if (invisibleTime < MIN_POP_INVISIBLE_TIME || invisibleTime > MAX_POP_INVISIBLE_TIME) {
                invisibleTime = 60000;
            }
            this.pullAPIWrapper.popAsync(popRequest.getMessageQueue(), invisibleTime, this.defaultMQPushConsumer.getPopBatchNums(),
                    popRequest.getConsumerGroup(), BROKER_SUSPEND_MAX_TIME_MILLIS, popCallback, true, popRequest.getInitMode(),
                    false, subscriptionData.getExpressionType(), subscriptionData.getSubString());
        } catch (Exception e) {
            log.error("popAsync exception", e);
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
        }
    }

    private PopResult processPopResult(final PopResult popResult, final SubscriptionData subscriptionData) {
        if (PopStatus.FOUND == popResult.getPopStatus()) {
            List<MessageExt> msgFoundList = popResult.getMsgFoundList();
            List<MessageExt> msgListFilterAgain = msgFoundList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()
                    && popResult.getMsgFoundList().size() > 0) {
                msgListFilterAgain = new ArrayList<>(popResult.getMsgFoundList().size());
                for (MessageExt msg : popResult.getMsgFoundList()) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            if (!this.filterMessageHookList.isEmpty()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(this.defaultMQPushConsumer.isUnitMode());
                filterMessageContext.setMsgList(msgListFilterAgain);
                if (!this.filterMessageHookList.isEmpty()) {
                    for (FilterMessageHook hook : this.filterMessageHookList) {
                        try {
                            hook.filterMessage(filterMessageContext);
                        } catch (Throwable e) {
                            log.error("execute hook error. hookName={}", hook.hookName());
                        }
                    }
                }
            }

            if (msgFoundList.size() != msgListFilterAgain.size()) {
                for (MessageExt msg : msgFoundList) {
                    if (!msgListFilterAgain.contains(msg)) {
                        ackAsync(msg, this.groupName());
                    }
                }
            }

            popResult.setMsgFoundList(msgListFilterAgain);
        }

        return popResult;
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    public void setPause(boolean pause) {
        this.pause = pause;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    void executePopPullRequestLater(final PopRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePopPullRequestLater(pullRequest, timeDelay);
    }

    void executePopPullRequestImmediately(final PopRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePopPullRequestImmediately(pullRequest);
    }

    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
            InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    @Deprecated
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, brokerName, null);
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final MessageQueue mq)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, null, mq);
    }


    private void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, final MessageQueue mq)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        boolean needRetry = true;
        try {
            if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
                    || mq != null && mq.getBrokerName().startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
                needRetry = false;
                sendMessageBackAsNormalMessage(msg);
            } else {
                String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                        : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                // 发送消息消费失败请求
                this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, brokerName, msg,
                        this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
            }
        } catch (Throwable t) {
            log.error("Failed to send message back, consumerGroup={}, brokerName={}, mq={}, message={}",
                    this.defaultMQPushConsumer.getConsumerGroup(), brokerName, mq, msg, t);
            if (needRetry) {
                sendMessageBackAsNormalMessage(msg);
            }
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
        }
    }

    private void sendMessageBackAsNormalMessage(MessageExt msg) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

        String originMsgId = MessageAccessor.getOriginMessageId(msg);
        MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

        newMsg.setFlag(msg.getFlag());
        MessageAccessor.setProperties(newMsg, msg.getProperties());
        MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
        MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
        MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
        MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
        newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

        this.mQClientFactory.getDefaultMQProducer().send(newMsg);
    }

    void ackAsync(MessageExt message, String consumerGroup) {
        final String extraInfo = message.getProperty(MessageConst.PROPERTY_POP_CK);

        try {
            String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
            String brokerName = ExtraInfoUtil.getBrokerName(extraInfoStrs);
            int queueId = ExtraInfoUtil.getQueueId(extraInfoStrs);
            long queueOffset = ExtraInfoUtil.getQueueOffset(extraInfoStrs);
            String topic = message.getTopic();

            String desBrokerName = brokerName;
            if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
                desBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(this.defaultMQPushConsumer.queueWithNamespace(new MessageQueue(topic, brokerName, queueId)));
            }


            FindBrokerResult
                    findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
            if (null == findBrokerResult) {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
            }

            if (findBrokerResult == null) {
                log.error("The broker[" + desBrokerName + "] not exist");
                return;
            }

            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
            requestHeader.setQueueId(queueId);
            requestHeader.setOffset(queueOffset);
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setExtraInfo(extraInfo);
            requestHeader.setBname(brokerName);
            this.mQClientFactory.getMQClientAPIImpl().ackMessageAsync(findBrokerResult.getBrokerAddr(), ASYNC_TIMEOUT, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    if (ackResult != null && !AckStatus.OK.equals(ackResult.getStatus())) {
                        log.warn("Ack message fail. ackResult: {}, extraInfo: {}", ackResult, extraInfo);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    log.warn("Ack message fail. extraInfo: {}  error message: {}", extraInfo, e.toString());
                }
            }, requestHeader);

        } catch (Throwable t) {
            log.error("ack async error.", t);
        }
    }

    void changePopInvisibleTimeAsync(String topic, String consumerGroup, String extraInfo, long invisibleTime, AckCallback callback)
            throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        String brokerName = ExtraInfoUtil.getBrokerName(extraInfoStrs);
        int queueId = ExtraInfoUtil.getQueueId(extraInfoStrs);

        String desBrokerName = brokerName;
        if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
            desBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(this.defaultMQPushConsumer.queueWithNamespace(new MessageQueue(topic, brokerName, queueId)));
        }

        FindBrokerResult
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
        }
        if (findBrokerResult != null) {
            ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
            requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
            requestHeader.setQueueId(queueId);
            requestHeader.setOffset(ExtraInfoUtil.getQueueOffset(extraInfoStrs));
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setExtraInfo(extraInfo);
            requestHeader.setInvisibleTime(invisibleTime);
            requestHeader.setBname(brokerName);
            //here the broker should be polished
            this.mQClientFactory.getMQClientAPIImpl().changeInvisibleTimeAsync(brokerName, findBrokerResult.getBrokerAddr(), requestHeader, ASYNC_TIMEOUT, callback);
            return;
        }
        throw new MQClientException("The broker[" + desBrokerName + "] not exist", null);
    }

    public int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    public void shutdown() {
        shutdown(0);
    }

    public synchronized void shutdown(long awaitTerminateMillis) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.consumeMessageService.shutdown(awaitTerminateMillis);
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.destroy();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    /**
     * 消费者启动逻辑
     */
    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                        this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                this.serviceState = ServiceState.START_FAILED;

                // 检查基础配置信息，例如：消费者组是否为空、订阅关系是否为空，消息模式是否属于广播/订阅，等等一系列的校验
                this.checkConfig();

                // 将订阅关系复制到内部数据结构中，用于后续的消息拉取
                // rebalanceImpl 设置重试队列信息
                // this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData)
                this.copySubscription();

                // 为了在集群模式下确保消费者的 instanceName 不会与其他消费者实例冲突，以免在注册到 Broker 时出现重复的 consumerGroup。
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }

                // 创建MQClientInstance实例
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                // 设置消费者之间消息分配的策略算法。 默认是空接口
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                // 创建 PullAPIWrapper 实例，该实例用于封装拉取消息的操作，包括向 Broker 发送请求、处理响应等。
                if (this.pullAPIWrapper == null) {
                    this.pullAPIWrapper = new PullAPIWrapper(
                            mQClientFactory,
                            this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                }
                // 构建过滤消息钩子函数--当拉取到消息之后，执行消费者前执行。具体可参考：consumeRequest.run();
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                // 构建offset偏移量对象。 初始为空
                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING:
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            // 构建远程broker的offset请求对象。
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                }
                // 加载offset
                // 针对集群模式，为空实现
                // 针对广播模式，从本地读取
                this.offsetStore.load();

                // 构建顺序消息/普通消息  消费时的对象
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    this.consumeOrderly = true;
                    this.consumeMessageService =
                            new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                    //POPTODO reuse Executor ?
                    this.consumeMessagePopService = new ConsumeMessagePopOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    this.consumeOrderly = false;
                    // 创建并发消息处理对象，并创建线程池，默认线程数20
                    this.consumeMessageService =
                            new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                    //POPTODO reuse Executor ?
                    this.consumeMessagePopService =
                            new ConsumeMessagePopConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }

                // 启动定时任务，每15分钟执行一次，清除超时未消费的消息，
                this.consumeMessageService.start();
                // POPTODO
                this.consumeMessagePopService.start();

                // 注册consumerGroup消费者组，将当前对象加入到MQClientInstance中的consumerTable中
                // 用于负载均衡时，mqClientFactory.doRebalance();
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                /**
                 * 所有消费者当调用start();方法时，都会启动一次。注意：如果消费者则是全局公用一个
                 *
                 * 开启请求响应通道     mQClientAPIImpl.start()
                 * 启动各种计划任务     startScheduledTask()
                 * 启动拉动服务        pullMessageService.start()
                 * 启动负载平衡服务     rebalanceService.start()
                 * 启动推送服务        defaultMQProducer.getDefaultMQProducerImpl().start(false)
                 */
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }

        // 更新主题订阅信息
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();
        this.mQClientFactory.checkClientInBroker();
        // 发送心跳
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        // 负载均衡
        this.mQClientFactory.rebalanceImmediately();
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException(
                    "consumerGroup is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                    "consumerGroup can not equal "
                            + MixAll.DEFAULT_CONSUMER_GROUP
                            + ", please specify another one."
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException(
                    "messageModel is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException(
                    "consumeFromWhere is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException(
                    "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                            + this.defaultMQPushConsumer.getConsumeTimestamp()
                            + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException(
                    "subscription is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException(
                    "messageListener is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                    "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
                || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException(
                    "consumeThreadMin Out of range [1, 1000]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException(
                    "consumeThreadMax Out of range [1, 1000]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMin can't be larger than consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                    "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                            + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                    null);
        }

        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
                || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException(
                    "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                    "pullThresholdForQueue Out of range [1, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullThresholdForTopic
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException(
                        "pullThresholdForTopic Out of range [1, 6553500]"
                                + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                        null);
            }
        }

        // pullThresholdSizeForQueue
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException(
                    "pullThresholdSizeForQueue Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException(
                        "pullThresholdSizeForTopic Out of range [1, 102400]"
                                + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                        null);
            }
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                    "pullInterval Out of range [0, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
                || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                    "consumeMessageBatchMaxSize Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                    "pullBatchSize Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // popInvisibleTime
        if (this.defaultMQPushConsumer.getPopInvisibleTime() < MIN_POP_INVISIBLE_TIME
                || this.defaultMQPushConsumer.getPopInvisibleTime() > MAX_POP_INVISIBLE_TIME) {
            throw new MQClientException(
                    "popInvisibleTime Out of range [" + MIN_POP_INVISIBLE_TIME + ", " + MAX_POP_INVISIBLE_TIME + "]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // popBatchNums
        if (this.defaultMQPushConsumer.getPopBatchNums() <= 0 || this.defaultMQPushConsumer.getPopBatchNums() > 32) {
            throw new MQClientException(
                    "popBatchNums Out of range [1, 32]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
    }

    private void copySubscription() throws MQClientException {
        try {
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }

            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        if (doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged) {
            return;
        }
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, SubscriptionData.SUB_ALL);
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }

            SubscriptionData subscriptionData = FilterAPI.build(topic,
                    messageSelector.getExpression(), messageSelector.getExpressionType());

            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    public MessageExt viewMessage(String msgId)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    public void resetOffsetByTimeStamp(long timeStamp) throws MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            if (CollectionUtils.isNotEmpty(mqs)) {
                Map<MessageQueue, Long> offsetTable = new HashMap<>(mqs.size(), 1);
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        return new HashSet<>(this.rebalanceImpl.getSubscriptionInner().values());
    }

    @Override
    public void doRebalance() {
        if (!this.pause) {
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        Iterator<Entry<MessageQueue, PopProcessQueue>> popIt = this.rebalanceImpl.getPopProcessQueueTable().entrySet().iterator();
        while (popIt.hasNext()) {
            Entry<MessageQueue, PopProcessQueue> next = popIt.next();
            MessageQueue mq = next.getKey();
            PopProcessQueue pq = next.getValue();

            PopProcessQueueInfo pqinfo = new PopProcessQueueInfo();
            pq.fillPopProcessQueueInfo(pqinfo);
            info.getMqPopTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public synchronized void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    public void tryResetPopRetryTopic(final List<MessageExt> msgs, String consumerGroup) {
        String popRetryPrefix = MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup + "_";
        for (MessageExt msg : msgs) {
            if (msg.getTopic().startsWith(popRetryPrefix)) {
                String normalTopic = KeyBuilder.parseNormalTopic(msg.getTopic(), consumerGroup);
                if (normalTopic != null && !normalTopic.isEmpty()) {
                    msg.setTopic(normalTopic);
                }
            }
        }
    }


    public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }

            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    int[] getPopDelayLevel() {
        return popDelayLevel;
    }
}

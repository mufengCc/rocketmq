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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class RebalancePushImpl extends RebalanceImpl {
    private final static long UNLOCK_DELAY_TIME_MILLS = Long.parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;


    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        /**
         * When rebalance result changed, should update subscription's version to notify broker.
         * Fix: inconsistency subscription may lead to consumer miss messages.
         */
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        long newVersion = System.currentTimeMillis();
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);

        int currentQueueCount = this.processQueueTable.size();
        if (currentQueueCount != 0) {
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            if (pullThresholdForTopic != -1) {
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }

            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForTopic();
            if (pullThresholdSizeForTopic != -1) {
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        // notify broker
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithLockV2(true);
    }

    /**
     * 负载均衡时，移除不再需要的处理队列
     *
     * @param mq    消息队列
     * @param pq    处理队列
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        // 将指定的消息队列（mq）的消费进度（Offset）持久化到存储中，以确保消息的消费进度在 Consumer 重启后仍然可用
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        // 从 Offset 存储中移除指定消息队列的消费进度信息。这通常是在不再需要消费该消息队列时执行的操作，以清除该队列的 Offset 信息
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
        // 如果是顺序消费且是集群消费模式
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
            && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                // 首先尝试获取处理队列对应的消费锁，这个是顺序消费时的第三把锁。这是为了确保只有一个线程在处理该消息队列的消息，以维护顺序性
                if (pq.getConsumeLock().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {
                        // 向 Broker 发起解锁请求
                        return this.unlockDelay(mq, pq);
                    } finally {
                        pq.getConsumeLock().unlock();
                    }
                } else {
                    log.warn("[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",
                        mq,
                        pq.getTryUnlockTimes());

                    pq.incTryUnlockTimes();
                }
            } catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }

    @Override
    public boolean clientRebalance(String topic) {
        // POPTODO order pop consume not implement yet
        return defaultMQPushConsumerImpl.getDefaultMQPushConsumer().isClientRebalance() || defaultMQPushConsumerImpl.isConsumeOrderly() || MessageModel.BROADCASTING.equals(messageModel);
    }

    public boolean removeUnnecessaryPopMessageQueue(final MessageQueue mq, final PopProcessQueue pq) {
        return true;
    }

    private boolean unlockDelay(final MessageQueue mq, final ProcessQueue pq) {

        if (pq.hasTempMessage()) {
            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(new Runnable() {
                @Override
                public void run() {
                    log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);
                    RebalancePushImpl.this.unlock(mq, true);
                }
            }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
        } else {
            this.unlock(mq, true);
        }
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Deprecated
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1L;
        try {
            result = computePullFromWhereWithException(mq);
        } catch (MQClientException e) {
            log.warn("Compute consume offset exception, mq={}", mq);
        }
        return result;
    }

    @Override
    public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
        long result = -1;
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
            case CONSUME_FROM_LAST_OFFSET: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                }
                // First start,no offset
                else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    } else {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query consume offset from " +
                            "offset store");
                }
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    //the offset will be fixed by the OFFSET_ILLEGAL process
                    result = 0L;
                } else {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query offset from offset " +
                            "store");
                }
                break;
            }
            case CONSUME_FROM_TIMESTAMP: {
                long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    } else {
                        try {
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                UtilAll.YYYYMMDDHHMMSS).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    throw new MQClientException(ResponseCode.QUERY_NOT_FOUND, "Failed to query offset from offset " +
                            "store");
                }
                break;
            }

            default:
                break;
        }

        if (result < 0) {
            throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Found unexpected result " + result);
        }

        return result;
    }

    @Override
    public int getConsumeInitMode() {
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        if (ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET == consumeFromWhere) {
            return ConsumeInitMode.MIN;
        } else {
            return ConsumeInitMode.MAX;
        }
    }

    /**
     * 将PullRequest加入PullMessageService,以便唤醒PullMessageService线程
     *
     * @param pullRequestList   拉取请求
     * @param delay 延迟等级
     */
    @Override
    public void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay) {
        for (PullRequest pullRequest : pullRequestList) {
            if (delay <= 0) {
                this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            } else {
                // 将拉取消息请求-放入队列
                this.defaultMQPushConsumerImpl.executePullRequestLater(pullRequest, delay);
            }
        }
    }

    @Override
    public void dispatchPopPullRequest(final List<PopRequest> pullRequestList, final long delay) {
        for (PopRequest pullRequest : pullRequestList) {
            if (delay <= 0) {
                this.defaultMQPushConsumerImpl.executePopPullRequestImmediately(pullRequest);
            } else {
                this.defaultMQPushConsumerImpl.executePopPullRequestLater(pullRequest, delay);
            }
        }
    }

    @Override
    public ProcessQueue createProcessQueue() {
        return new ProcessQueue();
    }

    @Override
    public ProcessQueue createProcessQueue(String topicName) {
        return createProcessQueue();
    }

    @Override
    public PopProcessQueue createPopProcessQueue() {
        return new PopProcessQueue();
    }
}

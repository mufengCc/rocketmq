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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Queue consumption snapshot
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
        Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final Logger log = LoggerFactory.getLogger(ProcessQueue.class);

    /**
     * 用于保护 msgTreeMap 的读写操作。当往msgTreeMap中put数据时，需要拿到该锁
     * msgTreeMap 存储了拉取到的消息，以便后续的消费
     */
    private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock();

    /**
     * 存储拉取到的消息。当消息成功消费后再进行移除
     * 存储拉取到的消息，按照消息的 Offset 排序，以便按照消息 Offset 顺序进行消费。这个数据结构被设计为一个树形映射，以提供高效的消息查找和处理
     */
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<>();

    /**
     * 用于跟踪 msgTreeMap 中消息的数量，以便进行流控等操作
     * 最大存储1000条数据
     */
    private final AtomicLong msgCount = new AtomicLong();

    /**
     * 用于跟踪 msgTreeMap 中消息的总大小，通常以字节为单位，也用于流控等操作。
     * 最大256kb
     */
    private final AtomicLong msgSize = new AtomicLong();

    /**
     * 用于保护 msgTreeMap 和 msgCount 的并发操作，以确保多个消费者线程之间的安全性
     * 用于顺序消费时的第三把锁。先获取该锁才能进行消费，否则会很大概率出现重复消息问题。
     *  参考代码：ConsumeMessageOrderlyService.ConsumeRequest.processQueue.getConsumeLock().lock();
     */
    private final Lock consumeLock = new ReentrantLock();
    /**
     * 仅在顺序消费模式（consumeOrderly 为 true）下使用，存储已被顺序消费的消息。这个树形映射用于支持顺序消费，以确保消息按照 Offset 的顺序被消费
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<>();

    /**
     * 用于记录尝试解锁的次数，通常在消息的消费过程中用于解锁消费队列
     */
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);

    /**
     * 标识当前队列的最大 Offset 值，用于跟踪消费进度
     */
    private volatile long queueOffsetMax = 0L;

    /**
     * 标识队列是否被丢弃。在负载均衡时，队列可能会被暂时丢弃以确保消息不被重复消费
     *  在负载均衡时，当出现新的消费者客户端时，会将当前监听的队列，分配到新的客户端，此时messQueue对应的processQueue的dropped=true。
     */
    private volatile boolean dropped = false;

    /**
     * 记录上一次消息拉取的时间戳，用于检测是否需要再次拉取消息
     */
    private volatile long lastPullTimestamp = System.currentTimeMillis();

    /**
     * 记录上一次消息消费的时间戳，用于检测是否需要再次消费消息
     */
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    /**
     * 标识队列是否被锁定，通常在顺序消费时会被锁定以确保只有一个消费者消费
     *  用于顺序消息的第一把锁。在负载均衡时，向broker申请锁成功之后，会修改true。
     *   当重新分配到新的客户端时，会修改false
     */
    private volatile boolean locked = false;

    /**
     * 记录上一次锁定队列的时间戳，用于检测是否需要解锁队列
     */
    private volatile long lastLockTimestamp = System.currentTimeMillis();

    /**
     * 标识队列是否正在被消费，通常在负载均衡过程中用于避免多个消费者同时消费一个队列
     *  当执行PullCallback中的onSuccess中的found找到消息时，会往该对象的msgTreeMap新增数据，此时会设置为true
     */
    private volatile boolean consuming = false;

    /**
     * 记录队列上消息的累积消费数量，通常用于统计和监控。在往该对象的msgTreeMap新增数据，此时msgAccCnt会自动加1
     */
    private volatile long msgAccCnt = 0;

    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 清理过期消息
     *   处理拉取到但长时间未被消费的消息，从而将这些消息返回给 Broker
     * 当消费在启动时，会启动定时任务，每间隔15分钟执行一次。
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {

        //如果当前消费者是有序消费者则直接返回，因为有序消费者不需要执行此清理操作
        if (pushConsumer.isConsumeOrderly()) {
            return;
        }

        // 计算要处理的循环次数，为了限制每次清理的消息数量，以避免一次性清理大量消息，影响性能
        int loop = Math.min(msgTreeMap.size(), 16);
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                // 获取读锁，防止在清理时，有新增数据的动作
                this.treeMapLock.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty()) {
                        // 获取第一条消息的消费开始时间戳
                        String consumeStartTimeStamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
                        // 当前时间距离第一条消息的消费时间超过了15分钟，就认为这条消息已经过期，需要进行清理
                        if (StringUtils.isNotEmpty(consumeStartTimeStamp) && System.currentTimeMillis() - Long.parseLong(consumeStartTimeStamp) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                            // 获取要清理的消息
                            msg = msgTreeMap.firstEntry().getValue();
                        }
                    }
                } finally {
                    // 释放锁
                    this.treeMapLock.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            if (msg == null) {
                break;
            }

            try {

                // 针对要清理的过期数据，不能直接删除，而是将延迟等级设置为3（延迟10s），放入延迟队列中，之后再次消费
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    // 再次获取读锁
                    this.treeMapLock.writeLock().lockInterruptibly();
                    try {
                        // 开始从msgTreeMap中移除数据
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        // 释放锁
                        this.treeMapLock.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 当拉取到消息后，往msgTreeMap中新增数据
     *
     * @param msgs  拉取到的消息
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            // 获取写锁，确保对 msgTreeMap 的写操作是互斥的，以防止多线程并发访问时的数据不一致性
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;

                // 新增数据
                for (MessageExt msg : msgs) {
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {
                        validMsgCnt++;
                        // 更新最大偏移量
                        this.queueOffsetMax = msg.getQueueOffset();
                        // 更新队列中消息的总大小
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }

                // 记录队列中消息的数量
                msgCount.addAndGet(validMsgCnt);

                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true;
                    // 设置当前processQueue正在消费中
                    this.consuming = true;
                }

                /**
                 * 如果消息列表 msgs 不为空，还会尝试从消息中获取 MessageConst.PROPERTY_MAX_OFFSET 属性。
                 *  该属性用于跟踪最大的消息偏移量。如果该属性存在，将计算当前消息的累积消息数，即在队列中已经处理的消息数。最后，将 msgAccCnt 更新为累积消息数
                 */
                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    // 获取本次拉取最大的offset
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                // 最后解锁
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    public long getMaxSpan() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 当某个队列中的消息成功消费后，获取该队列最小offset偏移量
     *
     * @param msgs  消费的消息
     * @return      最小的offset偏移量
     */
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    result = this.queueOffsetMax + 1;
                    int removedCnt = 0;
                    for (MessageExt msg : msgs) {
                        // 从msgTreeMap中移除当前消息
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            removedCnt--;
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    msgCount.addAndGet(removedCnt);

                    if (!msgTreeMap.isEmpty()) {
                        // 获取未被消费的首位偏移量，这里可能会出现重复消费问题
                        // 也就是最小位点提交机制获取还未被消费的offset
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    /**
     * 顺序消费时，如果是手动提交，且状态是rollback时，则将当前消息再次消费。
     */
    public void rollback() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 顺序消息消费成功后要执行的逻辑
     * 清空已经成功消费的有序消息，更新消息计数和大小，返回下一次应该拉取的消息偏移量，以维护消息的有序消费进度
     * 1.减少存储数据的总数
     * 2.减少存储数据的总大小
     * 3.清除已被顺序消息的集合
     *
     */
    public long commit() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                this.consumingMsgOrderlyTreeMap.clear();
                // 更新消费者的进度，确保下一次消费从正确的位置开始
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 将一组消息重新标记为等待消费状态。这通常用于有序消息消费，以处理在某些情况下需要重新排队的消息。
     *  重新排队后，这些消息将会被再次提供给消费者进行处理
     * 当是顺序消费，且是手动提交，提交的状态为 SUSPEND_CURRENT_QUEUE_A_MOMENT时，则需要重新消费
     */
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    /**
     * 获取消息，用于顺序消费
     *
     * @param batchSize 消息数量，默认为1
     */
    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    /**
     * Return the result that whether current message is exist in the process queue or not.
     */
    public boolean containsMessage(MessageExt message) {
        if (message == null) {
            // should never reach here.
            return false;
        }
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                return this.msgTreeMap.containsKey(message.getQueueOffset());
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (Throwable t) {
            log.error("Failed to check message's existence in process queue, message={}", message, t);
        }
        return false;
    }

    public boolean hasTempMessage() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getConsumeLock() {
        return consumeLock;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.treeMapLock.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.treeMapLock.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}

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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.common.Attributes;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.DBMsgConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected static final Logger DLQ_LOG = LoggerFactory.getLogger(LoggerName.DLQ_LOGGER_NAME);

    protected List<ConsumeMessageHook> consumeMessageHookList;

    protected final static int DLQ_NUMS_PER_GROUP = 1;
    protected final BrokerController brokerController;
    protected final Random random = new Random(System.currentTimeMillis());
    private List<SendMessageHook> sendMessageHookList;

    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    /**
     * 处理了消费者发送回消息的逻辑，允许消费者将某些消息重新发送到消息队列，以便重新消费或处理消费失败的情况。
     * 这有助于处理消息消费过程中的问题，确保消息最终能够被成功消费。
     *
     * 也是创建重试队列和死信独立的入口
     */
    protected RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        // 解析消费者发送回消息的相关信息，其中包含了发送回消息所需的信息，例如消费者组名、消息的原始主题等
        final ConsumerSendMsgBackRequestHeader requestHeader =
            (ConsumerSendMsgBackRequestHeader) request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);

        // The send back requests sent to SlaveBroker will be forwarded to the master broker beside
        // 检查主从模式：检查当前 Broker 是否为主 Broker。如果不是主 Broker，表示当前 Broker 是从 Broker，
        //  需要将请求发送到主 Broker 来处理发送回消息的操作。这是为了确保操作在主 Broker 上执行，以保证数据一致性
        final BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (null == masterBroker) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("no master available along with " + brokerController.getBrokerConfig().getBrokerIP1());
            return response;
        }

        // The broker that received the request.
        // It may be a master broker or a slave broker
        final BrokerController currentBroker = this.brokerController;

        // 获取订阅组配置：从主 Broker 的订阅组管理器获取消费者组的订阅配置信息。如果配置不存在，说明该消费者组没有正确配置，返回相应的错误响应
        SubscriptionGroupConfig subscriptionGroupConfig =
            masterBroker.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return response;
        }

        BrokerConfig masterBrokerConfig = masterBroker.getBrokerConfig();
        // 校验权限：检查主 Broker 是否具有写权限（PermName.isWriteable），如果没有写权限，则返回无权限的错误响应
        if (!PermName.isWriteable(masterBrokerConfig.getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + masterBrokerConfig.getBrokerIP1() + "] sending message is forbidden");
            return response;
        }

        // 检查重试队列：检查是否支持重试队列。如果重试队列数量小于等于0，表示不支持重试队列，直接返回成功的响应
        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        // 构建重试队列 Topic：根据消费组名和随机数生成一个新的 Topic 名称，这个 Topic 名称用于存储重试队列的消息。如果消息重试失败，会将消息发送到这个 Topic
        // 重试队列topic：例如："%RETRY%"topic_consumer_group
        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        // 随机选择一个数字
        int queueIdInt = this.random.nextInt(subscriptionGroupConfig.getRetryQueueNums());

        int topicSysFlag = 0;
        if (requestHeader.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        // 创建 Topic 配置：创建或获取重试队列 Topic 的配置，包括写权限和读权限。如果 Topic 配置不存在，返回相应的错误响应
        // 创建重试队列，重试主题名称为%RETRY%+消费者组名称
        TopicConfig topicConfig = masterBroker.getTopicConfigManager().createTopicInSendMessageBackMethod(
            newTopic,
            subscriptionGroupConfig.getRetryQueueNums(),
            PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);

        // 创建失败，则响应系统异常
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return response;
        }

        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return response;
        }

        // 根据消息offset，从commitLog中获取消息
        MessageExt msgExt = currentBroker.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return response;
        }

        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        msgExt.setWaitStoreMsgOK(false);

        // 延迟等级
        int delayLevel = requestHeader.getDelayLevel();

        // 设置最大重试次数
        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            Integer times = requestHeader.getMaxReconsumeTimes();
            if (times != null) {
                maxReconsumeTimes = times;
            }
        }

        // 是否为死信队列
        boolean isDLQ = false;

        // 设置是否为死信队列：如果消息的重试次数达到最大重试次数或延迟级别小于0，则将消息标记为死信队列，准备发送到死信队列
        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes
            || delayLevel < 0) {

            Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                .put(LABEL_CONSUMER_GROUP, requestHeader.getGroup())
                .put(LABEL_TOPIC, requestHeader.getOriginTopic())
                .put(LABEL_IS_SYSTEM, BrokerMetricsManager.isSystem(requestHeader.getOriginTopic(), requestHeader.getGroup()))
                .build();
            BrokerMetricsManager.sendToDlqMessages.add(1, attributes);

            // 设置死信队列标志
            isDLQ = true;
            // 设置死信队列topic，此时会将刚刚的重试队列名称修改为死信队列的名称
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            queueIdInt = randomQueueId(DLQ_NUMS_PER_GROUP);

            // Create DLQ topic to master broker
            // 创建死信队列
            topicConfig = masterBroker.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                DLQ_NUMS_PER_GROUP,
                PermName.PERM_WRITE | PermName.PERM_READ, 0);

            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return response;
            }
            msgExt.setDelayTimeLevel(0);
        } else {
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }

            msgExt.setDelayTimeLevel(delayLevel);
        }

        // 创建新消息：创建一个新的消息对象（msgInner），这个消息包含了要发送回的内容，其中 Topic 可能是重试队列或者死信队列
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        // 这个newTopic：可能是死信队列、也可能是重试队列
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        boolean succeeded = false;

        // Put retry topic to master message store
        // 尝试将消息写入 Commit Log：调用主 Broker 的消息存储模块，将构建的重试消息写入 Commit Log 文件
        PutMessageResult putMessageResult = masterBroker.getMessageStore().putMessage(msgInner);

        // 处理响应：如果消息成功写入 Commit Log，则返回成功的响应，否则返回系统错误响应
        if (putMessageResult != null) {
            String commercialOwner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);

            switch (putMessageResult.getPutMessageStatus()) {
                case PUT_OK:
                    String backTopic = msgExt.getTopic();
                    String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                    if (correctTopic != null) {
                        backTopic = correctTopic;
                    }

                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msgInner.getTopic())) {
                        masterBroker.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
                        masterBroker.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
                        masterBroker.getBrokerStatsManager().incQueuePutNums(msgInner.getTopic(), msgInner.getQueueId());
                        masterBroker.getBrokerStatsManager().incQueuePutSize(msgInner.getTopic(), msgInner.getQueueId(), putMessageResult.getAppendMessageResult().getWroteBytes());
                    }
                    masterBroker.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);

                    if (isDLQ) {
                        masterBroker.getBrokerStatsManager().incDLQStatValue(
                            BrokerStatsManager.SNDBCK2DLQ_TIMES,
                            commercialOwner,
                            requestHeader.getGroup(),
                            requestHeader.getOriginTopic(),
                            BrokerStatsManager.StatsType.SEND_BACK_TO_DLQ.name(),
                            1);

                        String uniqKey = msgInner.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                        DLQ_LOG.info("send msg to DLQ {}, owner={}, originalTopic={}, consumerId={}, msgUniqKey={}, storeTimestamp={}",
                            newTopic,
                            commercialOwner,
                            requestHeader.getOriginTopic(),
                            requestHeader.getGroup(),
                            uniqKey,
                            putMessageResult.getAppendMessageResult().getStoreTimestamp());
                    }

                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);

                    succeeded = true;
                    break;
                default:
                    break;
            }

            if (!succeeded) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(putMessageResult.getPutMessageStatus().name());
            }
        } else {
            // 消息写入失败，响应系统异常。调用者会将延迟5s再次消费该消息。
            if (isDLQ) {
                String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
                String uniqKey = msgInner.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                DLQ_LOG.info("failed to send msg to DLQ {}, owner={}, originalTopic={}, consumerId={}, msgUniqKey={}, result={}",
                    newTopic,
                    owner,
                    requestHeader.getOriginTopic(),
                    requestHeader.getGroup(),
                    uniqKey,
                    "null");
            }

            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("putMessageResult is null");
        }

        // 执行钩子函数
        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
            String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getGroup());
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setNamespace(namespace);
            context.setTopic(requestHeader.getOriginTopic());
            context.setConsumerGroup(requestHeader.getGroup());
            context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
            context.setCommercialRcvTimes(1);
            context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));

            context.setAccountAuthType(request.getExtFields().get(BrokerStatsManager.ACCOUNT_AUTH_TYPE));
            context.setAccountOwnerParent(request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_PARENT));
            context.setAccountOwnerSelf(request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_SELF));
            context.setRcvStat(isDLQ ? BrokerStatsManager.StatsType.SEND_BACK_TO_DLQ : BrokerStatsManager.StatsType.SEND_BACK);
            context.setSuccess(succeeded);
            context.setRcvMsgNum(1);
            //Set msg body size 0 when sent back by consumer.
            context.setRcvMsgSize(0);
            context.setCommercialRcvMsgNum(succeeded ? 1 : 0);

            try {
                this.executeConsumeMessageHookAfter(context);
            } catch (AbortProcessException e) {
                response.setCode(e.getResponseCode());
                response.setRemark(e.getErrorMessage());
            }
        }

        return response;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx,
        SendMessageRequestHeader requestHeader, RemotingCommand request) {
        String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic());

        SendMessageContext sendMessageContext;
        sendMessageContext = new SendMessageContext();
        sendMessageContext.setNamespace(namespace);
        sendMessageContext.setProducerGroup(requestHeader.getProducerGroup());
        sendMessageContext.setTopic(requestHeader.getTopic());
        sendMessageContext.setBodyLength(request.getBody().length);
        sendMessageContext.setMsgProps(requestHeader.getProperties());
        sendMessageContext.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        sendMessageContext.setBrokerAddr(this.brokerController.getBrokerAddr());
        sendMessageContext.setQueueId(requestHeader.getQueueId());
        sendMessageContext.setBrokerRegionId(this.brokerController.getBrokerConfig().getRegionId());
        sendMessageContext.setBornTimeStamp(requestHeader.getBornTimestamp());
        sendMessageContext.setRequestTimeStamp(System.currentTimeMillis());

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        sendMessageContext.setCommercialOwner(owner);

        Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String uniqueKey = properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        properties.put(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        properties.put(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));
        requestHeader.setProperties(MessageDecoder.messageProperties2String(properties));

        if (uniqueKey == null) {
            uniqueKey = "";
        }
        sendMessageContext.setMsgUniqueKey(uniqueKey);

        if (properties.containsKey(MessageConst.PROPERTY_SHARDING_KEY)) {
            sendMessageContext.setMsgType(MessageType.Order_Msg);
        } else {
            sendMessageContext.setMsgType(MessageType.Normal_Msg);
        }
        return sendMessageContext;
    }

    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }

    protected MessageExtBrokerInner buildInnerMsg(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, final byte[] body, TopicConfig topicConfig) {
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }
        int sysFlag = requestHeader.getSysFlag();

        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner,
            MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
            msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader
            .getReconsumeTimes());
        return msgInner;
    }

    public SocketAddress getStoreHost() {
        return brokerController.getStoreHost();
    }

    protected RemotingCommand msgContentCheck(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, RemotingCommand request,
        final RemotingCommand response) {
        String topic = requestHeader.getTopic();
        if (topic.length() > Byte.MAX_VALUE) {
            LOGGER.warn("msgContentCheck: message topic length is too long, topic={}, topic length={}, threshold={}",
                topic, topic.length(), Byte.MAX_VALUE);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (requestHeader.getProperties() != null && requestHeader.getProperties().length() > Short.MAX_VALUE) {
            LOGGER.warn(
                "msgContentCheck: message properties length is too long, topic={}, properties length={}, threshold={}",
                topic, requestHeader.getProperties().length(), Short.MAX_VALUE);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (request.getBody().length > DBMsgConstants.MAX_BODY_SIZE) {
            LOGGER.warn(
                "msgContentCheck: message body size exceeds the threshold, topic={}, body size={}, threshold={}bytes",
                topic, request.getBody().length, DBMsgConstants.MAX_BODY_SIZE);
            response.setRemark("msg body must be less 64KB");
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        return response;
    }

    protected RemotingCommand msgCheck(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, final RemotingCommand request,
        final RemotingCommand response) {
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
            && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                + "] sending message is forbidden");
            return response;
        }

        TopicValidator.ValidateTopicResult result = TopicValidator.validateTopic(requestHeader.getTopic());
        if (!result.isValid()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(result.getRemark());
            return response;
        }
        if (TopicValidator.isNotAllowedSendTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("Sending message to topic[" + requestHeader.getTopic() + "] is forbidden.");
            return response;
        }

        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                } else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            LOGGER.warn("the topic {} not exist, producer: {}", requestHeader.getTopic(), ctx.channel().remoteAddress());
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(
                requestHeader.getTopic(),
                requestHeader.getDefaultTopic(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                requestHeader.getDefaultTopicQueueNums(), topicSysFlag);

            if (null == topicConfig) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig =
                        this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                            requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ,
                            topicSysFlag);
                }
            }

            if (null == topicConfig) {
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                    + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return response;
            }
        }

        int queueIdInt = requestHeader.getQueueId();
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        if (queueIdInt >= idValid) {
            String errorInfo = String.format("request queueId[%d] is illegal, %s Producer: %s",
                queueIdInt,
                topicConfig,
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

            LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);

            return response;
        }
        return response;
    }

    public void registerSendMessageHook(List<SendMessageHook> sendMessageHookList) {
        this.sendMessageHookList = sendMessageHookList;
    }

    protected void doResponse(ChannelHandlerContext ctx, RemotingCommand request,
        final RemotingCommand response) {
        NettyRemotingAbstract.writeResponse(ctx.channel(), request, response);
    }

    public void executeSendMessageHookBefore(SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Throwable e) {
                    //ignore
                }
            }
        }
    }

    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        return SendMessageRequestHeader.parseRequestHeader(request);
    }

    protected int randomQueueId(int writeQueueNums) {
        return ThreadLocalRandom.current().nextInt(99999999) % writeQueueNums;
    }

    public void executeSendMessageHookAfter(final RemotingCommand response, final SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    if (response != null) {
                        final SendMessageResponseHeader responseHeader =
                            (SendMessageResponseHeader) response.readCustomHeader();
                        context.setMsgId(responseHeader.getMsgId());
                        context.setQueueId(responseHeader.getQueueId());
                        context.setQueueOffset(responseHeader.getQueueOffset());
                        context.setCode(response.getCode());
                        context.setErrorMsg(response.getRemark());
                    }
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    //ignore
                }
            }
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}

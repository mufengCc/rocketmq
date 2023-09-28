package org.apache.rocketmq.client.hook;

import org.apache.rocketmq.common.message.MessageExt;

public class MyFilterMessageHook implements FilterMessageHook {
    @Override
    public String hookName() {
        return "myFilterMessageHook";
    }

    @Override
    public void filterMessage(FilterMessageContext context) {
        System.out.println("myFilterMessageHook " + context);

        for (MessageExt messageExt : context.getMsgList()) {
            String keys = messageExt.getProperty("KEYS");
            System.out.println(keys);
        }
    }
}

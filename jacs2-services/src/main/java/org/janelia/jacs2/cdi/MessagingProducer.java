package org.janelia.jacs2.cdi;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.messaging.core.ConnectionManager;
import org.janelia.messaging.core.MessageConnection;
import org.janelia.messaging.core.MessageSender;
import org.janelia.messaging.core.impl.MessageSenderImpl;
import org.janelia.model.access.cdi.AsyncIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class MessagingProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MessagingProducer.class);

    @ApplicationScoped
    @Produces
    public MessageConnection createIndexingMessageConnection(@PropertyValue(name = "Messaging.Server") String messagingServer,
                                                             @PropertyValue(name = "Messaging.User") String messagingUser,
                                                             @PropertyValue(name = "Messaging.Password") String messagingPassword) {
        if (StringUtils.isNotBlank(messagingServer)) {
            LOG.info("Create messaging connection to {} as {}", messagingServer, messagingUser);
            return ConnectionManager.getInstance().getConnection(messagingServer, messagingUser, messagingPassword, 1);
        } else {
            return ConnectionManager.getInstance().getConnection();
        }
    }

    @AsyncIndex
    @ApplicationScoped
    @Produces
    public MessageSender createIndexingMessageSender(MessageConnection messageConnection,
                                                     @PropertyValue(name = "Messaging.AsyncIndexingExchange") String asyncIndexingExchange,
                                                     @PropertyValue(name = "Messaging.AsyncIndexingRoutingKey") String asyncIndexingRoutingKey) {
        MessageSender messageSender = new MessageSenderImpl(messageConnection);
        if (StringUtils.isNotBlank(asyncIndexingExchange)) {
            LOG.info("Create message sender connected to {}:{}", asyncIndexingExchange, asyncIndexingRoutingKey);
            messageSender.connectTo(asyncIndexingExchange, asyncIndexingRoutingKey);
        } else {
            LOG.warn("Created an unconnected sender since no message exchange was provided");
        }
        return messageSender;
    }

}

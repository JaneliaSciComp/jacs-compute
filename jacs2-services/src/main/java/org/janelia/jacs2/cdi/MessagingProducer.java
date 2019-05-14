package org.janelia.jacs2.cdi;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.messaging.core.MessageSender;
import org.janelia.messaging.core.impl.MessageConnection;
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
    @AsyncIndex
    @Produces
    public MessageConnection createIndexingMessageConnection(@PropertyValue(name = "Messaging.Server") String messagingServer,
                                                             @PropertyValue(name = "Messaging.User") String messagingUser,
                                                             @PropertyValue(name = "Messaging.Password") String messagingPassword) {
        LOG.info("Create messaging connection to {} as {}", messagingServer, messagingUser);
        if (StringUtils.isNotBlank(messagingServer)) {
            MessageConnection messageConnection = new MessageConnection();
            messageConnection.openConnection(messagingServer, messagingUser, messagingPassword, 1);
            return messageConnection;
        } else {
            return null;
        }
    }

    @ApplicationScoped
    @AsyncIndex
    @Produces
    public MessageSender createIndexingMessageSender(@AsyncIndex MessageConnection messageConnection,
                                                     @PropertyValue(name = "Messaging.AsyncIndexingExchange") String asyncIndexingExchange,
                                                     @PropertyValue(name = "Messaging.AsyncIndexingRoutingKey") String asyncIndexingRoutingKey) {
        LOG.info("Create sender and connect to {}:{}", asyncIndexingExchange, asyncIndexingRoutingKey);
        MessageSender messageSender = new MessageSenderImpl(messageConnection);
        messageSender.connectTo(asyncIndexingExchange, asyncIndexingRoutingKey);
        return messageSender;
    }

}

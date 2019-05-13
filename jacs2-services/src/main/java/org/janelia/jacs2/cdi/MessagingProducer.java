package org.janelia.jacs2.cdi;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.messaging.core.MessageSender;
import org.janelia.messaging.core.impl.ConnectionManager;
import org.janelia.messaging.core.impl.MessageSenderImpl;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class MessagingProducer {

    @ApplicationScoped
    @Produces
    public ConnectionManager createConnectionManager() {
        return ConnectionManager.getInstance();
    }

    @ApplicationScoped
    @Produces
    public MessageSender createIndexingMessageSender(ConnectionManager connectionManager,
                                                     @PropertyValue(name = "Messaging.Server") String messagingServer,
                                                     @PropertyValue(name = "Messaging.User") String messagingUser,
                                                     @PropertyValue(name = "Messaging.Password") String messagingPassword,
                                                     @PropertyValue(name = "Messaging.AsyncIndexingExchange") String asyncIndexingExchange,
                                                     @PropertyValue(name = "Messaging.AsyncIndexingRoutingKey") String asyncIndexingRoutingKey) {
        if (StringUtils.isNotBlank(messagingServer)) {
            MessageSender messageSender = new MessageSenderImpl(connectionManager);
            messageSender.connect(messagingServer, messagingUser, messagingPassword, asyncIndexingExchange, asyncIndexingRoutingKey);
            return messageSender;
        } else {
            return null;
        }
    }

}

package org.janelia.jacs2.dataservice.notifservice;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;

public class EmailNotificationService {

    private static final Logger LOG = LoggerFactory.getLogger(EmailNotificationService.class);

    private final boolean disabled;
    private final Properties emailProperties = new Properties();
    private final String senderEmail;
    private final String senderPassword;
    private final boolean authRequired;

    @Inject
    public EmailNotificationService(@PropertyValue(name = "service.email.senderEmail") String senderEmail,
                                    @PropertyValue(name = "service.email.senderPassword") String senderPassword,
                                    @BoolPropertyValue(name = "service.email.authRequired") boolean authRequired,
                                    @BoolPropertyValue(name = "service.email.enableTLS") boolean enableTLS,
                                    @PropertyValue(name = "service.email.smtpHost") String smtpHost,
                                    @IntPropertyValue(name = "service.email.smtpPort", defaultValue = 25) Integer smtpPort) {
        this.senderEmail = senderEmail;
        this.senderPassword = senderPassword;
        this.authRequired = authRequired;
        this.disabled = StringUtils.isBlank(smtpHost);
        emailProperties.put("mail.smtp.auth", String.valueOf(authRequired));
        emailProperties.put("mail.smtp.starttls.enable", String.valueOf(enableTLS));
        emailProperties.put("mail.smtp.host", smtpHost);
        emailProperties.put("mail.smtp.port", smtpPort == null ? "" : smtpPort.toString());
    }

    public void sendNotification(String subject, String textMessage, Collection<String> recipientsCollection) {
        if (disabled) {
            LOG.info("Message {} will not be sent because SMTP host is not configured", subject);
            return;
        }
        String recipients = recipientsCollection.stream().filter(r -> StringUtils.isNotBlank(r)).reduce((r1, r2) -> r1 + "," + r2).orElse("");
        if (StringUtils.isBlank(recipients)) {
            LOG.info("No recipients have been specified for message {}", subject);
            return;
        }
        Session session;
        try {
            if (authRequired) {
                session = Session.getInstance(emailProperties,
                        new javax.mail.Authenticator() {
                            protected PasswordAuthentication getPasswordAuthentication() {
                                return new PasswordAuthentication(senderEmail, senderPassword);
                            }
                        });
            } else {
                session = Session.getInstance(emailProperties);
            }
            LOG.debug("Send {} to {}", subject, recipients);
            Message emailMessage = new MimeMessage(session);
            emailMessage.setFrom(new InternetAddress(senderEmail));
            emailMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients));
            emailMessage.setSubject(subject);
            emailMessage.setText(textMessage);
            Transport.send(emailMessage);
        } catch (Exception e) {
            LOG.warn("Error sending {} to {} from {} using {}", textMessage, recipients, senderEmail, emailProperties);
        }
    }
}

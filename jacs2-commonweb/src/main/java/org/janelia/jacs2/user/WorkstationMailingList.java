package org.janelia.jacs2.user;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Minimalist API for interacting with the Workstation mailing list.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class WorkstationMailingList {

    private static final Logger log = LoggerFactory.getLogger(WorkstationMailingList.class);

    private String subscribeUrl;

    @Inject
    public WorkstationMailingList(@PropertyValue(name = "user.listServ.subscribeUrl")
                                  String subscribeUrl) {
        this.subscribeUrl = subscribeUrl;
    }

    /**
     * Subscribe the given user to the mailing list. This is a synchronous API which blocks
     * on the network.
     * @param email the email address to subscribe
     * @param fullname first and last name of the person being subscribed
     */
    public void subscribe(String email, String fullname) throws Exception {

        if (StringUtils.isBlank(subscribeUrl)) {
            log.debug("No user.listServ.subscribeUrl is configured. User will not be subscribed to mailing list.");
            return;
        }

        Form form = new Form();
        form.param("email-button", "Subscribe")
                .param("email", email)
                .param("fullname", fullname);

        Client client = HttpUtils.createHttpClient();
        WebTarget target = client.target(subscribeUrl);
        Response response = target.request(MediaType.APPLICATION_FORM_URLENCODED)
                .accept(MediaType.TEXT_PLAIN)
                .buildPost(Entity.form(form)).invoke();

        if (!response.getStatusInfo().getFamily().equals(Response.Status.Family.SUCCESSFUL)) {
            throw new Exception("ListServ responded with code " + response.getStatus());
        }

        String body = response.readEntity(String.class);
        if (!body.contains("Your subscription request has been received")) {
            log.debug("Response from ListServ website:\n{}", body);
            throw new Exception("ListServ responded with invalid body");
        }
    }
}

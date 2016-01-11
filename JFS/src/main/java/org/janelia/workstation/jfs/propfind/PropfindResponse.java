package org.janelia.workstation.jfs.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * Created by schauderd on 7/1/15.
 */
public class PropfindResponse {
    @JacksonXmlProperty(localName = "D:href")
    String href;

    @JacksonXmlProperty(localName = "D:propstat")
    Propstat propstat;

    public Propstat getPropstat() {
        return propstat;
    }

    public void setPropstat(Propstat propstat) {
        this.propstat = propstat;
    }

    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
    }
}


package org.janelia.workstation.jfs.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * Created by schauderd on 7/1/15.
 */
public class Propstat {
    @JacksonXmlProperty(localName = "D:prop")
    Prop prop;

    @JacksonXmlProperty(localName = "D:status")
    String status;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Prop getProp() {
        return prop;
    }

    public void setProp(Prop prop) {
        this.prop = prop;
    }
}


package org.janelia.jacs2.utils;

import org.janelia.model.service.JacsServiceData;

import javax.inject.Singleton;

@Singleton
public class CurrentServiceHolder {

    private final ThreadLocal<JacsServiceData> threadLocal = new ThreadLocal<>();

    public JacsServiceData getJacsServiceData() {
       return threadLocal.get();
    }

    public void setJacsServiceData(JacsServiceData jacsServiceData) {
         threadLocal.set(jacsServiceData);
    }    

}
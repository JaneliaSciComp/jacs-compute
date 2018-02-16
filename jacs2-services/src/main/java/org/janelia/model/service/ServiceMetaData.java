package org.janelia.model.service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

import java.util.ArrayList;
import java.util.List;

public class ServiceMetaData {

    private String serviceName;
    private String description;
    private List<ServiceArgDescriptor> serviceArgDescriptors = new ArrayList<>();
    @JsonIgnore
    private ServiceArgs serviceArgsObject;

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<ServiceArgDescriptor> getServiceArgDescriptors() {
        return serviceArgDescriptors;
    }

    public void setServiceArgDescriptors(List<ServiceArgDescriptor> serviceArgDescriptors) {
        this.serviceArgDescriptors = serviceArgDescriptors;
    }

    public void addServiceArgDescriptor(ServiceArgDescriptor serviceArgDescriptor) {
        serviceArgDescriptors.add(serviceArgDescriptor);
    }

    public ServiceArgs getServiceArgsObject() {
        return serviceArgsObject;
    }

    public void setServiceArgsObject(ServiceArgs serviceArgsObject) {
        this.serviceArgsObject = serviceArgsObject;
    }
}

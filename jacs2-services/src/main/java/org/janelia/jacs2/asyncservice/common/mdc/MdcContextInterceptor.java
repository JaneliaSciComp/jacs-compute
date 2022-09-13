package org.janelia.jacs2.asyncservice.common.mdc;

import org.janelia.model.service.JacsServiceData;
import org.slf4j.MDC;

import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

@MdcContext
@Interceptor
public class MdcContextInterceptor {

    @AroundInvoke
    public Object setMdcContext(InvocationContext invocationContext)
            throws Exception {
        Object[] args = invocationContext.getParameters();
        String currentRootServiceContext = MDC.get("rootService");
        String currentServiceNameContext = MDC.get("serviceName");
        String currentServiceIdContext = MDC.get("serviceId");
        // find an arg of type JacsServiceData and put the service name and ID into the MDC
        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof JacsServiceData) {
                    JacsServiceData sd = (JacsServiceData) arg;
                    MDC.put("serviceName", sd.getShortName());
                    if (sd.getServiceId() != null) {
                        MDC.put("serviceId", sd.getServiceId());
                    } else {
                        MDC.put("serviceId", "N/A");
                    }
                    if (sd.hasRootServiceId()) {
                        MDC.put("rootService", sd.getRootServiceId().toString());
                    } else {
                        MDC.put("rootService", "N/A");
                    }
                }
            }
        }
        Object result = invocationContext.proceed();
        if (currentServiceNameContext != null) {
            MDC.put("serviceName", currentServiceNameContext);
        } else {
            MDC.remove("serviceName");
        }
        if (currentServiceIdContext != null) {
            MDC.put("serviceId", currentServiceIdContext);
        } else {
            MDC.remove("serviceId");
        }
        if (currentRootServiceContext != null) {
            MDC.put("rootService", currentRootServiceContext);
        } else {
            MDC.remove("rootService");
        }
        return result;
    }

}

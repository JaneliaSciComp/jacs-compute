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
        String currentServiceNameContext = MDC.get("serviceName");
        String currentServiceIdContext = MDC.get("serviceId");
        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof JacsServiceData) {
                    JacsServiceData sd = (JacsServiceData) arg;
                    MDC.put("serviceName", sd.getName());
                    if (sd.getServiceId() != null) {
                        MDC.put("serviceId", sd.getServiceId());
                    } else {
                        MDC.put("serviceId", "N/A");
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
        return result;
    }

}

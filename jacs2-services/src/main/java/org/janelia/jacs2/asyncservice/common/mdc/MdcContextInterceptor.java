package org.janelia.jacs2.asyncservice.common.mdc;

import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.MDC;

import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

@MdcContext
@Interceptor
public class MdcContextInterceptor {

    @AroundInvoke
    public Object setMdcContext(InvocationContext invocationContext) throws Exception {

        // Save state
        String currentServiceContext = MDC.get("serviceName");
        String currentRootService = MDC.get("rootService");

        // Add MDC if possible
        Object[] args = invocationContext.getParameters();
        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof JacsServiceData) {
                    JacsServiceData sd = (JacsServiceData) arg;
                    addServiceDataMDC(sd);
                }
                else if (arg instanceof JacsServiceResult) {
                    JacsServiceResult sr = (JacsServiceResult) arg;
                    addServiceDataMDC(sr.getJacsServiceData());
                }
            }
        }

        // Execute the method
        Object result = invocationContext.proceed();

        // Restore state
        if (currentServiceContext != null) {
            MDC.put("serviceName", currentServiceContext);
        }
        else {
            MDC.remove("serviceName");
        }

        if (currentRootService != null) {
            MDC.put("rootService", currentRootService);
        }
        else {
            MDC.remove("rootService");
        }

        return result;
    }

    private void addServiceDataMDC(JacsServiceData sd) {
        MDC.put("serviceName", sd.getName());
        if (sd.getRootServiceId() != null) {
            MDC.put("rootService", sd.getRootServiceId().toString());
        } else {
            if (sd.getId()!=null) {
                MDC.put("rootService", sd.getId().toString());
            }
            else {
                MDC.put("rootService", "NEW");
            }
        }
    }
}

package org.janelia.jacs2.asyncservice.common.mdc;

import org.janelia.jacs2.model.jacsservice.JacsServiceData;
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
        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof JacsServiceData) {
                    JacsServiceData sd = (JacsServiceData) arg;
                    MDC.put("serviceName", sd.getName());
                    if (sd.getRootServiceId() != null) {
                        MDC.put("rootService", sd.getRootServiceId().toString());
                    } else {
                        MDC.put("rootService", "ROOT");
                    }
                }
            }
        }
        return invocationContext.proceed();
    }

}

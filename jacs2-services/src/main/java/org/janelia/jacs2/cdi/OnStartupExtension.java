package org.janelia.jacs2.cdi;

import java.lang.reflect.Type;
import java.util.LinkedHashSet;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AfterDeploymentValidation;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.ProcessBean;

import org.janelia.jacs2.cdi.qualifier.OnStartup;

public class OnStartupExtension implements Extension {

    private Set<Type> eagerBeanTypes = new LinkedHashSet<>();

    public <T> void collect(@Observes ProcessBean<T> event) {
        if (event.getAnnotated().isAnnotationPresent(OnStartup.class)
                && event.getAnnotated().isAnnotationPresent(ApplicationScoped.class)) {
            eagerBeanTypes.add(event.getAnnotated().getBaseType());
        }
    }

    public void load(@Observes AfterDeploymentValidation event, BeanManager beanManager) {
        for (Type beanType : eagerBeanTypes) {
            // note: toString() is important to instantiate the bean
            beanManager.getBeans(beanType)
                    .forEach(b -> {
                        beanManager.getReference(b, beanType, beanManager.createCreationalContext(b)).toString();
                    });
        }
    }

}

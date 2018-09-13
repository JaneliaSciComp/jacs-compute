package org.janelia.jacs2.cdi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.CDI;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class CDIUtils {

    private static final Logger log = LoggerFactory.getLogger(CDIUtils.class);

    @SuppressWarnings("unchecked")
    public static <T> T getBeanByName(Class<T> beanType, String beanName) {

        // Find beans with the given type and name
        BeanManager bm = CDI.current().getBeanManager();
        Set<Bean<?>> beans = bm.getBeans(beanName).stream()
                .filter(b -> beanType.isAssignableFrom(b.getBeanClass()))
                .collect(Collectors.toSet());

        if (beans==null || beans.isEmpty()) {
            log.error("No service found with name '{}'", beanName);
            return null;
        }

        if (beans.size()>1) {
            log.warn("More than one service found with name '{}'. Choosing one at random!", beanName);
        }

        // Get an instance
        Bean<T> bean = (Bean<T>) beans.iterator().next();
        CreationalContext<T> ctx = bm.createCreationalContext(bean);
        return (T) bm.getReference(bean, bean.getBeanClass(), ctx);
    }
}

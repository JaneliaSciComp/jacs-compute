package org.janelia.jacs2.dataservice.search;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.ontology.DomainAnnotationGetter;
import org.janelia.model.domain.ontology.SimpleDomainAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedDomainAnnotationGetter implements DomainAnnotationGetter {

    private static final Logger LOG = LoggerFactory.getLogger(CachedDomainAnnotationGetter.class);

    private final AnnotationDao annotationDao;
    private final Cache<String, Map<Reference, Set<SimpleDomainAnnotation>>> domainAnnotationsMapCache;

    @Inject
    public CachedDomainAnnotationGetter(AnnotationDao annotationDao) {
        this.annotationDao = annotationDao;
        domainAnnotationsMapCache = CacheBuilder.newBuilder()
                .expireAfterWrite(10, TimeUnit.HOURS)
                .build();
    }

    @Override
    public Set<SimpleDomainAnnotation> getAnnotations(Reference ref) {
        return getAnnotationsMap().get(ref);
    }

    private Map<Reference, Set<SimpleDomainAnnotation>> getAnnotationsMap() {
        try {
            return domainAnnotationsMapCache.get("annotationMapCache", () -> loadCache());
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<Reference, Set<SimpleDomainAnnotation>> loadCache() {
        LOG.info("Start domain annotations cache");
        try {
            return annotationDao.streamAll()
                    .collect(Collectors.groupingBy(a -> a.getTarget(), Collectors.mapping(
                            a -> new SimpleDomainAnnotation(a.getName(),
                            a.getReaders()), Collectors.toSet())));
        } finally {
            LOG.info("Finished loading annotations cache");
        }
    }
}

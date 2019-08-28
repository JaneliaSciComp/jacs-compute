package org.janelia.jacs2.rest.sync.testrest;

import org.glassfish.hk2.api.Injectee;
import org.glassfish.hk2.api.InjectionResolver;
import org.glassfish.hk2.api.ServiceHandle;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.janelia.jacs2.asyncservice.maintenanceservices.DbMaintainer;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.dataservice.search.IndexingService;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.access.domain.dao.OntologyDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.access.domain.dao.SummaryDao;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmReviewTaskDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.rendering.RenderedVolumeLoader;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.lang.annotation.Annotation;
import java.util.concurrent.ExecutorService;

public class TestResourceBinder extends AbstractBinder {
    static class PropertyResolver implements InjectionResolver<PropertyValue> {

        @Inject
        ApplicationConfig applicationConfig;


        @Override
        public Object resolve(Injectee injectee, ServiceHandle<?> root) {
            return injectee.getRequiredQualifiers().stream().filter(a -> a.annotationType().equals(PropertyValue.class))
                    .findFirst()
                    .map(a -> (PropertyValue) a)
                    .map(pv -> applicationConfig.getStringPropertyValue(pv.name()))
                    .orElseThrow(() -> new IllegalStateException("PropertyValue not found"))
                    ;
        }

        @Override
        public boolean isConstructorParameterIndicator() {
            return false;
        }

        @Override
        public boolean isMethodParameterIndicator() {
            return false;
        }
    };

    private final TestResourceDependenciesProducer dependenciesProducer;

    @Inject
    public TestResourceBinder(TestResourceDependenciesProducer dependenciesProducer) {
        this.dependenciesProducer = dependenciesProducer;
    }

    @Override
    protected void configure() {
        // for a class qualified with an annotation to be injected properly in the unit test,
        // it must both be annotated in the producer and qualified with the proper annotation instance in the binder
        ApplicationProperties applicationPropertiesAnnotation = getAnnotation(ApplicationProperties.class, "getApplicationConfig");
        AsyncIndex asyncIndexAnnotation = getAnnotation(AsyncIndex.class, "getDatasetSearchableDao");

        bind(dependenciesProducer.getLogger()).to(Logger.class);
        bind(dependenciesProducer.getApplicationConfig()).to(ApplicationConfig.class).qualifiedBy(applicationPropertiesAnnotation);
        bind(dependenciesProducer.getAnnotationSearchableDao()).to(AnnotationDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getDatasetDao()).to(DatasetDao.class);
        bind(dependenciesProducer.getDatasetSearchableDao()).to(DatasetDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getLineReleaseSearchableDao()).to(LineReleaseDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getLegacyDomainDao()).to(LegacyDomainDao.class);
        bind(dependenciesProducer.getObjectMapperFactory()).to(ObjectMapperFactory.class);
        bind(dependenciesProducer.getVolumeLocationFactory()).to(RenderedVolumeLocationFactory.class);
        bind(dependenciesProducer.getJwtProvider()).to(JWTProvider.class);
        bind(dependenciesProducer.getOntologySearchableDao()).to(OntologyDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getRenderedVolumeLoader()).to(RenderedVolumeLoader.class);
        bind(dependenciesProducer.getStorageService()).to(StorageService.class);
        bind(dependenciesProducer.getSummaryDao()).to(SummaryDao.class);
        bind(dependenciesProducer.getTmReviewTaskSearchableDao()).to(TmReviewTaskDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getTmSampleSearchableDao()).to(TmSampleDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getWorkspaceNodeSearchableDao()).to(WorkspaceNodeDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getTmNeuronMetadataSearchableDao()).to(TmNeuronMetadataDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getTmWorkspaceSearchableDao()).to(TmWorkspaceDao.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getSubjectDao()).to(SubjectDao.class);
        bind(dependenciesProducer.getPwProvider()).to(PasswordProvider.class);
        bind(dependenciesProducer.getAuthProvider()).to(AuthProvider.class);
        bind(dependenciesProducer.getDomainObjectIndexer()).to(DomainObjectIndexer.class);
        bind(dependenciesProducer.getIndexingService()).to(IndexingService.class);
        bind(dependenciesProducer.getSampleDataService()).to(SampleDataService.class);
        bind(dependenciesProducer.getSageDataService()).to(SageDataService.class);
        bind(dependenciesProducer.getIndexingExecutorService()).to(ExecutorService.class).qualifiedBy(asyncIndexAnnotation);
        bind(dependenciesProducer.getDbMaintainer()).to(DbMaintainer.class);
        bind(PropertyResolver.class)
                .to(new TypeLiteral<InjectionResolver<PropertyValue>>() {})
                .in(Singleton.class);
    }

    private <A extends Annotation> A getAnnotation(Class<A> annotationClass, String exampleMethodName, Class<?>... exampleMethodParameterTypes) {
        try {
            return dependenciesProducer.getClass().getMethod(exampleMethodName, exampleMethodParameterTypes).getAnnotation(annotationClass);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

}

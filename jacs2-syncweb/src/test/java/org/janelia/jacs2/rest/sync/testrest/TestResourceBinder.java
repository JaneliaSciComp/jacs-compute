package org.janelia.jacs2.rest.sync.testrest;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.dataservice.search.SolrConnector;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.OntologyDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.access.domain.dao.SummaryDao;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmReviewTaskDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.rendering.RenderedVolumeLoader;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.lang.annotation.Annotation;

public class TestResourceBinder extends AbstractBinder {
    private final TestResourceDependenciesProducer dependenciesProducer;

    @Inject
    public TestResourceBinder(TestResourceDependenciesProducer dependenciesProducer) {
        this.dependenciesProducer = dependenciesProducer;
    }

    @Override
    protected void configure() {
        bind(dependenciesProducer.getLogger()).to(Logger.class);
        bind(dependenciesProducer.getAnnotationDao()).to(AnnotationDao.class);
        bind(dependenciesProducer.getDatasetDao()).to(DatasetDao.class);
        bind(dependenciesProducer.getLegacyDomainDao()).to(LegacyDomainDao.class);
        bind(dependenciesProducer.getObjectMapperFactory()).to(ObjectMapperFactory.class);
        bind(dependenciesProducer.getVolumeLocationFactory()).to(RenderedVolumeLocationFactory.class);
        bind(dependenciesProducer.getJwtProvider()).to(JWTProvider.class);
        bind(dependenciesProducer.getOntologyDao()).to(OntologyDao.class);
        bind(dependenciesProducer.getRenderedVolumeLoader()).to(RenderedVolumeLoader.class);
        bind(dependenciesProducer.getStorageService()).to(StorageService.class);
        bind(dependenciesProducer.getSummaryDao()).to(SummaryDao.class);
        bind(dependenciesProducer.getTmReviewTaskDao()).to(TmReviewTaskDao.class);
        bind(dependenciesProducer.getTmSampleDao()).to(TmSampleDao.class);
        bind(dependenciesProducer.getWorkspaceNodeDao()).to(WorkspaceNodeDao.class);
        bind(dependenciesProducer.getTmNeuronMetadataDao()).to(TmNeuronMetadataDao.class);
        bind(dependenciesProducer.getTmWorkspaceDao()).to(TmWorkspaceDao.class);
        bind(dependenciesProducer.getSubjectDao()).to(SubjectDao.class);
        bind(dependenciesProducer.getPwProvider()).to(PasswordProvider.class);
        bind(dependenciesProducer.getAuthProvider()).to(AuthProvider.class);
        bind(dependenciesProducer.getSolrConnector()).to(SolrConnector.class);
        bind(dependenciesProducer.getSampleDataService()).to(SampleDataService.class);
        bind(dependenciesProducer.getSageDataService()).to(SageDataService.class);
        bind(dependenciesProducer.defaultVolume).to(String.class).qualifiedBy(new Annotation() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return PropertyValue.class;
            }
        });
    }
}

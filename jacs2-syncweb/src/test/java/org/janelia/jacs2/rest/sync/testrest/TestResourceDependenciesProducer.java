package org.janelia.jacs2.rest.sync.testrest;

import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.dataservice.search.SolrConnector;
import org.janelia.jacs2.dataservice.search.SolrIndexer;
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
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Produces;

import static org.mockito.Mockito.mock;

public class TestResourceDependenciesProducer {

    private Logger logger = LoggerFactory.getLogger(TestResourceDependenciesProducer.class);
    private AnnotationDao annotationDao = mock(AnnotationDao.class);
    private DatasetDao datasetDao = mock(DatasetDao.class);
    private JWTProvider jwtProvider = mock(JWTProvider.class);
    private LegacyDomainDao legacyDomainDao = mock(LegacyDomainDao.class);
    private ObjectMapperFactory objectMapperFactory = ObjectMapperFactory.instance();
    private RenderedVolumeLocationFactory volumeLocationFactory = mock(RenderedVolumeLocationFactory.class);
    private OntologyDao ontologyDao = mock(OntologyDao.class);
    private RenderedVolumeLoader renderedVolumeLoader = mock(RenderedVolumeLoader.class);
    private SummaryDao summaryDao = mock(SummaryDao.class);
    private StorageService storageService = mock(StorageService.class);
    private TmNeuronMetadataDao tmNeuronMetadataDao = mock(TmNeuronMetadataDao.class);
    private TmWorkspaceDao tmWorkspaceDao = mock(TmWorkspaceDao.class);
    private TmReviewTaskDao tmReviewTaskDao = mock(TmReviewTaskDao.class);
    private TmSampleDao tmSampleDao = mock(TmSampleDao.class);
    private WorkspaceNodeDao workspaceNodeDao = mock(WorkspaceNodeDao.class);
    private SubjectDao subjectDao = mock(SubjectDao.class);
    private PasswordProvider pwProvider = mock(PasswordProvider.class);
    private AuthProvider authProvider = mock(AuthProvider.class);
    private SolrConnector solrConnector = mock(SolrConnector.class);
    private SolrIndexer solrIndexer = mock(SolrIndexer.class);
    private SampleDataService sampleDataService = mock(SampleDataService.class);
    private SageDataService sageDataService = mock(SageDataService.class);

    @Produces
    @PropertyValue(name = "Dataset.Storage.DefaultVolume")
    public String defaultVolume = "testVolume";

    @Produces
    public Logger getLogger() {
        return logger;
    }

    @Produces
    public AnnotationDao getAnnotationDao() {
        return annotationDao;
    }

    @Produces
    public DatasetDao getDatasetDao() {
        return datasetDao;
    }

    @Produces
    public JWTProvider getJwtProvider() {
        return jwtProvider;
    }

    @Produces
    public LegacyDomainDao getLegacyDomainDao() {
        return legacyDomainDao;
    }

    @Produces
    public ObjectMapperFactory getObjectMapperFactory() {
        return objectMapperFactory;
    }

    @Produces
    public RenderedVolumeLocationFactory getVolumeLocationFactory() {
        return volumeLocationFactory;
    }

    @Produces
    public OntologyDao getOntologyDao() {
        return ontologyDao;
    }

    @Produces
    public RenderedVolumeLoader getRenderedVolumeLoader() {
        return renderedVolumeLoader;
    }

    @Produces
    public SummaryDao getSummaryDao() {
        return summaryDao;
    }

    @Produces
    public StorageService getStorageService() {
        return storageService;
    }

    @Produces
    public TmNeuronMetadataDao getTmNeuronMetadataDao() {
        return tmNeuronMetadataDao;
    }

    @Produces
    public TmWorkspaceDao getTmWorkspaceDao() {
        return tmWorkspaceDao;
    }

    @Produces
    public TmReviewTaskDao getTmReviewTaskDao() {
        return tmReviewTaskDao;
    }

    @Produces
    public TmSampleDao getTmSampleDao() {
        return tmSampleDao;
    }

    @Produces
    public WorkspaceNodeDao getWorkspaceNodeDao() {
        return workspaceNodeDao;
    }

    @Produces
    public SubjectDao getSubjectDao() {
        return subjectDao;
    }

    @Produces
    public PasswordProvider getPwProvider() {
        return pwProvider;
    }

    @Produces
    public AuthProvider getAuthProvider() {
        return authProvider;
    }

    @Produces
    public SolrConnector getSolrConnector() {
        return solrConnector;
    }

    @Produces
    public SolrIndexer getSolrIndexer() {
        return solrIndexer;
    }

    @Produces
    public SampleDataService getSampleDataService() {
        return sampleDataService;
    }

    @Produces
    public SageDataService getSageDataService() {
        return sageDataService;
    }
}

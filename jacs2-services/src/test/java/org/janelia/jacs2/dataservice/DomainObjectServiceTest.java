package org.janelia.jacs2.dataservice;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.dao.DaoFactory;
import org.janelia.model.domain.Reference;
import org.janelia.model.jacs2.dao.ImageDao;
import org.janelia.model.jacs2.dao.SampleDao;
import org.janelia.model.jacs2.domain.DomainObject;
import org.janelia.model.jacs2.domain.Subject;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DomainObjectServiceTest {

    private SampleDao sampleDao;
    private ImageDao imageDao;

    private DomainObjectService testService;

    @Before
    public void setUp() {
        sampleDao = mock(SampleDao.class);
        DaoFactory daoFactory = mock(DaoFactory.class);
        imageDao = mock(ImageDao.class);
        testService  = new DomainObjectService(daoFactory);
        when(daoFactory.createDomainObjectDao("LSMImage")).thenAnswer(invocation -> imageDao);
        when(daoFactory.createDomainObjectDao("Sample")).thenAnswer(invocation -> sampleDao);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void streamListOfMixedReferences() {
        List<Reference> testData = ImmutableList.of(
                Reference.createFor("LSMImage", 1L),
                Reference.createFor("Sample", 2L),
                Reference.createFor("Sample", 3L),
                Reference.createFor("LSMImage", 4L),
                Reference.createFor("Sample", 5L));
        Stream<DomainObject> testStream = testService.streamAllReferences(null, testData.stream());
        verify(imageDao, never()).findByIds(any(Subject.class), anyList());
        verify(sampleDao, never()).findByIds(any(Subject.class), anyList());

        testStream.collect(Collectors.toList());
        verify(imageDao).findByIds(null, ImmutableList.of(1L, 4L));
        verify(sampleDao).findByIds(null, ImmutableList.of(2L, 3L, 5L));
    }

}

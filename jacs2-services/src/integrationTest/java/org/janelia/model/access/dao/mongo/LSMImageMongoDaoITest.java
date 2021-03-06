package org.janelia.model.access.dao.mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.model.domain.Reference;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.dao.LSMImageDao;
import org.janelia.model.jacs2.dao.mongo.LSMImageMongoDao;
import org.janelia.model.jacs2.domain.sample.LSMImage;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.jacs2.page.SortDirection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

public class LSMImageMongoDaoITest extends AbstractDomainObjectDaoITest<LSMImage> {
    private List<LSMImage> testData = new ArrayList<>();
    private LSMImageDao testDao;

    @Before
    public void setUp() {
        testDao = new LSMImageMongoDao(testMongoDatabase, idGenerator);
    }

    @After
    public void tearDown() {
        // delete the data that was created for testing
        deleteAll(testDao, testData);
    }

    @Test
    public void findMatchingImagesBySageId() {
        List<LSMImage> testImages = createMultipleTestItems(10);
        int sageIdGroups = 3;
        testImages.stream()
                .map(im -> {
                    im.setSageId((im.getSageId() % sageIdGroups) + 1);
                    return im;
                })
                .forEach(testDao::save);
        PageRequest pageRequest = new PageRequest();
        pageRequest.setSortCriteria(ImmutableList.of(
                new SortCriteria("line", SortDirection.ASC)));
        IntStream.rangeClosed(1, sageIdGroups)
                .forEach(i -> {
                    LSMImage lsmPattern = new LSMImage();
                    lsmPattern.setSageId(i);
                    PageResult<LSMImage> res = testDao.findMatchingLSMs(null, lsmPattern, pageRequest);
                    MatcherAssert.assertThat(res.getResultList(), everyItem(
                            allOf(
                                    Matchers.<LSMImage>instanceOf(LSMImage.class),
                                    Matchers.in(testImages),
                                    new HasPropertyWithValue<>("sageId", equalTo(i))
                            )));
                });
    }

    @Test
    public void findMatchingImagesBySlideCode() {
        List<LSMImage> testImages = createMultipleTestItems(10);
        int slideCodeGroups = 3;
        testImages.stream()
                .map(im -> {
                    im.setSlideCode(String.valueOf(im.getSageId() % slideCodeGroups));
                    return im;
                })
                .forEach(testDao::save);
        PageRequest pageRequest = new PageRequest();
        pageRequest.setSortCriteria(ImmutableList.of(
                new SortCriteria("line", SortDirection.ASC)));
        IntStream.range(0, slideCodeGroups)
                .mapToObj(i -> String.valueOf(i))
                .forEach(sc -> {
                    LSMImage lsmPattern = new LSMImage();
                    lsmPattern.setSlideCode(sc);
                    PageResult<LSMImage> res = testDao.findMatchingLSMs(null, lsmPattern, pageRequest);
                    MatcherAssert.assertThat(res.getResultList(), everyItem(
                            allOf(
                                    Matchers.<LSMImage>instanceOf(LSMImage.class),
                                    Matchers.in(testImages),
                                    new HasPropertyWithValue<>("slideCode", equalTo(sc))
                            )));
                });
    }

    @Test
    public void updateSampleRefs() {
        List<LSMImage> testImages = createMultipleTestItems(10);
        int sageIdGroups = 3;
        testImages.stream()
                .map(im -> {
                    im.setSageId((im.getSageId() % sageIdGroups) + 1);
                    return im;
                })
                .forEach(testDao::save);
        Reference newSampleRef = Reference.createFor("Sample#456");
        Map<String, EntityFieldValueHandler<?>> updatedFields = ImmutableMap.of("sampleRef", new SetFieldValueHandler<>(newSampleRef));
        testImages.stream()
                .forEach(im -> testDao.update(im, updatedFields));

        PageRequest pageRequest = new PageRequest();
        PageResult<LSMImage> result = testDao.findAll(pageRequest);
        MatcherAssert.assertThat(result.getResultList(), hasSize(testImages.size()));
        assertTrue(result.getResultList().stream().allMatch(im -> newSampleRef.equals(im.getSampleRef())));
    }

    @Test
    public void updateSampleRefsToNull() {
        List<LSMImage> testImages = createMultipleTestItems(10);
        int sageIdGroups = 3;
        testImages.stream()
                .map(im -> {
                    im.setSageId((im.getSageId() % sageIdGroups) + 1);
                    return im;
                })
                .forEach(testDao::save);
        Map<String, EntityFieldValueHandler<?>> updatedFields = new HashMap<>();
        updatedFields.put("sampleRef", new SetFieldValueHandler<>(null));
        testImages.stream()
                .forEach(im -> testDao.update(im, updatedFields));
        PageRequest pageRequest = new PageRequest();
        PageResult<LSMImage> result = testDao.findAll(pageRequest);
        MatcherAssert.assertThat(result.getResultList(), hasSize(testImages.size()));
        assertTrue(result.getResultList().stream().allMatch(im -> im.getSampleRef() == null));
    }

    @Override
    protected List<LSMImage> createMultipleTestItems(int nItems) {
        List<LSMImage> testItems = new ArrayList<>();
        Reference sampleRef = Reference.createFor("Sample#12345");
        for (int i = 0; i < nItems; i++) {
            testItems.add(createImage(i % 2 == 0 ? sampleRef : null, i + 1, "l" + (i + 1), "a" + (i + 1)));
        }
        return testItems;
    }

    private LSMImage createImage(Reference sampleRef, int sageId, String line, String area) {
        LSMImage lsmImage = new LSMImage();
        lsmImage.setChannelColors("cygkrgb");
        lsmImage.setChannelDyeNames("dye");
        lsmImage.setBrightnessCompensation("compensated");
        lsmImage.setSageId(dataGenerator.nextInt());
        lsmImage.setLine(line);
        lsmImage.setAnatomicalArea(area);
        lsmImage.setSageId(sageId);
        lsmImage.setOwnerKey(TEST_OWNER_KEY);
        lsmImage.setSampleRef(sampleRef);
        testData.add(lsmImage);
        return lsmImage;
    }
}

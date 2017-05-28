package org.janelia.jacs2.dao.mongo;

import com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.jacs2.dao.LSMImageDao;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;
import org.janelia.jacs2.model.page.SortCriteria;
import org.janelia.jacs2.model.page.SortDirection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.Assert.assertThat;

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
                    assertThat(res.getResultList(), everyItem(
                            allOf(
                                    Matchers.<LSMImage>instanceOf(LSMImage.class),
                                    isIn(testImages),
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
                    assertThat(res.getResultList(), everyItem(
                            allOf(
                                    Matchers.<LSMImage>instanceOf(LSMImage.class),
                                    isIn(testImages),
                                    new HasPropertyWithValue<>("slideCode", equalTo(sc))
                            )));
                });
    }

    @Override
    protected List<LSMImage> createMultipleTestItems(int nItems) {
        List<LSMImage> testItems = new ArrayList<>();
        for (int i = 0; i < nItems; i++) {
            testItems.add(createImage(i + 1, "l" + (i + 1), "a" + (i + 1)));
        }
        return testItems;
    }

    private LSMImage createImage(int sageId, String line, String area) {
        LSMImage lsmImage = new LSMImage();
        lsmImage.setChannelColors("cygkrgb");
        lsmImage.setChannelDyeNames("dye");
        lsmImage.setBrightnessCompensation("compensated");
        lsmImage.setSageId(dataGenerator.nextInt());
        lsmImage.setLine(line);
        lsmImage.setAnatomicalArea(area);
        lsmImage.setSageId(sageId);
        lsmImage.setOwnerKey(TEST_OWNER_KEY);
        testData.add(lsmImage);
        return lsmImage;
    }
}

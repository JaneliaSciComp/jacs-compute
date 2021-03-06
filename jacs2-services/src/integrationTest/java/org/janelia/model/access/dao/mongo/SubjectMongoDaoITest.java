package org.janelia.model.access.dao.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.hamcrest.MatcherAssert;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.dao.SubjectDao;
import org.janelia.model.jacs2.dao.mongo.SubjectMongoDao;
import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.jacs2.page.SortDirection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Objects.isNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.same;

public class SubjectMongoDaoITest extends AbstractMongoDaoITest<Subject> {

    private List<Subject> testData = new ArrayList<>();
    private SubjectDao testDao;

    @Before
    public void setUp() {
        testDao = new SubjectMongoDao(testMongoDatabase, idGenerator);
    }

    @After
    public void tearDown() {
        // delete the data that was created for testing
        deleteAll(testDao, testData);
    }

    @Test
    public void findAll() {
        List<Subject> testSubjects = createMultipleTestItems(9);
        testSubjects.parallelStream().forEach(s -> testDao.save(s));
        PageRequest pageRequest = new PageRequest();
        pageRequest.setPageNumber(0);
        pageRequest.setSortCriteria(ImmutableList.of(
                new SortCriteria("name", SortDirection.ASC)));
        PageResult<Subject> res = testDao.findAll(pageRequest);
        MatcherAssert.assertThat(res.getResultList(), hasSize(testSubjects.size()));
        assertEquals(testSubjects.stream().map(s -> s.getId()).collect(Collectors.toList()),
                        res.getResultList().stream().map(s -> s.getId()).collect(Collectors.toList()));
    }

    @Test
    public void findByName() {
        Subject testSubject = createTestSubject("s1", "g1");
        testDao.save(testSubject);
        Subject retrievedSubject = testDao.findByName(testSubject.getName());
        MatcherAssert.assertThat(retrievedSubject, not(isNull(Subject.class)));
        MatcherAssert.assertThat(retrievedSubject, not(same(testSubject)));
    }

    @Test
    public void persistSubject() {
        Subject testSubject = createTestSubject("s1", "g1");
        testDao.save(testSubject);
        Subject retrievedSubject = testDao.findById(testSubject.getId());
        MatcherAssert.assertThat(retrievedSubject, not(isNull(Subject.class)));
        MatcherAssert.assertThat(retrievedSubject, not(same(testSubject)));
    }

    @Test
    public void updateSubject() {
        Subject testSubject = createTestSubject("s1", "g1", "g2", "g3");
        String testFullname = "New Fullname";
        testDao.save(testSubject);
        testSubject.addGroup("newGroup1");
        testSubject.addGroup("newGroup2");
        testSubject.setFullName(testFullname);
        testDao.update(testSubject, ImmutableMap.of(
                "groups", new SetFieldValueHandler<>(testSubject.getGroups()),
                "fullName", new SetFieldValueHandler<>(testSubject.getFullName()))
        );
        Subject retrievedSample = testDao.findById(testSubject.getId());
        MatcherAssert.assertThat(retrievedSample, hasProperty("key", equalTo("user:s1")));
        MatcherAssert.assertThat(retrievedSample, hasProperty("fullName", equalTo(testFullname)));
    }

    protected List<Subject> createMultipleTestItems(int nItems) {
        List<Subject> testItems = new ArrayList<>();
        for (int i = 0; i < nItems; i++) {
            testItems.add(createTestSubject("s" + (i + 1), "group" + (i + 1)));
        }
        return testItems;
    }

    private Subject createTestSubject(String name, String... groups) {
        Subject testSubject = new Subject();
        testSubject.setName(name);
        testSubject.setKey("user:" + name);
        testSubject.setEmail(name +"@example.com");
        for (String group : groups) {
            testSubject.addGroup("group:" + group);
        }
        testData.add(testSubject);
        return testSubject;
    }

}

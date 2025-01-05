package org.janelia.jacs2.rest.sync.v2;

import java.util.List;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.MatcherAssert;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class UserResourceTest extends AbstractSyncServicesAppResourceTest {

    private static final String TEST_USERNAME = "testlogin";
    private static final String TEST_USER_KEY = "user:" + TEST_USERNAME;

    @Test
    public void getSubjectByNameOrKey() {
        List<Pair<String, User>> testData = ImmutableList.of(
                ImmutablePair.of("u1", null),
                ImmutablePair.of("u2", createUser("user:u1", "u1")),
                ImmutablePair.of("user:u2", createUser("user:u2", "u2"))
        );
        prepareTestData(testData);
        testData.forEach(ksPair -> {
            Response testResponse = target()
                    .path("data/user/subject")
                    .queryParam("subjectKey", ksPair.getLeft())
                    .request(MediaType.APPLICATION_JSON)
                    .header("username", TEST_USERNAME)
                    .get();
            assertEquals("Status check failed for " + ksPair.getKey(), 200, testResponse.getStatus());
            Subject returnedSubject = testResponse.readEntity(Subject.class);
            if (ksPair.getRight() == null) {
                assertNull(returnedSubject);
            } else {
                MatcherAssert.assertThat(returnedSubject, hasProperty("key", equalTo(ksPair.getRight().getKey())));
            }
        });
    }

    @Test
    public void getSubjectByNameOrKeyWhenNoAuthenticationHeader() {
        List<Pair<String, User>> testData = ImmutableList.of(
                ImmutablePair.of("u1", null),
                ImmutablePair.of("u2", createUser("user:u1", "u1")),
                ImmutablePair.of("user:u2", createUser("user:u2", "u2"))
        );
        prepareTestData(testData);
        testData.forEach(ksPair -> {
            Response testResponse = target()
                    .path("data/user/subject")
                    .queryParam("subjectKey", ksPair.getLeft())
                    .request(MediaType.APPLICATION_JSON)
                    .get();
            assertEquals(401, testResponse.getStatus());
        });
    }

    private User createUser(String key, String name) {
        User s = new User();
        s.setKey(key);
        s.setName(name);
        return s;
    }

    private void prepareTestData(List<Pair<String, User>> keyToSubjectMapping) {
        keyToSubjectMapping.forEach(keySubjectPair -> {
            Mockito.when(dependenciesProducer.getSubjectDao().findSubjectByNameOrKey(keySubjectPair.getLeft())).thenReturn(keySubjectPair.getRight());
            Mockito.when(dependenciesProducer.getSubjectDao().findUserByNameOrKey(keySubjectPair.getLeft())).thenReturn(keySubjectPair.getRight());
        });
        User testUser = createUser(TEST_USER_KEY, TEST_USERNAME);
        Mockito.when(dependenciesProducer.getSubjectDao().findSubjectByNameOrKey(TEST_USERNAME))
                .thenReturn(testUser);
        Mockito.when(dependenciesProducer.getSubjectDao().findUserByNameOrKey(TEST_USERNAME))
                .thenReturn(testUser);
    }
}

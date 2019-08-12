package org.janelia.jacs2.rest.sync.v2;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class UserResourceTest extends AbstractSyncServicesAppResourceTest {

    private static final String TEST_USERNAME = "testlogin";
    private static final String TEST_USER_KEY = "user:" + TEST_USERNAME;

    @Test
    public void getSubjectByNameOrKey() {
        List<Pair<String, Subject>> testData = ImmutableList.of(
                ImmutablePair.of("u1", null),
                ImmutablePair.of("u2", createSubject("user:u1", "u1", () -> new User())),
                ImmutablePair.of("user:u2", createSubject("user:u2", "u2", () -> new User()))
        );
        prepareTestData(testData);
        testData.forEach(ksPair -> {
            Response testResponse = target()
                    .path("data/user/subject")
                    .queryParam("subjectKey", ksPair.getLeft())
                    .request(MediaType.APPLICATION_JSON)
                    .header("username", TEST_USERNAME)
                    .get();
            assertEquals(200, testResponse.getStatus());
            Subject returnedSubject = testResponse.readEntity(Subject.class);
            if (ksPair.getRight() == null) {
                assertNull(returnedSubject);
            } else {
                assertThat(returnedSubject, hasProperty("key", equalTo(ksPair.getRight().getKey())));
            }
        });
    }

    @Test
    public void getSubjectByNameOrKeyWhenNoAuthenticationHeader() {
        List<Pair<String, Subject>> testData = ImmutableList.of(
                ImmutablePair.of("u1", null),
                ImmutablePair.of("u2", createSubject("user:u1", "u1", () -> new User())),
                ImmutablePair.of("user:u2", createSubject("user:u2", "u2", () -> new User()))
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

    private Subject createSubject(String key, String name, Supplier<Subject> subjectSupplier) {
        Subject s = subjectSupplier.get();
        s.setKey(key);
        s.setName(name);
        return s;
    }

    private void prepareTestData(List<Pair<String, Subject>> keyToSubjectMapping) {
        keyToSubjectMapping.forEach(keySubjectPair -> {
            Mockito.when(dependenciesProducer.getSubjectDao().findByNameOrKey(keySubjectPair.getLeft())).thenReturn(keySubjectPair.getRight());
        });
        Mockito.when(dependenciesProducer.getLegacyDomainDao().getSubjectByNameOrKey(TEST_USERNAME))
                .thenReturn(createSubject(TEST_USER_KEY, TEST_USERNAME, () -> new User()));
    }
}

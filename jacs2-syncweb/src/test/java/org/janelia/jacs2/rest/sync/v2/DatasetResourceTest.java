package org.janelia.jacs2.rest.sync.v2;

import org.hamcrest.Matchers;
import org.janelia.model.domain.sample.DataSet;
import org.janelia.model.security.User;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class DatasetResourceTest extends AbstractSyncServicesAppResourceTest {

    private static final int N_TEST_USERS = 5;
    private static final int N_TEST_DATASETS_PER_USER = 2;
    private static final String TEST_USERNAME = "test";
    private static final String TEST_USER_KEY = "user:" + TEST_USERNAME;

    @Test
    public void getSageSyncedDataSetsAsXml() {
        List<String> testOwners = IntStream.range(0, N_TEST_USERS).mapToObj(i -> TEST_USER_KEY + i ).collect(Collectors.toList());
        boolean testSageSyncFlag = true;
        List<DataSet> testDataSets = prepareSageSyncedDatasetTest(testOwners, testSageSyncFlag);
        Response testResponse = target()
                .path("data/dataSet/sage")
                .queryParam("owners", (Object[]) testOwners.toArray(new String[0]))
                .queryParam("sageSync", testSageSyncFlag)
                .request(MediaType.APPLICATION_XML)
                .header("username", TEST_USERNAME)
                .get();
        assertEquals(200, testResponse.getStatus());
        String xmlResult = testResponse.readEntity(String.class);
        String innerDataSetListXml = testDataSets.stream()
                .map(ds -> String.format("<dataSet><name>%1$s</name>%4$s<user>%2$s</user><dataSetIdentifier>%3$s</dataSetIdentifier></dataSet>",
                        ds.getName(), ds.getOwnerKey(), ds.getIdentifier(), ds.isSageSync() ? "<sageSync>SAGE Sync</sageSync>" : "<sageSync/>"))
                .reduce("", (s1, s2) -> s1 + s2);
        assertThat(xmlResult, Matchers.equalTo("<dataSetList>" + innerDataSetListXml + "</dataSetList>"));
    }

    @Test
    public void getSageSyncedDataSetsAsJson() {
        List<String> testOwners = IntStream.range(0, N_TEST_USERS).mapToObj(i -> TEST_USER_KEY + i ).collect(Collectors.toList());
        boolean testSageSyncFlag = true;
        List<DataSet> testDataSets = prepareSageSyncedDatasetTest(testOwners, testSageSyncFlag);
        Response testResponse = target()
                .path("data/dataSet/sage")
                .queryParam("owners", (Object[]) testOwners.toArray(new String[0]))
                .queryParam("sageSync", testSageSyncFlag)
                .request(MediaType.APPLICATION_JSON)
                .header("username", TEST_USERNAME)
                .get();
        assertEquals(200, testResponse.getStatus());
        String jsonResult = testResponse.readEntity(String.class);
        String innerDataSetList = testDataSets.stream()
                .map(ds -> String.format("{\"name\":\"%1$s\",\"sageSync\":%4$s,\"user\":\"%2$s\",\"dataSetIdentifier\":\"%3$s\"}",
                        ds.getName(), ds.getOwnerKey(), ds.getIdentifier(), ds.isSageSync() ? "\"SAGE Sync\"": null))
                .reduce(null, (s1, s2) -> s1 == null ? s2 : s1 + "," + s2);

        assertThat(jsonResult, Matchers.equalTo("{\"dataSetList\":[" + innerDataSetList + "]}"));
    }

    private List<DataSet> prepareSageSyncedDatasetTest(List<String> testOwners, boolean testSageSyncFlag) {
        List<DataSet> testDataSets = testOwners.stream()
                .flatMap(userKey -> IntStream.range(0, N_TEST_DATASETS_PER_USER).mapToObj(i -> {
                    DataSet ds = new DataSet();
                    String dsName = userKey.replace(':', '_') + "_ds" + i;
                    ds.setOwnerKey(userKey);
                    ds.setIdentifier(dsName + "_id");
                    ds.setName(dsName + "_name");
                    ds.setSageSync(testSageSyncFlag);
                    return ds;
                }))
                .collect(Collectors.toList());

        User testUser = new User();
        testUser.setKey(TEST_USER_KEY);
        testUser.setName(TEST_USERNAME);
        Mockito.when(dependenciesProducer.getSubjectDao().findSubjectByNameOrKey(TEST_USERNAME))
                .thenReturn(testUser);

        Mockito.when(dependenciesProducer.getDatasetSearchableDao().getDatasetsByOwnersAndSageSyncFlag(testOwners, testSageSyncFlag))
                .thenReturn(testDataSets)
        ;
        return testDataSets;
    }
}

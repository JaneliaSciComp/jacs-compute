package org.janelia.jacs2.dataservice.search;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        SolrConnector.class,
        HttpSolrServer.class
})
public class SolrConnectorTest {

    @Test
    public void indexDocumentStream() throws Exception {
        class TestData {
            private final int batchSize;
            private TestData(int batchSize) {
                this.batchSize = batchSize;
            }
        }
        List<SolrInputDocument> testSolrDocs = Arrays.asList(
                createTestSolrDoc("1"),
                createTestSolrDoc("2"),
                createTestSolrDoc("3"),
                createTestSolrDoc("4"),
                createTestSolrDoc("5"),
                createTestSolrDoc("6"),
                createTestSolrDoc("7"),
                createTestSolrDoc("8"),
                createTestSolrDoc("9"),
                createTestSolrDoc("10"),
                createTestSolrDoc("11"),
                createTestSolrDoc("12")
        );
        TestData[] testData = new TestData[] {
                new TestData(-2),
                new TestData(0),
                new TestData(4),
                new TestData(5),
                new TestData(testSolrDocs.size()),
                new TestData(testSolrDocs.size() + 1)
        };
        HttpSolrServer testSolrServer = Mockito.mock(HttpSolrServer.class);
        int commitDelay = 100;
        SolrConnector solrConnector = createSolrConnector(testSolrServer, commitDelay);
        for (TestData td : testData) {
            solrConnector.addDocsToIndex(testSolrDocs.stream(), td.batchSize);
            int batchSize = Math.max(1, td.batchSize);
            int nInvocations;
            if (batchSize == 1 || testSolrDocs.size() % batchSize == 0) {
                nInvocations = testSolrDocs.size() / batchSize;
            } else {
                nInvocations = testSolrDocs.size() / batchSize + 1;
            }
            Mockito.verify(testSolrServer, times(nInvocations)).add(anyList(), eq(commitDelay));
            Mockito.reset(testSolrServer);
        }
    }

    @Test
    public void updateDocAncestors() throws Exception {
        class TestData {
            private final List<Long> childrenDocIds;
            private final Long ancestorDocId;
            private final int batchSize;
            private final List<String> expectedQueries;

            private TestData(List<Long> childrenDocIds, Long ancestorDocId, int batchSize, List<String> expectedQueries) {
                this.childrenDocIds = childrenDocIds;
                this.ancestorDocId = ancestorDocId;
                this.batchSize = batchSize;
                this.expectedQueries = expectedQueries;
            }
        }

        TestData[] testData = new TestData[] {
                new TestData(null, 10L, 0, Collections.emptyList()),
                new TestData(Arrays.asList(1L, 2L, 3L, 4L), 10L, 0, Arrays.asList("id:1 OR id:2 OR id:3 OR id:4")),
                new TestData(Arrays.asList(1L, 2L, 3L, 4L), 10L, 3, Arrays.asList("id:1 OR id:2 OR id:3", "id:4")),
                new TestData(Arrays.asList(1L, 2L, 3L, 4L), 10L, 2, Arrays.asList("id:1 OR id:2", "id:3 OR id:4"))
        };
        HttpSolrServer testSolrServer = Mockito.mock(HttpSolrServer.class);
        int commitDelay = 100;
        SolrConnector solrConnector = createSolrConnector(testSolrServer, commitDelay);
        for (TestData td : testData) {
            try {
                Mockito.when(testSolrServer.query(any(SolrQuery.class))).then(invocation -> {
                    QueryResponse testResponse = Mockito.mock(QueryResponse.class);
                    SolrQuery solrQuery = invocation.getArgument(0);
                    String query = solrQuery.getQuery();
                    SolrDocumentList solrDocumentList = new SolrDocumentList();
                    solrDocumentList.addAll(Splitter.on(" OR ").splitToList(query).stream().map(s -> {
                        SolrDocument doc = new SolrDocument();
                        doc.put("id", s);
                        return doc;
                    }).collect(Collectors.toList()));
                    Mockito.when(testResponse.getResults()).thenReturn(solrDocumentList);
                    return testResponse;
                });
            } catch (SolrServerException e) {
                fail(e.toString());
            }
            solrConnector.addAncestorIdToAllDocs(td.ancestorDocId, td.childrenDocIds, td.batchSize);
            td.expectedQueries.forEach(q -> {
                try {
                    Mockito.verify(testSolrServer).query(argThat((ArgumentMatcher<SolrQuery>) solrQuery -> solrQuery.getQuery().equals(q)));
                } catch (SolrServerException e) {
                    fail(e.toString());
                }
            });
            if (CollectionUtils.isEmpty(td.childrenDocIds)) {
                Mockito.verify(testSolrServer, never()).add(anyList(), eq(commitDelay));
            } else {
                int batchSize = Math.max(1, td.batchSize);
                int nInvocations;
                if (batchSize == 1 || td.childrenDocIds.size() % batchSize == 0) {
                    nInvocations = td.childrenDocIds.size() / batchSize;
                } else {
                    nInvocations = td.childrenDocIds.size() / batchSize + 1;
                }
                Mockito.verify(testSolrServer, times(nInvocations)).add(anyList(), eq(commitDelay));
            }
            Mockito.reset(testSolrServer);
        }
    }

    private SolrConnector createSolrConnector(HttpSolrServer testSolrServer, int commitDelay) {
        String testURL = "testURL";
        try {
            PowerMockito.whenNew(HttpSolrServer.class)
                    .withParameterTypes(String.class).withArguments(testURL)
                    .thenReturn(testSolrServer);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return new SolrConnector(testURL, commitDelay);
    }
    private SolrInputDocument createTestSolrDoc(String id) {
        SolrInputDocument solrDoc = new SolrInputDocument();
        solrDoc.setField("id", id);
        return solrDoc;
    }
}

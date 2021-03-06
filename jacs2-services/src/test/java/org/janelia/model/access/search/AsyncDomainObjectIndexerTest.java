package org.janelia.model.access.search;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.janelia.messaging.core.MessageSender;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        DomainUtils.class,
})
public class AsyncDomainObjectIndexerTest {
    private static class TestData {
        private final MessageSender messageSender;
        private final int expectedMessages;

        TestData(MessageSender messageSender, int expectedMessages) {
            this.messageSender = messageSender;
            this.expectedMessages = expectedMessages;
        }
    }

    @Test
    public void indexMultipleDocuments() {

        MessageSender connectedMessageSender = Mockito.mock(MessageSender.class);
        Mockito.when(connectedMessageSender.isConnected()).thenReturn(true);
        MessageSender unconnectedMessageSender = Mockito.mock(MessageSender.class);
        Mockito.when(unconnectedMessageSender.isConnected()).thenReturn(false);

        List<DomainObject> domainObjects = prepareDocumentsForIndexing(true);

        TestData[] testData = new TestData[] {
                new TestData(null, 0),
                new TestData(unconnectedMessageSender, 0),
                new TestData(connectedMessageSender, domainObjects.size())
        };
        for (TestData td : testData) {
            AsyncDomainObjectIndexer indexer = createIndexer(td.messageSender);
            assertEquals(td.expectedMessages, indexer.indexDocumentStream(domainObjects.stream()));
            if (td.messageSender != null) {
                Mockito.verify(td.messageSender, times(td.expectedMessages)).sendMessage(anyMap(), isNull());
            }
        }
    }

    @Test
    public void skipIndexingNonSearchableDocuments() {

        MessageSender connectedMessageSender = Mockito.mock(MessageSender.class);
        Mockito.when(connectedMessageSender.isConnected()).thenReturn(true);

        List<DomainObject> domainObjects = prepareDocumentsForIndexing(false);

        TestData[] testData = new TestData[] {
                new TestData(connectedMessageSender, 0)
        };
        for (TestData td : testData) {
            AsyncDomainObjectIndexer indexer = createIndexer(td.messageSender);
            assertEquals(td.expectedMessages, indexer.indexDocumentStream(domainObjects.stream()));
            Mockito.verify(td.messageSender, times(td.expectedMessages)).sendMessage(anyMap(), isNull());
        }
    }

    private List<DomainObject> prepareDocumentsForIndexing(boolean searchable) {
        PowerMockito.mockStatic(DomainUtils.class);

        return LongStream.rangeClosed(1, 10)
                .mapToObj(i -> {
                    DomainObject dObj = Mockito.mock(DomainObject.class);
                    Mockito.when(dObj.getId()).thenReturn(i);
                    Mockito.when(DomainUtils.isSearcheableType(dObj.getClass())).thenReturn(searchable);
                    return dObj;
                })
                .collect(Collectors.toList());
    }

    @Test
    public void removeMultipleDocuments() {

        MessageSender connectedMessageSender = Mockito.mock(MessageSender.class);
        Mockito.when(connectedMessageSender.isConnected()).thenReturn(true);
        MessageSender unconnectedMessageSender = Mockito.mock(MessageSender.class);
        Mockito.when(unconnectedMessageSender.isConnected()).thenReturn(false);

        List<Long> domainObjectIds = LongStream.rangeClosed(1, 10)
                .mapToObj(i -> i)
                .collect(Collectors.toList());
        TestData[] testData = new TestData[] {
                new TestData(null, 0),
                new TestData(unconnectedMessageSender, 0),
                new TestData(connectedMessageSender, domainObjectIds.size())
        };
        for (TestData td : testData) {
            AsyncDomainObjectIndexer indexer = createIndexer(td.messageSender);
            assertEquals(td.expectedMessages, indexer.removeDocumentStream(domainObjectIds.stream()));
            if (td.messageSender != null) {
                Mockito.verify(td.messageSender, times(td.expectedMessages)).sendMessage(anyMap(), isNull());
            }
        }
    }

    @Test
    public void updateDocsAncestors() {

        MessageSender connectedMessageSender = Mockito.mock(MessageSender.class);
        Mockito.when(connectedMessageSender.isConnected()).thenReturn(true);
        MessageSender unconnectedMessageSender = Mockito.mock(MessageSender.class);
        Mockito.when(unconnectedMessageSender.isConnected()).thenReturn(false);

        Long ancestorId = 100L;
        Set<Long> domainObjectIds = LongStream.rangeClosed(1, 10)
                .mapToObj(i -> i)
                .collect(Collectors.toSet());
        TestData[] testData = new TestData[] {
                new TestData(null, 0),
                new TestData(unconnectedMessageSender, 0),
                new TestData(connectedMessageSender, domainObjectIds.size())
        };
        for (TestData td : testData) {
            AsyncDomainObjectIndexer indexer = createIndexer(td.messageSender);
            indexer.updateDocsAncestors(domainObjectIds, ancestorId);
            if (td.messageSender != null) {
                Mockito.verify(td.messageSender, times(td.expectedMessages)).sendMessage(anyMap(), isNull());
            }
        }
    }

    private AsyncDomainObjectIndexer createIndexer(MessageSender messageSender) {
        return new AsyncDomainObjectIndexer(messageSender, null);
    }
}

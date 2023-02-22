package org.janelia.model.access.search;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;

public class AsyncDomainObjectIndexer implements DomainObjectIndexer {

    private final DomainObjectIndexer indexer;
    private final ExecutorService indexingExecutor;

    public AsyncDomainObjectIndexer(MessageSender messageSender, ExecutorService indexingExecutor) {
        this.indexer = new DomainObjectIndexSender(messageSender);
        this.indexingExecutor = indexingExecutor;
    }

    @Override
    public DocumentSearchResults searchIndex(DocumentSearchParams searchParams) {
        throw new UnsupportedOperationException("Search is not supported by the async indexer");
    }

    @Override
    public boolean indexDocument(DomainObject domainObject) {
        if (indexingExecutor != null) {
            indexingExecutor.submit(() -> indexer.indexDocument(domainObject));
            return false; // the actual result is in the future but there's no need to wait for it
        } else {
            return indexer.indexDocument(domainObject);
        }
    }

    @Override
    public boolean removeDocument(Long docId) {
        if (indexingExecutor != null) {
            indexingExecutor.submit(() -> indexer.removeDocument(docId));
            return false; // the actual result is in the future but there's no need to wait for it
        } else {
            return indexer.removeDocument(docId);
        }
    }

    @Override
    public int indexDocumentStream(Stream<? extends DomainObject> domainObjectStream) {
        if (indexingExecutor != null) {
            indexingExecutor.submit(() -> indexer.indexDocumentStream(domainObjectStream));
            return 0; // the actual result is in the future but there's no need to wait for it
        } else {
            return indexer.indexDocumentStream(domainObjectStream);
        }
    }

    @Override
    public int removeDocumentStream(Stream<Long> docIdsStream) {
        if (indexingExecutor != null) {
            indexingExecutor.submit(() -> indexer.removeDocumentStream(docIdsStream));
            return 0; // the actual result is in the future but there's no need to wait for it
        } else {
            return indexer.removeDocumentStream(docIdsStream);
        }
    }

    @Override
    public void removeIndex() {
        throw new UnsupportedOperationException("Remove index is not supported by the async indexer");
    }

    @Override
    public void updateDocsAncestors(Set<Long> docIds, Long ancestorId) {
        if (indexingExecutor != null) {
            indexingExecutor.submit(() -> indexer.updateDocsAncestors(docIds, ancestorId));
        } else {
            indexer.updateDocsAncestors(docIds, ancestorId);
        }
    }

    @Override
    public void commitChanges() {
        if (indexingExecutor != null) {
            indexingExecutor.submit(indexer::commitChanges);
        } else {
            indexer.commitChanges();
        }
    }
}

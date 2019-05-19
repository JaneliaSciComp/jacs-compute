package org.janelia.model.access.search;

import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class AsyncDomainObjectIndexer implements DomainObjectIndexer {

    private final MessageSender messageSender;

    public AsyncDomainObjectIndexer(MessageSender messageSender) {
        this.messageSender = messageSender;
    }

    @Override
    public DocumentSearchResults searchIndex(DocumentSearchParams searchParams) {
        throw new UnsupportedOperationException("Search is not supported by the async indexer");
    }

    @Override
    public boolean indexDocument(DomainObject domainObject) {
        if (messageSender != null) {
            Map<String, Object> messageHeaders = new LinkedHashMap<>();
            messageHeaders.put("msgType", "UPDATE_DOC");
            messageHeaders.put("objectId", domainObject.getId());
            messageHeaders.put("objectClass", domainObject.getClass().getName());
            messageSender.sendMessage(messageHeaders, null);
            return true;
        }
        return false;
    }

    @Override
    public int indexDocumentStream(Stream<? extends DomainObject> domainObjectStream) {
        return (int) domainObjectStream.map(this::indexDocument).count();
    }

    @Override
    public boolean removeDocument(Long docId) {
        if (messageSender != null) {
            Map<String, Object> messageHeaders = new LinkedHashMap<>();
            messageHeaders.put("msgType", "DELETE_DOC");
            messageHeaders.put("objectId", docId);
            messageSender.sendMessage(messageHeaders, null);
            return true;
        }
        return false;
    }

    @Override
    public int removeDocumentStream(Stream<Long> docIdsStream) {
        return (int) docIdsStream.map(this::removeDocument).count();
    }

    @Override
    public void removeIndex() {
        throw new UnsupportedOperationException("Remove index is not supported by the async indexer");
    }

    @Override
    public void updateDocsAncestors(Set<Long> docIds, Long ancestorId) {
        if (messageSender != null) {
            Map<String, Object> messageHeaders = new LinkedHashMap<>();
            messageHeaders.put("msgType", "ADD_ANCESTOR");
            messageHeaders.put("ancestorId", ancestorId);
            docIds.forEach(docId -> {
                messageHeaders.put("objectId", docId);
                messageSender.sendMessage(messageHeaders, null);
            });
        }
    }
}

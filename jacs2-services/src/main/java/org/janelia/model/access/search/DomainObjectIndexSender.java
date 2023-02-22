package org.janelia.model.access.search;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;

class DomainObjectIndexSender implements DomainObjectIndexer {

    private final MessageSender messageSender;

    DomainObjectIndexSender(MessageSender messageSender) {
        this.messageSender = messageSender;
    }

    @Override
    public DocumentSearchResults searchIndex(DocumentSearchParams searchParams) {
        throw new UnsupportedOperationException("Search is not supported by the async indexer");
    }

    @Override
    public boolean indexDocument(DomainObject domainObject) {
        if (isEnabled() && DomainUtils.isSearcheableType(domainObject.getClass())) {
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
    public boolean removeDocument(Long docId) {
        if (isEnabled()) {
            Map<String, Object> messageHeaders = new LinkedHashMap<>();
            messageHeaders.put("msgType", "DELETE_DOC");
            messageHeaders.put("objectId", docId);
            messageSender.sendMessage(messageHeaders, null);
            return true;
        }
        return false;
    }

    @Override
    public int indexDocumentStream(Stream<? extends DomainObject> domainObjectStream) {
        return (int) domainObjectStream.filter(d -> DomainUtils.isSearcheableType(d.getClass())).map(this::indexDocument).filter(r -> r).count();
    }

    @Override
    public int removeDocumentStream(Stream<Long> docIdsStream) {
        return (int) docIdsStream.map(this::removeDocument).filter(r -> r).count();
    }

    @Override
    public void removeIndex() {
        throw new UnsupportedOperationException("Remove index is not supported by the async indexer");
    }

    @Override
    public void updateDocsAncestors(Set<Long> docIds, Long ancestorId) {
        if (isEnabled()) {
            Map<String, Object> messageHeaders = new LinkedHashMap<>();
            messageHeaders.put("msgType", "ADD_ANCESTOR");
            messageHeaders.put("ancestorId", ancestorId);
            docIds.forEach(docId -> {
                messageHeaders.put("objectId", docId);
                messageSender.sendMessage(messageHeaders, null);
            });
        }
    }

    private boolean isEnabled() {
        return messageSender != null && messageSender.isConnected();
    }

    @Override
    public void commitChanges() {
        throw new UnsupportedOperationException("Explicit commit is not supported by the async indexer");
    }
}

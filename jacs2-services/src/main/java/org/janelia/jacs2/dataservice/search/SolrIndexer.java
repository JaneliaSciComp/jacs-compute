package org.janelia.jacs2.dataservice.search;

import com.google.common.collect.Streams;
import org.apache.solr.common.SolrInputDocument;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TreeNodeDao;
import org.janelia.model.access.domain.search.SolrConnector;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.support.SearchType;
import org.janelia.model.domain.workspace.NodeUtils;
import org.janelia.model.domain.workspace.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SolrIndexer {
    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexer.class);
    private static final int INDEXING_BATCH_SIZE = 200;

    private final LegacyDomainDao legacyDomainDao;
    private final TreeNodeDao treeNodeDao;
    private final SolrConnector solrConnector;

    @Inject
    SolrIndexer(LegacyDomainDao legacyDomainDao,
                TreeNodeDao treeNodeDao,
                SolrConnector solrConnector) {
        this.legacyDomainDao = legacyDomainDao;
        this.treeNodeDao = treeNodeDao;
        this.solrConnector = solrConnector;
    }

    public boolean indexDocument(DomainObject domainObject) {
        Set<Long> domainObjectAncestorsIds = new LinkedHashSet<>();
        NodeUtils.traverseAllAncestors(
                Reference.createFor(domainObject),
                nodeReference -> {
                    List<TreeNode> nodeAncestors = treeNodeDao.getNodeDirectAncestors(nodeReference);
                    return nodeAncestors.stream().map(n -> Reference.createFor(n)).collect(Collectors.toSet());
                },
                n -> domainObjectAncestorsIds.add(n.getTargetId()));
        return solrConnector.addDocToIndex(solrConnector.createSolrDoc(domainObject, domainObjectAncestorsIds));
    }

    @SuppressWarnings("unchecked")
    public boolean indexAllDocuments() {
        Set<Class<?>> searcheableClasses = DomainUtils.getDomainClassesAnnotatedWith(SearchType.class);
        Stream<SolrInputDocument> solrDocsStream = searcheableClasses.stream()
                .filter(clazz -> DomainObject.class.isAssignableFrom(clazz))
                .map(clazz -> (Class<? extends DomainObject>)clazz)
                .flatMap(domainClass -> streamAllDocsOfTypeToIndex(domainClass))
                ;
        return solrConnector.addDocsToIndex(solrDocsStream, INDEXING_BATCH_SIZE);
    }

    private <T extends DomainObject> Stream<SolrInputDocument> streamAllDocsOfTypeToIndex(Class<T> domainClass) {
        Iterator<T> domainObjectsItr = legacyDomainDao.iterateDomainObjects(domainClass);
        return Streams.stream(domainObjectsItr)
                .map(domainObject -> {
                    Set<Long> domainObjectAncestorsIds = new LinkedHashSet<>();
                    NodeUtils.traverseAllAncestors(
                            Reference.createFor(domainObject),
                            nodeReference -> {
                                List<TreeNode> nodeAncestors = treeNodeDao.getNodeDirectAncestors(nodeReference);
                                return nodeAncestors.stream().map(n -> Reference.createFor(n)).collect(Collectors.toSet());
                            },
                            n -> domainObjectAncestorsIds.add(n.getTargetId()));
                    return solrConnector.createSolrDoc(domainObject, domainObjectAncestorsIds);
                });
    }

    public void clearIndex() {
        solrConnector.clearIndex();
    }

    public boolean removeFromIndexById(String id) {
        return solrConnector.removeFromIndexById(id);
    }

    public void addAncestorIdToAllDocs(Long ancestorDocId, List<Long> solrDocIds) {
        solrConnector.addAncestorIdToAllDocs(ancestorDocId, solrDocIds, INDEXING_BATCH_SIZE);
    }

}

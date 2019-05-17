package org.janelia.jacs2.cdi;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServer;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.dataservice.search.DomainObjectIndexerConstructor;
import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.TreeNodeDao;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.access.domain.search.SolrBasedDomainObjectIndexer;
import org.janelia.model.access.search.AsyncDomainObjectIndexer;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.workspace.TreeNode;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class IndexingProducer {

    @ApplicationScoped
    @AsyncIndex
    @Produces
    public DomainObjectIndexer createDomainObjectIndexer(@AsyncIndex MessageSender mesageSender) {
        return new AsyncDomainObjectIndexer(mesageSender);
    }

    @Produces
    public DomainObjectIndexerConstructor<SolrServer> createSolrBasedDomainObjectIndexerProvider(TreeNodeDao treeNodeDao,
                                                                                                 @IntPropertyValue(name = "Solr.BatchSize", defaultValue = 20000) int solrBatchSize,
                                                                                                 @IntPropertyValue(name = "Solr.CommitDelayInMillis", defaultValue = 100) int solrCommitDelayInMillis) {
        return (SolrServer solrServer) -> new SolrBasedDomainObjectIndexer(solrServer,
                nodeReference -> {
                    List<TreeNode> nodeAncestors = treeNodeDao.getNodeDirectAncestors(nodeReference);
                    return nodeAncestors.stream().map(n -> Reference.createFor(n)).collect(Collectors.toSet());
                },
                solrBatchSize,
                solrCommitDelayInMillis)
                ;
    }
}

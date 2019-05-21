package org.janelia.jacs2.dataservice.search;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.model.access.domain.dao.TreeNodeDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.workspace.NodeAncestorsGetter;
import org.janelia.model.domain.workspace.NodeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedNodeAncestorsGetter implements NodeAncestorsGetter {

    private static final Logger LOG = LoggerFactory.getLogger(CachedNodeAncestorsGetter.class);

    private final TreeNodeDao treeNodeDao;
    private final Cache<String, Map<Reference, Set<Reference>>> nodeAncestorsMapCache;

    @Inject
    public CachedNodeAncestorsGetter(TreeNodeDao treeNodeDao) {
        this.treeNodeDao = treeNodeDao;
        nodeAncestorsMapCache = CacheBuilder.newBuilder()
                .expireAfterWrite(10, TimeUnit.HOURS)
                .build();
    }

    @Override
    public Set<Reference> getNodeAncestors(Reference nodeReference) {
        return getNodeAncestorMap().get(nodeReference);
    }

    private Map<Reference, Set<Reference>> getNodeAncestorMap() {
        try {
            return nodeAncestorsMapCache.get("ancestorMapCache", () -> loadCache());
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<Reference, Set<Reference>> loadCache() {
        LOG.info("Start loading node ancestors cache");
        try {
            Map<Reference, Set<Reference>> directAncestors =
                    treeNodeDao.streamAll()
                            .flatMap(tn -> {
                                if (tn.hasChildren()) {
                                    return tn.getChildren().stream().map(childRef -> ImmutablePair.of(childRef, Reference.createFor(tn)));
                                } else {
                                    return Stream.of();
                                }
                            })
                            .collect(Collectors.groupingBy(ancestorNodePair -> ancestorNodePair.getLeft(),
                                    Collectors.mapping(ancestorNodePair -> ancestorNodePair.getRight(), Collectors.toSet())));
            Map<Reference, Set<Reference>> allAncestorsMap = new HashMap<>();
            directAncestors.forEach((node, nodeDirectAncestors) -> {
                Set<Reference> nodeAncestors = new HashSet<>();
                NodeUtils.traverseAllAncestors(node, directAncestors::get, ref -> nodeAncestors.add(ref));
                allAncestorsMap.put(node, nodeAncestors);
            });
            return allAncestorsMap;
        } finally {
            LOG.info("Finished loading node ancestors cache");
        }
    }
}

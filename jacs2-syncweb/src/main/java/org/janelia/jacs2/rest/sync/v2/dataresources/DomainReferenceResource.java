package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.domain.dao.NodeDao;
import org.janelia.model.access.domain.dao.ReferenceDomainObjectReadDao;
import org.janelia.model.access.domain.nodetools.DirectNodeAncestorsGetter;
import org.janelia.model.access.domain.nodetools.DirectNodeAncestorsGetterImpl;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.workspace.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web service for handling Node entities.
 */
@Tag(name = "DomainReference", description = "Data Node Service")
@RequireAuthentication
@Path("/reference")
public class DomainReferenceResource {

    private static final Logger LOG = LoggerFactory.getLogger(DomainReferenceResource.class);

    @Inject
    private ReferenceDomainObjectReadDao referenceDomainObjectReadDao;
    @Any
    @Inject
    private Instance<NodeDao<? extends Node>> nodeDaos;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{classname}/{id}")
    public <T extends DomainObject> Response getDataNode(@PathParam("classname") String classname,
                                                         @PathParam("id") String id) {
        T domainData = referenceDomainObjectReadDao.findByReference(Reference.createFor(classname + "#" + id));
        if (domainData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        return Response
                .ok(domainData)
                .build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/direct_ancestors")
    public Response getNodeDirectAncestors(@Parameter @QueryParam("subjectKey") String subjectKey,
                                           @Parameter @QueryParam("nodeRef") String nodeReferenceParam) {
        LOG.trace("Start getNodeDirectAncestors({}, {})", subjectKey, nodeReferenceParam);
        try {
            Reference nodeReference = Reference.createFor(nodeReferenceParam);
            Set<Reference> directAncestors = nodeDaos.stream()
                    .flatMap(nodeDao -> {
                        DirectNodeAncestorsGetter<? extends Node> nodeAncestorsGetter = new DirectNodeAncestorsGetterImpl<>(nodeDao);
                        Set<Reference> ancestors = nodeAncestorsGetter.getDirectAncestors(nodeReference);
                        LOG.debug("Ancestors for {} using {}: {}", nodeReference, nodeDao, ancestors);
                        return ancestors.stream();
                    })
                    .collect(Collectors.toSet())
                    ;
            return Response.ok()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new GenericEntity<Set<Reference>>(directAncestors){})
                    .build();
        } finally {
            LOG.trace("Finished getNodeDirectAncestors({}, {})", subjectKey, nodeReferenceParam);
        }
    }

}

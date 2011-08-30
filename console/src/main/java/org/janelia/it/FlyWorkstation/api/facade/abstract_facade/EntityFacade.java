package org.janelia.it.FlyWorkstation.api.facade.abstract_facade;

import org.janelia.it.jacs.model.entity.Entity;
import org.janelia.it.jacs.model.entity.EntityData;
import org.janelia.it.jacs.model.entity.EntityType;

import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: saffordt
 * Date: 7/25/11
 * Time: 3:52 PM
 */
public interface EntityFacade {
    public List<EntityType> getEntityTypes();

    public Entity getEntityById(String entityId) throws Exception;

    public Entity getEntityTree(Long entityId) throws Exception;

    public Entity getCachedEntityTree(Long entityId) throws Exception;

    public List<Entity> getEntitiesByName(String entityName);

    public List<Entity> getCommonRootEntitiesByTypeName(String entityTypeName);

    public Set<Entity> getChildEntities(Long parentEntityId);

    public List<EntityData> getParentEntityDatas(Long childEntityId);

    public List<Entity> getEntitiesByTypeName(String entityTypeName);

    public EntityData saveEntityDataForEntity(EntityData newData) throws Exception;

    public boolean deleteEntityById(Long entityId);

    public void deleteEntityTree(Long entityId) throws Exception;

    public Entity cloneEntityTree(Long entityId, String rootName) throws Exception;
}

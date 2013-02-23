package org.janelia.it.FlyWorkstation.model.domain;

import org.janelia.it.FlyWorkstation.api.entity_model.management.ModelMgr;
import org.janelia.it.FlyWorkstation.gui.framework.viewer.RootedEntity;
import org.janelia.it.jacs.model.entity.EntityConstants;
import org.janelia.it.jacs.model.entity.EntityData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Folder entity can contain entities of any other domain type. 
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class Folder extends EntityWrapper {

    private static final Logger log = LoggerFactory.getLogger(Folder.class);
    
    public Folder(RootedEntity rootedEntity) {
        super(rootedEntity);
    }    

    @Override
    public void loadContextualizedChildren(AlignmentContext alignmentContext) throws Exception {

        initChildren();
        ModelMgr.getModelMgr().loadLazyEntity(getInternalEntity(), false);
        
        // TODO: in the future, this should only show samples which have results in this context, but that's currently
        // too compute-intensive
        
        for(EntityData childEd : getInternalEntity().getOrderedEntityData()) {
            if (childEd.getChildEntity()==null) continue;
            try {
                EntityWrapper child = EntityWrapperFactory.wrap(getInternalRootedEntity().getChild(childEd));
                child.setParent(this);
                addChild(child);
            }
            catch (IllegalArgumentException e) {
                log.warn("Can't add child: "+childEd.getChildEntity().getName()+", "+e);
            }
        }       
    }

    public boolean isCommonRoot() {
        return getInternalEntity().getValueByAttributeName(EntityConstants.ATTRIBUTE_COMMON_ROOT)!=null;
    }
}

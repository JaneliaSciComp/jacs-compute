package org.janelia.it.FlyWorkstation.model.viewer;

import java.util.ArrayList;
import java.util.Collection;

import javax.swing.JOptionPane;

import org.janelia.it.FlyWorkstation.api.entity_model.management.ModelMgr;
import org.janelia.it.FlyWorkstation.gui.framework.session_mgr.SessionMgr;
import org.janelia.it.FlyWorkstation.gui.framework.viewer.alignment_board.AlignmentBoardEvent;
import org.janelia.it.FlyWorkstation.gui.framework.viewer.alignment_board.AlignmentBoardItemChangeEvent;
import org.janelia.it.FlyWorkstation.gui.framework.viewer.alignment_board.AlignmentBoardItemChangeEvent.ChangeType;
import org.janelia.it.FlyWorkstation.model.domain.*;
import org.janelia.it.FlyWorkstation.model.entity.RootedEntity;
import org.janelia.it.jacs.model.entity.Entity;
import org.janelia.it.jacs.model.entity.EntityConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlignmentBoardContext extends AlignedItem {

    private static final Logger log = LoggerFactory.getLogger(AlignmentBoardContext.class);

    private AlignmentContext context;
    
    public AlignmentBoardContext(RootedEntity rootedEntity) {
        super(rootedEntity);
        String as = getInternalEntity().getValueByAttributeName(EntityConstants.ATTRIBUTE_ALIGNMENT_SPACE);
        String ores = getInternalEntity().getValueByAttributeName(EntityConstants.ATTRIBUTE_OPTICAL_RESOLUTION);
        String pres = getInternalEntity().getValueByAttributeName(EntityConstants.ATTRIBUTE_PIXEL_RESOLUTION);
        this.context = new AlignmentContext(as, ores, pres);
    }

    @Override
    public void loadContextualizedChildren(AlignmentContext alignmentContext) throws Exception {

        log.debug("Loading contextualized children for alignment board '{}' (id={})",getName(),getId());
        initChildren();
        
        ModelMgr.getModelMgr().loadLazyEntity(getInternalEntity(), false);
        
        RootedEntity rootedEntity = getInternalRootedEntity();
        
        for(RootedEntity child : rootedEntity.getChildrenForAttribute(EntityConstants.ATTRIBUTE_ITEM)) {
            log.debug("Adding child item: {} (id={})",child.getName(),child.getId());
            AlignedItem item = new AlignedItem(child);
            addChild(item);
        }
    }

    public AlignmentContext getAlignmentContext() {
        return context;
    }

    private boolean verifyCompatability(String itemName, String alignmentSpaceName, String opticalResolution, String pixelResolution) {
        if (context==null) return true;
        
        if (!context.getAlignmentSpaceName().equals(alignmentSpaceName)) {
            JOptionPane.showMessageDialog(SessionMgr.getBrowser(),
                    "Neuron is not aligned to a compatible alignment space ("+context.getAlignmentSpaceName()+"!="+alignmentSpaceName+")", "Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        else if (!context.getOpticalResolution().equals(opticalResolution)) {
            JOptionPane.showMessageDialog(SessionMgr.getBrowser(),
                    "Neuron is not aligned to a compatible optical resolution ("+context.getOpticalResolution()+"!="+opticalResolution+")", "Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        else if (!context.getPixelResolution().equals(pixelResolution)) {
            JOptionPane.showMessageDialog(SessionMgr.getBrowser(),
                    "Neuron is not aligned to a compatible pixel resolution ("+context.getPixelResolution()+"!="+pixelResolution+")", "Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        
        return true;
    }
    
    /**
     * Add the given entities to the specified alignment board, if possible.
     * @param alignmentBoardContext
     * @param entitiesToAdd
     */
    public void addRootedEntity(RootedEntity rootedEntity) throws Exception {
    
        String type = rootedEntity.getType();
        
        if (EntityConstants.TYPE_SAMPLE.equals(type)) {
            Sample sample = (Sample)EntityWrapperFactory.wrap(rootedEntity);
            sample.loadContextualizedChildren(context);
            addNewAlignedEntity(sample);
        }
        else if (EntityConstants.TYPE_NEURON_FRAGMENT.equals(type)) {
            Entity sampleEntity = ModelMgr.getModelMgr().getAncestorWithType(rootedEntity.getEntity(), EntityConstants.TYPE_SAMPLE);
            Sample sample = (Sample)EntityWrapperFactory.wrap(new RootedEntity(sampleEntity));
            
            Entity separationEntity = ModelMgr.getModelMgr().getAncestorWithType(rootedEntity.getEntity(), EntityConstants.TYPE_NEURON_SEPARATOR_PIPELINE_RESULT);
            if (separationEntity==null) {
                throw new IllegalStateException("Neuron is not part of a neuron separation result");
            }
            
            Entity alignmentEntity = ModelMgr.getModelMgr().getAncestorWithType(separationEntity, EntityConstants.TYPE_ALIGNMENT_RESULT);
            if (alignmentEntity==null) {
                JOptionPane.showMessageDialog(SessionMgr.getBrowser(), "Neuron is not aligned", "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }
            
            String alignmentSpaceName = alignmentEntity.getValueByAttributeName(EntityConstants.TYPE_ALIGNMENT_SPACE);
            String opticalResolution = separationEntity.getValueByAttributeName(EntityConstants.ATTRIBUTE_OPTICAL_RESOLUTION);
            String pixelResolution = separationEntity.getValueByAttributeName(EntityConstants.ATTRIBUTE_PIXEL_RESOLUTION);
            if (!verifyCompatability(rootedEntity.getName(), alignmentSpaceName, opticalResolution, pixelResolution)) {
                return;
            }
            
            if (context==null) {
                this.context = new AlignmentContext(alignmentSpaceName, opticalResolution, pixelResolution);
            }
            
            sample.loadContextualizedChildren(context);
            
            for(Neuron neuron : sample.getNeuronSet()) {
                if (neuron.getId().equals(rootedEntity.getEntityId())) {
                    addNewAlignedEntity(neuron);
                    return;
                }
            }

            JOptionPane.showMessageDialog(SessionMgr.getBrowser(),
                    "Could not find neuron in the aligned neuron separation", "Error", JOptionPane.ERROR_MESSAGE);
        }
        else {
            throw new Exception("This entity cannot be viewed in the alignment board.");
        }
    }
    
    /**
     * Add a new aligned entity to the board. This method must be called from a worker thread.
     * 
     * @param wrapper to be added to the board
     * @throws Exception
     */
    public void addNewAlignedEntity(EntityWrapper wrapper) throws Exception {

        final Collection<AlignmentBoardEvent> events = new ArrayList<AlignmentBoardEvent>();
        
        if (wrapper instanceof Sample) {
            Sample parent = (Sample)wrapper;
            AlignedItem parentAlignedItem = getAlignedItemWithEntityId(parent.getId());

            if (parentAlignedItem==null) {
                if (parent.getChildren()==null) {
                    parent.loadContextualizedChildren(getAlignmentContext());
                }
                
                parentAlignedItem = ModelMgr.getModelMgr().addAlignedItem(this, parent);
                parentAlignedItem.loadContextualizedChildren(getAlignmentContext());
                
                for (Neuron child : parent.getNeuronSet()) {
                    AlignedItem neuronItem = ModelMgr.getModelMgr().addAlignedItem(parentAlignedItem, child);
                    neuronItem.loadContextualizedChildren(getAlignmentContext());
                    neuronItem.setIsVisible(true);
                }

                events.add(new AlignmentBoardItemChangeEvent(this, parentAlignedItem, ChangeType.Added));
            }
            else {
                events.add(new AlignmentBoardItemChangeEvent(this, parentAlignedItem, ChangeType.VisibilityChange));
            }
            
            parentAlignedItem.setIsVisible(true);
        }
        else if (wrapper instanceof Neuron) {
            events.addAll(handleChildWrapper(wrapper));
        }
        else if (wrapper instanceof CompartmentSet) {
            CompartmentSet parent = (CompartmentSet) wrapper;
            AlignedItem parentAlignedItem = getAlignedItemWithEntityId(parent.getId());
            if ( parentAlignedItem == null ) {
                if (parent.getChildren()==null) {
                    parent.loadContextualizedChildren(getAlignmentContext());
                }

                parentAlignedItem = ModelMgr.getModelMgr().addAlignedItem(this, parent);
                parentAlignedItem.loadContextualizedChildren(getAlignmentContext());

                for (Compartment child : parent.getCompartmentSet()) {
                    log.debug("Adding compartment {}.", child.getName());
                    AlignedItem alignedItem = ModelMgr.getModelMgr().addAlignedItem(parentAlignedItem, child);
                    alignedItem.loadContextualizedChildren(getAlignmentContext());
                    alignedItem.setIsVisible(true);
                }

                events.add(new AlignmentBoardItemChangeEvent(this, parentAlignedItem, ChangeType.Added) );
            }
            else {
                events.add(new AlignmentBoardItemChangeEvent(this, parentAlignedItem, ChangeType.VisibilityChange));
            }

            parentAlignedItem.setIsVisible( true );

        }
        else if (wrapper instanceof Compartment) {
            log.debug("Handling compartment " + wrapper.getName());
            events.addAll(handleChildWrapper(wrapper));
        }
        else {
            throw new IllegalStateException("Cannot add entity of type "+wrapper.getType()+" to the alignment board.");
        }

        for (AlignmentBoardEvent event : events) {
            ModelMgr.getModelMgr().postOnEventBus(event);
        }
    }
    
    private Collection<AlignmentBoardEvent> handleChildWrapper(EntityWrapper wrapper) throws Exception {
        
        Collection<AlignmentBoardEvent> events = new ArrayList<AlignmentBoardEvent>();
        
        EntityWrapper child = wrapper;
        EntityWrapper parent = wrapper.getParent();

        AlignedItem parentAlignedItem = getAlignedItemWithEntityId(parent.getId());
        if (parentAlignedItem==null) {
            parentAlignedItem = ModelMgr.getModelMgr().addAlignedItem(this, parent);
            parentAlignedItem.loadContextualizedChildren(getAlignmentContext());
            log.debug("No parent found for {}.", parent.getName());
        }
        else {
            log.debug("Found parent item for {}, of {}.", parent.getName(), parentAlignedItem.getName() );
        }

        parentAlignedItem.setIsVisible(true);

        AlignedItem childAlignedItem = parentAlignedItem.getAlignedItemWithEntityId(child.getId());
        if (childAlignedItem == null) {
            childAlignedItem = ModelMgr.getModelMgr().addAlignedItem(parentAlignedItem, child);
            childAlignedItem.loadContextualizedChildren(getAlignmentContext());
            events.add(new AlignmentBoardItemChangeEvent(this, childAlignedItem, ChangeType.Added));
        }
        else {
            events.add(new AlignmentBoardItemChangeEvent(this, childAlignedItem, ChangeType.VisibilityChange));
        }

        childAlignedItem.setIsVisible(true);
        
        return events;
    }

    /**
     * Removes an aligned entity from the board. This method must be called from a worker thread.
     * 
     * @param alignedItem
     * @throws Exception
     */
    public void removeAlignedEntity(final AlignedItem alignedItem) throws Exception {

        RootedEntity rootedEntity = alignedItem.getInternalRootedEntity();
        ModelMgr.getModelMgr().deleteEntityTree(rootedEntity.getEntityId());
        
        final AlignmentBoardItemChangeEvent event = new AlignmentBoardItemChangeEvent(
                this, alignedItem, ChangeType.Removed);

        ModelMgr.getModelMgr().postOnEventBus(event);
    }

}

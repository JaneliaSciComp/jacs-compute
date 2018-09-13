package org.janelia.jacs2.asyncservice.sample.helpers;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.sample.aux.AnatomicalArea;
import org.janelia.jacs2.asyncservice.sample.aux.SlideImage;
import org.janelia.jacs2.asyncservice.sample.aux.SlideImageGroup;
import org.janelia.jacs2.asyncservice.utils.FileUtil;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.nodes.FileStorePath;
import org.janelia.jacs2.utils.StringUtils;
import org.janelia.model.access.domain.ChanSpecUtils;
import org.janelia.model.access.domain.DomainObjectAttribute;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.access.domain.LockingDAO;
import org.janelia.model.domain.*;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.enums.PipelineStatus;
import org.janelia.model.domain.interfaces.HasFileGroups;
import org.janelia.model.domain.interfaces.HasFiles;
import org.janelia.model.domain.interfaces.HasRelativeFiles;
import org.janelia.model.domain.interfaces.HasResults;
import org.janelia.model.domain.ontology.Annotation;
import org.janelia.model.domain.sample.*;
import org.janelia.model.domain.support.ReprocessOnChange;
import org.janelia.model.domain.support.SAGEAttribute;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.util.SampleResultComparator;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Helper methods for dealing with Samples.
 * 
 * The methods here do no sample locking. It's the caller's responsibility to call lockSample and unlockSample
 * when calling any of the methods which mutate a sample. 
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("sampleHelper")
public class SampleHelper extends DomainHelper {

    private static final Logger log = LoggerFactory.getLogger(SampleHelper.class);

    private static final String DEFAULT_SAMPLE_NAME_PATTERN = "{Line}-{Slide Code}";

    private long writeLockTimeoutMs = 1000 * 10;

    @PropertyValue(name = "service.DefaultWorkingDir")
    private String centralDir;

    @Inject
    private LockingDAO lockingDao;

    @Inject
    private QuotaValidator quotaValidator;

    @Inject
    private ColorDepthFileUtils colorDepthFileUtils;

    // Inputs
    private List<DataSet> dataSets;
    private String dataSetNameFilter;
    private String process;
    private String orderNo;
    
    // Lookup tables
    private Map<String,SageField> lsmSageFields;
    private Map<String,SageField> sampleSageFields;
    private Map<String,DomainObjectAttribute> sampleAttrs;
    
    // Processing state
    private Map<Long,LSMImage> lsmCache = new HashMap<>();
    private Set<Long> reprocessLsmIds = new HashSet<>();
    private Set<Long> changedLsmIds = new HashSet<>();
    private Set<Long> changedSampleIds = new HashSet<>();
    private Set<String> sageAttrsNotFound = new HashSet<>();
    private int numSamplesCreated = 0;
    private int numSamplesUpdated = 0;
    private int numSamplesReprocessed = 0;
    private boolean debug = false;


    public void setDebugMode(boolean debug) {
        this.debug = debug;
    }
    
    public void setDataSetNameFilter(String dataSetNameFilter) {
        this.dataSetNameFilter = dataSetNameFilter;
    }

    public Sample getSample(Long sampleId) {
        return domainDao.getDomainObject(getOwnerKey(), Sample.class, sampleId);
    }

    private Long getRootServiceId() {
        JacsServiceData sd = currentService.getJacsServiceData();
        if (sd.getRootServiceId()!=null) return sd.getRootServiceId().longValue();
        if (sd.getParentServiceId()!=null) return sd.getParentServiceId().longValue();
        return sd.getId().longValue();
    }

    private Long getServiceId() {
        JacsServiceData sd = currentService.getJacsServiceData();
        return sd.getId().longValue();
    }

    public SampleLock lockSample(Long sampleId, String description) {
        Long lockingTaskId = getRootServiceId().longValue();
        return domainDao.lockSample(getOwnerKey(), sampleId, lockingTaskId, description);
    }
    
    public boolean unlockSample(Long sampleId) {
        Long lockingTaskId = getRootServiceId().longValue();
        return domainDao.unlockSample(getOwnerKey(), sampleId, lockingTaskId);
    }
    
    public LSMImage createOrUpdateLSM(SlideImage slideImage) throws Exception {
        
    	log.debug("createOrUpdateLSM("+slideImage.getName()+")");
        boolean dirty = false;

        LSMImage lsm = findBestLsm(slideImage.getSageId(), slideImage.getDataSet());
        if (lsm==null) {
            lsm = new LSMImage();
            lsm.setFiles(new HashMap<FileType,String>());
            lsm.setSageSynced(true);
            reprocessLsmIds.add(lsm.getId());
            log.info("Created new LSM for SAGE image#"+slideImage.getSageId());
            dirty = true;
        }
        
        if (updateLsmAttributes(lsm, slideImage)) {
            lsm.setSageSynced(true);
            log.info("Updated LSM properties for "+slideImage.getName());
            dirty = true;
        }
        
        if (dirty) {
            lsm = saveLsm(lsm);
        }
        else if (!lsm.getSageSynced()) {
            log.info("Resynchronizing lsm "+lsm.getId());
            domainDao.updateProperty(getOwnerKey(), LSMImage.class, lsm.getId(), "sageSynced", true);
        }
        
        lsmCache.put(lsm.getId(), lsm);
        return lsm;
    }

    private LSMImage findBestLsm(Integer sageId, String dataSetIdentifier) {

        List<LSMImage> lsms = domainDao.getUserLsmsBySageId(getOwnerKey(), sageId);
        if (lsms.isEmpty()) {
            return null;
        }

        // Sort by descending creation date 
        Collections.sort(lsms, new Comparator<LSMImage>() {
            @Override
            public int compare(LSMImage o1, LSMImage o2) {
                return o2.getCreationDate().compareTo(o1.getCreationDate());
            }
        });
        
        LSMImage best = null;

        // If there is an active LSM with a matching data set, choose that
        for(LSMImage lsm : lsms) {
            if (lsm.getSageSynced() && StringUtils.areEqual(lsm.getDataSet(), dataSetIdentifier)) {
                if (best == null) best = lsm;
            }
        }

        // If there is an desync LSM with a matching data set, choose that
        for(LSMImage lsm : lsms) {
            if (StringUtils.areEqual(lsm.getDataSet(), dataSetIdentifier)) {
                if (best == null) best = lsm;
            }
        }
        
        // Desync every other LSM with this SAGE id that wasn't chosen
        for(LSMImage lsm : lsms) {
            if (best==null || !lsm.getId().equals(best.getId())) {
                try {
                    log.info("Desynchronized conflicting LSM "+lsm.getId());
                    domainDao.updateProperty(getOwnerKey(), LSMImage.class, lsm.getId(), "sageSynced", false);
                    log.info("Desynchronized conflicting LSM's Sample "+lsm.getSample().getTargetId());
                    domainDao.updateProperty(getOwnerKey(), Sample.class, lsm.getSample().getTargetId(), "sageSynced", false);
                }
                catch (Exception e) {
                    log.warn("Problem desynchrozing LSM "+lsm.getId(), e);
                }
            }
        }

        // Return the chosen one
        return best;
    }

    private boolean updateLsmAttributes(LSMImage lsm, SlideImage slideImage) throws Exception {

    	log.debug("updateLsmAttribute(lsmId="+lsm.getId()+",sageId="+slideImage.getSageId()+")");
        boolean changed = false;
        boolean dirty = false;
        
        Map<String,SageField> lsmSageAttrs = getLsmSageFields();
        for(String key : slideImage.getProperties().keySet()) {
            try {
                SageField sageField = lsmSageAttrs.get(key);
                if (sageField==null) {
                	if (!sageAttrsNotFound.contains(key)) {
                		log.trace("SAGE Attribute not found on LSMImage: "+key);
                		sageAttrsNotFound.add(key);
                	}
                	continue;
                }
                Object value = slideImage.getProperties().get(key);

                String strValue = value==null?null:value.toString();
                Object trueValue = null;
                if (value!=null) {
                    Class<?> fieldType = sageField.field.getType();
                    // Convert the incoming value from SAGE to the correct type in our domain model
                    if (fieldType.equals(String.class)) {
                        trueValue = value.toString();
                    }
                    else if (fieldType.equals(Date.class)) {
                        // Dates are represented as java.sql.Timestamps, which is a subclass of Date, 
                        // so this should be safe to assign directly
                        trueValue = value;
                    }
                    else if (fieldType.equals(Long.class)) {
                    	if (value instanceof Long) {
                    		trueValue = value;
                    	}
                    	else {
                            if (!StringUtils.isEmpty(strValue)) {
                            	trueValue = new Long(strValue);
                            }
                    	}
                    }
                    else if (fieldType.equals(Integer.class)) {
                    	if (value instanceof Integer) {
                    		trueValue = value;
                    	}
                    	else {
                            if (!StringUtils.isEmpty(strValue)) {
                            	trueValue = new Integer(strValue);
                            }
                    	}
                    }
                    else if (fieldType.equals(Boolean.class)) {
                        if (value instanceof Boolean) {
                            trueValue = value;
                        }
                        else if (value instanceof Integer) {
                            trueValue = new Boolean(((Integer)value)!=0);
                        }
                        else {
                            if (!StringUtils.isEmpty(strValue)) {
                            	trueValue = new Boolean(strValue);
                            }
                        }
                    }
                    else {
                        // This might take care of future types we may not have anticipated
                        trueValue = value;
                    }
                }

                UpdateType ut = updateValue(lsm, sageField.field.getName(), sageField, trueValue);
                if (ut != UpdateType.SAME) {
                    dirty = true;
                }
                if (ut == UpdateType.CHANGE) {
                    changed = true;

                    // check if this attribute triggers a reprocess.
                    Field lsmField = lsm.getClass().getDeclaredField(sageField.field.getName());
                    if (lsmField!=null) {
                        ReprocessOnChange reprocessAttr = lsmField.getAnnotation(ReprocessOnChange.class);
                        if (reprocessAttr!=null) {
                            reprocessLsmIds.add(lsm.getId());
                        }
                    }
                }
            }
            catch (Exception e) {
                log.error("Error setting SAGE attribute value "+key+" for LSM#"+lsm.getId(),e);
            }
        }

        // Other attributes which are not automatically populated using @SAGEAttribute
        
        if (!StringUtils.areEqual(lsm.getName(), slideImage.getName())) {
            lsm.setName(slideImage.getName());
            dirty = true;
            changed = true;
        }
        
        String filepath = slideImage.getFilepath();
        if (!StringUtils.areEqual(lsm.getFilepath(), filepath)) {
            lsm.setFilepath(filepath);
            lsm.getFiles().put(FileType.LosslessStack,filepath);
            dirty = true;
            changed = true;
        }

        String objective = slideImage.getObjective();
        if (!StringUtils.areEqual(lsm.getObjective(), objective)) {
            lsm.setObjective(objective);
            dirty = true;
            changed = true;
        }
        
        if (lsm.getVoxelSizeX()!=null && lsm.getVoxelSizeY()!=null && lsm.getVoxelSizeZ()!=null) {
            String opticalRes = lsm.getVoxelSizeX()+"x"+lsm.getVoxelSizeY()+"x"+lsm.getVoxelSizeZ();
            if (!StringUtils.areEqual(lsm.getOpticalResolution(), opticalRes)) {
                lsm.setOpticalResolution(opticalRes);
                dirty = true;
                changed = true;
            }
        }

        if (lsm.getDimensionX()!=null && lsm.getDimensionY()!=null && lsm.getDimensionZ()!=null) {
            String imageSize = lsm.getDimensionX()+"x"+lsm.getDimensionY()+"x"+lsm.getDimensionZ();
            if (!StringUtils.areEqual(lsm.getImageSize(), imageSize)) {
                lsm.setImageSize(imageSize);
                dirty = true;
                changed = true;
            }
        }

        if (lsm.getAnatomicalArea()==null) {
            lsm.setAnatomicalArea("");
            dirty = true;
        }


        if (changed) {
            changedLsmIds.add(lsm.getId());
        }

        return dirty;
    }
        
    public Sample createOrUpdateSample(String slideCode, DataSet dataSet, Collection<LSMImage> lsms) throws Exception {

    	log.info("Creating or updating sample: "+slideCode+" ("+(dataSet==null?"":"dataSet="+dataSet.getIdentifier())+")");
    	
        Multimap<String,SlideImageGroup> objectiveGroups = HashMultimap.create();
    	boolean lsmAdded = false;
        int tileNum = 0;
        for(LSMImage lsm : lsms) {

        	// Have any of the LSMs been changed significantly? If so, we need to mark the sample for reprocessing later.
        	if (reprocessLsmIds.contains(lsm.getId())) {
                lsmAdded = true;
        	}
        	
        	// Extract LSM metadata
        	String objective = lsm.getObjective();
            if (objective==null) {
                objective = "";
            }
            String tag = lsm.getTile();
            if (tag==null) {
                tag = "Tile "+(tileNum+1);
            }
            String area = lsm.getAnatomicalArea();

            // Group LSMs by objective, tile and area
            Collection<SlideImageGroup> subTileGroupList = objectiveGroups.get(objective);
            SlideImageGroup group = null;
            for (SlideImageGroup slideImageGroup : subTileGroupList) {
            	if (StringUtils.areEqual(slideImageGroup.getTag(), tag) && StringUtils.areEqual(slideImageGroup.getAnatomicalArea(), area)) {
            		group = slideImageGroup;
            		break;
            	}
            }
            if (group==null) {
            	group = new SlideImageGroup(area, tag);
            	objectiveGroups.put(objective, group);
            }
            group.addFile(lsm);
            
            tileNum++;
        }
        
        log.debug("  Sample objectives: "+objectiveGroups.keySet());

        Sample sample = null;
        boolean sampleNew = false;
        
        try {    
            sample = getOrCreateSample(slideCode, dataSet);
            sampleNew = sample.getId()==null;
            
            boolean sampleDirty = sampleNew;
    
            if (setSampleAttributes(dataSet, sample, objectiveGroups.values())) {
                sampleDirty = true;
            }
    
            // marks Samples that have been changed (lsm added/removed)
            boolean needsReprocessing = lsmAdded;
    
            if (lsmAdded && !sampleNew) {
                log.info("  LSMs modified significantly, will mark sample for reprocessing");
            }
    
            if (changedSampleIds.contains(sample.getId()) && !sampleNew) {
                log.info("  Sample attributes changed, will mark sample for reprocessing");
            }
    
            // First, remove all tiles/LSMSs from objectives which are no longer found in SAGE
            for(ObjectiveSample objectiveSample : new ArrayList<>(sample.getObjectiveSamples())) {
            	if (!objectiveGroups.containsKey(objectiveSample.getObjective())) {
    
                    if ("".equals(objectiveSample.getObjective()) && objectiveGroups.size()==1) {
                        log.warn("  Leaving empty objective alone, because it is the only one");
                        continue;
                    }
    
    	        	if (!objectiveSample.hasPipelineRuns()) {
                        log.warn("  Removing existing '"+objectiveSample.getObjective()+"' objective sample");
    	        		sample.removeObjectiveSample(objectiveSample);
    	        	}
    	        	else {
                        log.warn("  Resetting tiles for existing "+objectiveSample.getObjective()+" objective sample");
    	        		objectiveSample.setTiles(new ArrayList<SampleTile>());
    	        	}
    	            sampleDirty = true;
            	}
            }
            
            List<String> objectives = new ArrayList<>(objectiveGroups.keySet());
            Collections.sort(objectives);
            for(String objective : objectives) {
                Collection<SlideImageGroup> subTileGroupList = objectiveGroups.get(objective);
    
                log.info("  Processing objective '"+objective+"', tiles="+subTileGroupList.size());
                
                // Find the sample, if it exists, or create a new one.
                UpdateType ut = createOrUpdateObjectiveSample(sample, objective, subTileGroupList);
                if (ut!=UpdateType.SAME) {
                    sampleDirty = true;
                }
                if (ut==UpdateType.CHANGE && !sampleNew) {
                    log.info("  Objective sample '"+objective+"' changed, will mark sample for reprocessing");
                    needsReprocessing = true;
                }
            }
            
            if (!sample.getSageSynced()) {
                sample.setSageSynced(true);
                sampleDirty = true;
            }
    
            if (needsReprocessing) {
            	dispatchForProcessing(sample, false, PipelineStatus.Scheduled);
            	sampleDirty = true;
            }
            
            if (sampleDirty) {
                log.info("  Saving sample: "+sample.getName()+" (id="+sample.getId()+")");
                sample = saveSample(sample);
                if (!debug) {
                    domainDao.addSampleToOrder(orderNo, sample.getId());
                }
                numSamplesUpdated++;
            }
            else if (!sample.getSageSynced()) {
                log.info("Resynchronizing sample "+sample.getId());
                domainDao.updateProperty(getOwnerKey(), Sample.class, sample.getId(), "sageSynced", true);
            }
    
            // Update all back-references from the sample's LSMs
            Set<Long> includedLsmIds = new HashSet<>();
            Reference sampleRef = Reference.createFor(sample);
            List<Reference> lsmRefs = sample.getLsmReferences();
            for(Reference lsmRef : lsmRefs) {
                includedLsmIds.add(lsmRef.getTargetId());
            	LSMImage lsm = lsmCache.get(lsmRef.getTargetId());
            	if (lsm==null) {
            		log.warn("LSM (id="+lsmRef.getTargetId()+") not found in cache. This should never happen and indicates a bug.");
            		continue;
            	}
            	if (!StringUtils.areEqual(lsm.getSample(),sampleRef)) {
            		lsm.setSample(sampleRef);
            		saveLsm(lsm);
            		log.info("  Updated sample reference for LSM#"+lsm.getId());
            	}
            }
    
            // Desync all other LSMs that point to this sample
            for(LSMImage lsm : domainDao.getActiveLsmsBySampleId(sample.getOwnerKey(), sample.getId())) {
                if (!includedLsmIds.contains(lsm.getId())) {
                    log.info("Desynchronized obsolete LSM "+lsm.getId());
                    domainDao.updateProperty(getOwnerKey(), LSMImage.class, lsm.getId(), "sageSynced", false);
                }
            }
            
            if (sampleDirty) {
                // Update the permissions on the Sample and its LSMs and neuron fragments
                domainDao.addPermissions(dataSet.getOwnerKey(), Sample.class.getSimpleName(), sample.getId(), dataSet, true);
            }
            
            if (sampleNew) {
                // Index new samples immediately so that users can find them with search and check their progress
                IndexingHelper.sendReindexingMessage(sample);
            }
        }
        finally {
            try {
                if (!sampleNew && sample!=null) {
                    unlockSample(sample.getId());
                }
            }
            catch (Exception e) {
                log.error("Error unlocking sample", e);
            }
        }
        
        return sample;
    }

    private Sample getOrCreateSample(String slideCode, DataSet dataSet) {
        
        Sample sample = findBestSample(dataSet.getIdentifier(), slideCode);
        if (sample != null) {
        	log.info("  Found existing sample "+sample.getId()+" with status "+sample.getStatus());
            lockSample(sample.getId(), "Sample rediscovery");
        	return sample;
        }
        
        // If no matching samples were found, create a new sample
        sample = new Sample();
        sample.setDataSet(dataSet.getIdentifier());
        sample.setSlideCode(slideCode);
        sample.setStatus(PipelineStatus.New.getStatus());
        sample.setUnalignedCompressionType(dataSet.getUnalignedCompressionType());
        sample.setAlignedCompressionType(dataSet.getAlignedCompressionType());
        sample.setSeparationCompressionType(dataSet.getSeparationCompressionType());
        
    	log.info("  Creating new sample for "+dataSet.getIdentifier()+"/"+slideCode);
        numSamplesCreated++;

        return sample;
    }

    private Sample findBestSample(String dataSetIdentifier, String slideCode) {

        List<Sample> samples = domainDao.getUserSamplesBySlideCode(getOwnerKey(), dataSetIdentifier, slideCode);

        if (samples.isEmpty()) {
            return null;
        }

        // Sort by descending creation date 
        Collections.sort(samples, new Comparator<Sample>() {
            @Override
            public int compare(Sample o1, Sample o2) {
                return o2.getCreationDate().compareTo(o1.getCreationDate());
            }
        });
        
        Sample best = null;
        
        // If there is an active sample, return that
        for(Sample sample : samples) {
            if (sample.getSageSynced()) {
                if (best == null) best = sample;
            }
        }

        // Otherwise, return the most recently created Sample
        if (best == null) {
            best = samples.get(0);
        }

        // Desync every other Sample with this dataSet/slideCode that wasn't chosen
        for(Sample sample : samples) {
            if (best==null || !sample.getId().equals(best.getId())) {
                try {
                    log.info("Desynchronizing conflicting sample "+sample.getId());
                    domainDao.updateProperty(getOwnerKey(), Sample.class, sample.getId(), "sageSynced", false);
                }
                catch (Exception e) {
                    log.warn("Problem desynchronizing Sample"+sample.getId(), e);
                }
            }
        }
        
        return best;
    }

    private boolean setSampleAttributes(DataSet dataSet, Sample sample, Collection<SlideImageGroup> tileGroupList) {

        boolean dirty = false;
        boolean changed = false;
        Date maxTmogDate = null;

        Map<String,Object> consensusValues = new HashMap<>();
        Map<String,SageField> lsmSageAttrs = getLsmSageFields();
        Set<String> nonconsensus = new HashSet<>();
        
        for(SlideImageGroup tileGroup : tileGroupList) {
            for(LSMImage lsm : tileGroup.getImages()) {
                
                for(SageField lsmAttr : lsmSageAttrs.values()) {
                    String fieldName = lsmAttr.field.getName();
                    Class<?> fieldType = lsmAttr.field.getType();
                    Object value = null;
                    try {
                        value = org.janelia.model.util.ReflectionUtils.getFieldValue(lsm, lsmAttr.field);
                    }
                    catch (Exception e) {
                        log.error("  Problem getting value for LSMImage."+fieldName,e);
                    }
                    // Special consideration is given to the TMOG Date, so that the latest LSM TMOG date is recorded as the Sample TMOG date. 
                    if ("tmogDate".equals(fieldName)) {
                        Date date = (Date)value;
                        if (maxTmogDate==null || date.after(maxTmogDate)) {
                            maxTmogDate = date;
                        }
                    }
                    else if (!nonconsensus.contains(fieldName)) {
                        Object consensusValue = consensusValues.get(fieldName);
                        if (consensusValue==null) {
                            consensusValues.put(fieldName, value);
                        }
                        else if (!StringUtils.areEqual(consensusValue,value)) {
                            nonconsensus.add(fieldName);
                            consensusValues.put(fieldName, fieldType.equals(String.class)?DomainConstants.NO_CONSENSUS:null);
                        }    
                    }
                }
            }
        }

        if (maxTmogDate!=null) {
            consensusValues.put("tmogDate", maxTmogDate);
        }
        
        if (log.isTraceEnabled()) {
	        log.trace("  Consensus values: ");
	        for(String key : consensusValues.keySet()) {
	            Object value = consensusValues.get(key);
	            if (!DomainConstants.NO_CONSENSUS.equals(value) && value!=null) {
	                log.trace("    "+key+": "+value);
	            }
	        }
        }
        
        Map<String,SageField> sampleAttrs = getSampleSageFields();
        for(String fieldName : consensusValues.keySet()) {
        	SageField sampleAttr = sampleAttrs.get(fieldName);
            if (sampleAttr!=null) {
                try {
                    Object consensusValue = consensusValues.get(fieldName);
                    UpdateType ut = updateValue(sample, fieldName, sampleAttr, consensusValue);
                    if (ut != UpdateType.SAME) {
                        dirty = true;
                    }
                    if (ut == UpdateType.CHANGE) {
                        changed = true;
                    }
                }
                catch (Exception e) {
                    log.error("  Problem setting Sample."+fieldName,e);
                }
            }
        }
        
        String newName = getSampleName(dataSet, sample);
        if (!StringUtils.areEqual(sample.getName(),newName)) {
            log.info("  Updating sample name to: "+newName);
            sample.setName(newName);
            dirty = true;
            changed = true;
        }

        if (changed) {
            changedSampleIds.add(sample.getId());
        }

        return dirty;
    }

    /**
     * Create a new name for a sample, given the sample's attributes.
     * {Line}-{Slide Code}-Right_Optic_Lobe
     * {Line}-{Slide Code}-Left_Optic_Lobe
     * {VT line|Line}-{Slide Code}-Left_Optic_Lobe
     * {Line}-{Effector}-{Age}
     */
    public String getSampleName(DataSet dataSet, Sample sample) {
        
    	Map<String,DomainObjectAttribute> sampleAttrs = getSampleAttrs();
        Map<String,Object> valueMap = new HashMap<>();
        for(String key : sampleAttrs.keySet()) {
        	DomainObjectAttribute attr = sampleAttrs.get(key);
        	Object obj = null;
        	try {
        		obj = attr.getGetter().invoke(sample);
        	}
        	catch (Exception e) {
        		log.error("Error getting sample attribute value for: "+key,e);
        	}
        	if (obj!=null) {
        		valueMap.put(key, obj.toString());
        	}
        }

        String sampleNamePattern = dataSet==null?null:dataSet.getSampleNamePattern();
        String pattern = sampleNamePattern==null?DEFAULT_SAMPLE_NAME_PATTERN:sampleNamePattern;
        String name = StringUtils.replaceVariablePattern(pattern, valueMap);
        name = name.replaceAll(" ", ""); // Remove spaces, such as in "No Consensus"
        return name;
    }
    
    private UpdateType createOrUpdateObjectiveSample(Sample sample, String objective, Collection<SlideImageGroup> tileGroupList) throws Exception {

        ObjectiveSample objectiveSample = sample.getObjectiveSample(objective);
        if (objectiveSample==null) {
            objectiveSample = sample.getObjectiveSample("");
            if (objectiveSample==null) {
                objectiveSample = new ObjectiveSample(objective);
                sample.addObjectiveSample(objectiveSample);
                synchronizeTiles(objectiveSample, tileGroupList);
                log.debug("  Created new objective '"+objective+"' for sample "+sample.getName());
                return UpdateType.ADD;
            }
            else {
                objectiveSample.setObjective(objective);
                log.debug("  Updated objective to '"+objective+"' for legacy sample with empty objective");
                return UpdateType.ADD;
            }
        }
        else if (synchronizeTiles(objectiveSample, tileGroupList)) {
            return UpdateType.CHANGE;
        }

        return UpdateType.SAME;
    }

    public boolean synchronizeTiles(ObjectiveSample objectiveSample, Collection<SlideImageGroup> tileGroupList) throws Exception {

        if (objectiveSample.getTiles().isEmpty() || !tilesMatch(objectiveSample, tileGroupList)) {
            // Something has changed, so just recreate the tiles from scratch
            List<SampleTile> tiles = new ArrayList<>();
            for (SlideImageGroup tileGroup : tileGroupList) {
                SampleTile sampleTile = new SampleTile();
                sampleTile.setName(tileGroup.getTag());
                sampleTile.setAnatomicalArea(tileGroup.getAnatomicalArea());
                List<Reference> lsmReferences = new ArrayList<>();
                for(LSMImage lsm : tileGroup.getImages()) {
                    lsmReferences.add(Reference.createFor(lsm));
                    if (!lsm.getSageSynced()) {
                        log.info("  Resynchronizing LSM "+lsm.getId());
                        domainDao.updateProperty(getOwnerKey(), LSMImage.class, lsm.getId(), "sageSynced", true);
                    }
                }
                sampleTile.setLsmReferences(lsmReferences);
                tiles.add(sampleTile);
                log.debug("  Created tile "+sampleTile.getName()+" containing "+lsmReferences.size()+" LSMs");
            }
            objectiveSample.setTiles(tiles);
            log.info("  Updated "+tiles.size()+" tiles for objective '"+objectiveSample.getObjective()+"'");
            return true;
        }

        boolean dirty = false;

        for (SlideImageGroup tileGroup : tileGroupList) {
            SampleTile sampleTile = objectiveSample.getTileByNameAndArea(tileGroup.getTag(), tileGroup.getAnatomicalArea());
            if (sampleTile==null) {
            	throw new IllegalStateException("No such tile: "+tileGroup.getTag());
            }
            if (!StringUtils.areEqual(tileGroup.getAnatomicalArea(),sampleTile.getAnatomicalArea())) {
                sampleTile.setAnatomicalArea(tileGroup.getAnatomicalArea());
                log.info("  Updated anatomical area for tile "+sampleTile.getName()+" to "+sampleTile.getAnatomicalArea());
                dirty = true;
            }
        }
        
        return dirty;
    }
    
    public boolean tilesMatch(ObjectiveSample objectiveSample, Collection<SlideImageGroup> tileGroupList) throws Exception {
        
        Set<SampleTile> seenTiles = new HashSet<>();
        
        log.trace("  Checking if tiles match");
        
        for (SlideImageGroup tileGroup : tileGroupList) {
        	
        	log.trace("  Checking for "+tileGroup.getTag());

            // Ensure each tile is in the sample
            SampleTile sampleTile = objectiveSample.getTileByNameAndArea(tileGroup.getTag(), tileGroup.getAnatomicalArea());
            if (sampleTile==null) {
            	log.info("  Existing sample does not contain tile '"+tileGroup.getTag()+"' with anatomical area '"+tileGroup.getAnatomicalArea()+"'");
                return false;
            }
            seenTiles.add(sampleTile);
            
            Set<Long> lsmIds1 = new HashSet<>();
            for(LSMImage lsm : tileGroup.getImages()) {
                lsmIds1.add(lsm.getId());
            }

            Set<Long> lsmIds2 = new HashSet<>();
            for(Reference lsmReference : sampleTile.getLsmReferences()) {
                lsmIds2.add(lsmReference.getTargetId());
            }

            // Ensure each tiles references the correct LSMs
            if (!lsmIds1.equals(lsmIds2)) {
            	log.info("  LSM sets are not the same ("+lsmIds1+"!="+lsmIds2+").");
                return false;
            }
        }
        
        if (objectiveSample.getTiles().size() != seenTiles.size()) {
            // Ensure that the sample has no extra tiles it doesn't need
        	log.info("  Tile set sizes are not the same ("+objectiveSample.getTiles().size()+"!="+seenTiles.size()+").");
            return false;
        }
        
        log.trace("  Tiles match!");
        return true;
    }

    /**
     * Set the value of the given field name on the given object, if the new value is different from the current value.
     * @param object
     * @param fieldName
     * @param sageField
     * @param newValue
     * @return true if the value has changed
     * @throws Exception
     */
    private UpdateType updateValue(Object object, String fieldName, SageField sageField, Object newValue) throws Exception {
        Object currValue = org.janelia.model.util.ReflectionUtils.getFieldValue(object, sageField.field);
        if (!StringUtils.areEqual(currValue, newValue)) {
            org.janelia.model.util.ReflectionUtils.setFieldValue(object, sageField.field, newValue);

            if (currValue != null) {
                log.debug("  Setting " + fieldName + "='" + newValue+"' (previously '"+currValue+"')");
            }
            else {
                log.debug("  Setting " + fieldName + "='" + newValue+"'");
            }

            if (currValue == null) return UpdateType.ADD;
            else if (newValue == null) return UpdateType.REMOVE;
            else return UpdateType.CHANGE;
        }
        else {
            log.trace("  Already set "+fieldName+"="+newValue);
            return UpdateType.SAME;
        }
    }

    /**
     * Modify an existing sample's primary logical key (data set, slide code). Use this method carefully! It may trigger:
     * 1) Mass file moves on disk (if the new data set's owner is different)
     * 2) Updates to the Sample record
     * 3) Reprocessing of the sample (if the new data set uses a different pipeline)
     * 
     * The caller should lock the sample before calling this method. 
     *  
     * This method respects the "debug" mode setting on the SampleHelper. If debug mode is enabled (setDebugMode) then this method will
     * run as normal, but will refuse to actually persist any changes (e.g. to disk or to the database.)
     *  
     * @param sample the sample to be changed
     * @param newDataSet new data set (may be the same as the existing data set)
     * @param newSlideCode new slide code (may be the same as the existing slide code)
     * @throws Exception 
     */
    public Sample changePrimaryLogicalKey(Sample sample, String newDataSet, String newSlideCode, boolean dispatchIfNecessary) throws Exception {
                
        DataSet dataSet = null;
        if (newDataSet==null) {
            // New data set, same as the old data set
            dataSet = domainDao.getDataSetByIdentifier(null, sample.getDataSet());
        }
        else {
            dataSet = domainDao.getDataSetByIdentifier(null, newDataSet);
        }
        
        if (dataSet==null) {
            throw new IllegalArgumentException("No such data set: "+newDataSet);
        }
        
        String oldDataSet = sample.getDataSet();
        String oldSlideCode = sample.getSlideCode();
        String oldOwnerKey = sample.getOwnerKey();
        String newOwnerKey = dataSet.getOwnerKey();
        log.info("changePrimaryLogicalKey("+sample+
                ",oldDataSet="+oldDataSet+",oldSlideCode="+oldSlideCode+
                ",newDataSet="+dataSet.getIdentifier()+",newSlideCode="+newSlideCode+",dispatchIfNecessary="+dispatchIfNecessary+")");
                
        boolean sampleDirty = false;
        
        if (dataSet!=null) {
            sample.setDataSet(dataSet.getIdentifier());
            sampleDirty = true;
        }
        
        if (!StringUtils.isBlank(newSlideCode)) {
            sample.setSlideCode(newSlideCode);
            sampleDirty = true;
        }
        else {
            // New slide code, same as the old slide code
            newSlideCode = sample.getSlideCode();
        }
        
        String newName = getSampleName(dataSet, sample);
        if (StringUtils.areEqual(sample.getName(), newName)) {
            sample.setName(newName);    
            sampleDirty = true;
        }
        
        if (!oldOwnerKey.equals(newOwnerKey)) {
            changeOwner(sample, newOwnerKey);
            sampleDirty = true;
        }
        
        if (!oldDataSet.equals(newDataSet)) {
            if (verifyCDMLocations(sample, oldDataSet, newDataSet)) {
                log.info("Moved color depth MIPs to new data set location");
            }
        }

        // Also update the sample's LSMs and neurons, no matter who they are currently owned by (just in case the database was corrupted at some point)
        List<LSMImage> lsms = domainDao.getDomainObjectsAs(null, sample.getLsmReferences(), LSMImage.class);
        List<NeuronFragment> neurons = domainDao.getNeuronFragmentsBySampleId(null, sample.getId());
        
        // Ensure secondary files on are the correct disk location 
        if (verifySampleDiskLocation(sample, lsms, neurons)) {
            sampleDirty = true;
        }

        String savedOwnerKey = this.getOwnerKey();
        setOwnerKey(oldOwnerKey);
        Sample savedSample = sample;

        // Save the sample changes
        if (sampleDirty) {
            // The call to saveSample will return null because the ownerKey is changing
            // So we have to retrieve the sample manually, using the new ownerKey
            if (!debug) {
                saveSample(sample);
                savedSample = domainDao.getDomainObject(newOwnerKey, sample);
            }
            if (savedSample==null) {
                throw new IllegalStateException("Saving "+sample+" failed");
            }
        }
        
        log.info("Moved "+savedSample+" from "+
                oldDataSet+"/"+oldSlideCode+" to "+
                savedSample.getDataSet()+"/"+sample.getSlideCode());

        for (LSMImage lsm : lsms) {
            
            boolean lsmDirty = sampleDirty;
            
            // Update the owner
            String oldLsmOwnerKey = lsm.getOwnerKey();
            if (!oldLsmOwnerKey.equals(newOwnerKey)) {
                changeOwner(lsm, newOwnerKey);
                lsmDirty = true;
            }
            
            // Update attributes
            String oldLsmDataSet = lsm.getDataSet();
            if (!StringUtils.areEqual(oldLsmDataSet, dataSet.getIdentifier())) {
                lsm.setDataSet(dataSet.getIdentifier());
                lsmDirty = true;
            }

            String oldLsmSlideCode = lsm.getSlideCode();
            if (!StringUtils.areEqual(oldLsmSlideCode, newSlideCode)) {
                lsm.setSlideCode(newSlideCode);
                lsmDirty = true;
            }

            if (lsmDirty) {
                if (!debug) {
                    saveLsm(lsm);
                    lsm = domainDao.getDomainObject(newOwnerKey, lsm);
                }
            }
            log.info("Moved "+lsm+" from "+
                    oldLsmDataSet+"/"+oldLsmSlideCode+" to "+
                    lsm.getDataSet()+"/"+lsm.getSlideCode());
        }
        
        for (NeuronFragment neuronFragment : neurons) {

            boolean neuronDirty = sampleDirty;
            String oldNeuronOwnerKey = neuronFragment.getOwnerKey();
            
            // Update the owner
            if (!oldNeuronOwnerKey.equals(newOwnerKey)) {
                changeOwner(neuronFragment, newOwnerKey);
                neuronDirty = true;
            }
            
            if (neuronDirty) {
                if (!debug) {
                    saveNeuron(neuronFragment);
                    neuronFragment = domainDao.getDomainObject(newOwnerKey, neuronFragment);
                }
            }
            log.info("Moved "+neuronFragment+" from "+
                    oldNeuronOwnerKey+" to "+ newOwnerKey);
        }
        
        // The database has been updated, so now we need to use the new owner key to make further changes
        setOwnerKey(newOwnerKey);

        // Update disk usage information
        updateDiskSpaceUsage(sample);
        if (!dataSet.getIdentifier().equals(oldDataSet)) {
            // Data set has changed, update both old and new
            domainDao.updateDataSetDiskspaceUsage(oldDataSet);
            domainDao.updateDataSetDiskspaceUsage(dataSet.getIdentifier());
        }
        
        if (dispatchIfNecessary) {
            // Check to see if the sample needs to be re-run
            DataSet oldDS = domainDao.getDataSetByIdentifier(null, oldDataSet);
            DataSet newDS = domainDao.getDataSetByIdentifier(null, newDataSet);
            final Set<String> s1 = new HashSet<>(oldDS.getPipelineProcesses());
            final Set<String> s2 = new HashSet<>(newDS.getPipelineProcesses());
            if (!s1.equals(s2)) { 
                log.info("Old data set used "+oldDS.getPipelineProcesses()+" but new data set uses "+newDS.getPipelineProcesses()+". Dispatching for rerun.");
                dispatchForProcessing(savedSample, false, PipelineStatus.Scheduled);
            }        
        }

        // Return ownerKey to what it was, mainly so that sample lock can 
        // be removed by the caller, but also because it's good manners.
        setOwnerKey(savedOwnerKey);
        
        return savedSample;
    }
    
    private boolean changeOwner(DomainObject domainObject, String newOwnerKey) {

        if (domainObject.getOwnerKey().equals(newOwnerKey) 
                && DomainUtils.hasReadAccess(domainObject, newOwnerKey) 
                && DomainUtils.hasWriteAccess(domainObject, newOwnerKey)) {
            // Nothing to do
            return false;
        }
        
        // Remove permissions from existing owner
        domainObject.getReaders().remove(domainObject.getOwnerKey());
        domainObject.getWriters().remove(domainObject.getOwnerKey());
        
        // Add new permissions
        domainObject.setOwnerKey(newOwnerKey);
        domainObject.getReaders().add(newOwnerKey);
        domainObject.getWriters().add(newOwnerKey);
        
        return true;
    }
    
    public Collection<FileStorePath> getFileStorePaths(Sample sample) {

        Set<FileStorePath> paths = new HashSet<>();
        for(ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {
            
            for(SamplePipelineRun run : objectiveSample.getPipelineRuns()) {
                for(PipelineResult result : run.getResults()) {
                    
                    paths.addAll(getFileStorePaths(result, true));

                    if (result instanceof HasFileGroups) {
                        HasFileGroups hasFileGroups = (HasFileGroups)result;
                        for (FileGroup fileGroup : hasFileGroups.getGroups()) {
                            paths.addAll(getFileStorePaths(fileGroup, false));
                        }
                    }
                }
            }
            
            for (SampleTile sampleTile : objectiveSample.getTiles()) {
                paths.addAll(getFileStorePaths(sampleTile, true));
            }
        }
        
        return paths;
    }
    
    private Collection<FileStorePath> getFileStorePaths(HasFiles hasFiles, boolean recurse) {

        log.trace("Finding file store paths in result: "+hasFiles.getClass().getSimpleName());
        Set<FileStorePath> paths = new HashSet<>();
        
        // Add the root path, if any
        if (hasFiles instanceof HasRelativeFiles) {
            HasRelativeFiles hasRelativeFiles = (HasRelativeFiles)hasFiles;
            if (hasRelativeFiles.getFilepath()!=null) {
                FileStorePath fsp = FileStorePath.parseFilepath(hasRelativeFiles.getFilepath());
                log.trace("  Adding root path: "+fsp);
                paths.add(fsp);
            }
        }

        // Add paths for all included files, which may begin with the root path or not
        for(FileType key : hasFiles.getFiles().keySet()) {
            String path = DomainUtils.getFilepath(hasFiles, key);
            FileStorePath fsp = FileStorePath.parseFilepath(path);
            log.trace("  Adding file path: "+fsp);
            paths.add(fsp);
        }

        if (recurse) {
            if (hasFiles instanceof HasResults) {
                for(PipelineResult result : ((HasResults)hasFiles).getResults()) {
                    log.trace("  Recursing into result "+result.getName());
                    getFileStorePaths(result, recurse);
                }
            }
        }

        return paths;
    }
    
    private boolean verifyCDMLocations(Sample sample, String oldDataSet, String newDataSet) throws Exception {
        
        int moved = 0;

        for(ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {
            for(SamplePipelineRun run : objectiveSample.getPipelineRuns()) {
                for(SampleAlignmentResult alignment : run.getAlignmentResults()) {
                    
                    Map<FileType, String> files = alignment.getFiles();
                    String cdm1 = files.get(FileType.ColorDepthMip1);
                    String cdm2 = files.get(FileType.ColorDepthMip2);
                    String cdm3 = files.get(FileType.ColorDepthMip3);
                    String cdm4 = files.get(FileType.ColorDepthMip4);
                    
                    moved += moveCDMIfNecessary(sample, oldDataSet, newDataSet, alignment, cdm1);
                    moved += moveCDMIfNecessary(sample, oldDataSet, newDataSet, alignment, cdm2);
                    moved += moveCDMIfNecessary(sample, oldDataSet, newDataSet, alignment, cdm3);
                    moved += moveCDMIfNecessary(sample, oldDataSet, newDataSet, alignment, cdm4);
                }
            }
        }
        
        return moved>0;
    }
    
    private int moveCDMIfNecessary(Sample sample, String oldDataSet, String newDataSet, SampleAlignmentResult alignment, String filename) throws IOException {
        
        if (filename==null) return 0;
        
        int moved = 0;
        
        File oldDataSetDir = colorDepthFileUtils.getDir(alignment.getAlignmentSpace(), oldDataSet);
        File newDataSetDir = colorDepthFileUtils.getDir(alignment.getAlignmentSpace(), newDataSet);
        File oldFile = new File(oldDataSetDir, filename);
        File newFile = new File(newDataSetDir, filename);
        
        if (oldFile.exists()) {
            if (newFile.exists()) {
                // For some reason the file exists in both locations. Delete it first, then move it.
                if (!debug) {
                    if (newFile.delete()) {
                        FileUtils.moveFile(oldFile, newFile);
                        moved++;
                    }
                    else {
                        throw new IOException("Could not delete target file: "+newFile);
                    }
                }
            }
            else {
                // This is the most common case, just move the file to the new location
                if (!debug) {
                    FileUtils.moveFile(oldFile, newFile);
                    moved++;
                }
            }
        }
        else {
            if (newFile.exists()) {
                log.warn("File already exists at correct location: "+newFile);
            }
            else {
                log.warn("CDM does not exist in CDM store: "+oldFile);
                // Try to get it from the alignment dir
                File originalFile = new File(alignment.getFilepath(), filename);
                if (originalFile.exists()) {
                    if (!debug) {
                        FileUtils.copyFile(originalFile, newFile);
                        moved++;
                    }
                }
                else {
                    log.warn("CDM does not exist in alignment dir: "+originalFile);
                }
            }
        }
     
        return moved;
    }
    
    /**
     * Ensures that all files which are part of the sample are in the sample owner's filestore location.
     * This moves files and updates the Sample object, but it's the caller's responsibility to save the Sample and LSMs.
     * @param sample Sample to verify
     * @param lsms The Sample's LSMs to be updated if they contain paths that moved
     * @return True if the Sample or LSMs were updated and need to be saved
     * @throws Exception
     */
    public boolean verifySampleDiskLocation(Sample sample, Collection<LSMImage> lsms, Collection<NeuronFragment> neurons) throws Exception {

        // Walk all the paths in the sample and if changing the owner changes the path, keep track of the mapping
        Map<String,String> nodePaths = new HashMap<>();
        for (FileStorePath fileStorePath : getFileStorePaths(sample)) {
            String userName = DomainUtils.getNameFromSubjectKey(sample.getOwnerKey());
            FileStorePath newPath = fileStorePath.withChangedOwner(userName);
            String oldPrefix = fileStorePath.getFilepath(true);
            String newPrefix = newPath.getFilepath(true);
            if (!oldPrefix.equals(newPrefix)) {
                log.debug("Will change: "+oldPrefix+" -> "+newPrefix);
                nodePaths.put(oldPrefix, newPrefix);   
            }
        }
        
        if (nodePaths.isEmpty()) {
            log.info("  All files are in their correct locations, nothing to do.");
            return false;
        }

        // Make sure the new owner has enough disk space to receive the sample
        quotaValidator.validate(sample.getOwnerKey());
        
        return moveArtifacts(sample, lsms, neurons, nodePaths);
    }

    public boolean moveArtifacts(Sample sample, Collection<LSMImage> lsms, Collection<NeuronFragment> neurons, Map<String,String> nodePaths) throws Exception {
        Set<String> moved = new HashSet<String>();
        
        log.debug("Moving sample artifacts");
        boolean updated = moveArtifacts(sample, nodePaths, moved);
        
        log.debug("Updating LSMs");
        for(LSMImage lsm : lsms) {
            if (updateFilePaths(lsm, nodePaths)) {
                updated = true;
            }
        }

        log.debug("Updating neurons");
        for(NeuronFragment neuron : neurons) {
            if (updateFilePaths(neuron, nodePaths)) {
                updated = true;
            }
        }
        
        return updated;
    }
    
    private boolean moveArtifacts(Sample sample, Map<String,String> pathPrefixChanges, Set<String> moved) throws Exception {
        
        if (pathPrefixChanges==null || pathPrefixChanges.isEmpty()) return false;
        boolean updated = false;
        
        for(ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {
            
            // Move the pipeline artifacts
            for(SamplePipelineRun run : objectiveSample.getPipelineRuns()) {                
                if (moveArtifact(run, pathPrefixChanges, moved)) {
                    updated = true;
                }
            }
            
            // Update the denormalized records in SampleTile
            for (SampleTile sampleTile : objectiveSample.getTiles()) {
                moveFiles(sampleTile, pathPrefixChanges, moved);       
            }
            
            
        }
        return updated;
    }

    private boolean moveArtifact(HasResults hasResults, Map<String,String> pathPrefixChanges, Set<String> moved) throws Exception {
        
        boolean updated = false;
        
        for(PipelineResult result : hasResults.getResults()) {

            if (moveFiles(result, pathPrefixChanges, moved)) {
                updated = true;
            }
            
            if (result instanceof HasFileGroups) {
                HasFileGroups hasFileGroups = (HasFileGroups)result;
                // Each file group has to be addressed separately
                for (FileGroup fileGroup : hasFileGroups.getGroups()) {
                    log.info("Updating file group "+fileGroup.getKey());
                    if (moveFiles(fileGroup, pathPrefixChanges, moved)) {
                        updated = true;
                    }
                }
            }
            
            // Recurse into sub results
            if (moveArtifact(result, pathPrefixChanges, moved)) {
                updated = true;
            }
            
        }
        
        return updated;
    }
    
    private boolean moveFiles(HasFiles hasFiles, Map<String,String> pathPrefixChanges, Set<String> moved) throws Exception {

        boolean updated = false;

        if (hasFiles instanceof HasRelativeFiles) {
            HasRelativeFiles hasRelativeFiles = (HasRelativeFiles)hasFiles;
            String path = hasRelativeFiles.getFilepath(); 
            
            // Iterate over path changes and see if any need to be applied to the file path
            INNER: for (String oldPrefix : pathPrefixChanges.keySet()) {
    
                if (path!=null && path.startsWith(oldPrefix)) {
                    String newPrefix = pathPrefixChanges.get(oldPrefix);
                    moveFiles(path, oldPrefix, newPrefix, moved);
                    
                    // Update the sample
                    String updatedPath = path.replace(oldPrefix, newPrefix);
                    hasRelativeFiles.setFilepath(updatedPath);
                    log.info("Updated root path: "+path+" -> "+updatedPath);
                    updated = true;
                    
                    break INNER; // Only one path change can apply to a given result
                }
            }
        }

        // The same thing for each file. Actual file moves should have been mostly resolved above, but this updates any denormalized records.
        for(FileType fileType : hasFiles.getFiles().keySet()) {
            String filepath = DomainUtils.getFilepath(hasFiles, fileType);

            // Iterate over path changes and see if any need to be applied
            INNER: for (String oldPath : pathPrefixChanges.keySet()) {

                if (filepath!=null && filepath.startsWith(oldPath)) {
                    String newPath = pathPrefixChanges.get(oldPath);
                    moveFiles(filepath, oldPath, newPath, moved);
                    
                    String updatedPath = filepath.replace(oldPath, newPath);
                    DomainUtils.setFilepath(hasFiles, fileType, updatedPath);
                    log.info("Updated file path: "+filepath+" -> "+updatedPath);
                    updated = true;
                    
                    break INNER; // Only one path change can apply to a given result
                }
            }
        }
        
        return updated;
    }
    
    private void moveFiles(String path, String oldPrefix, String newPrefix, Set<String> moved) throws IOException {

        log.debug("Moving files from: "+oldPrefix+" -> "+newPrefix);
        
        if (moved.contains(oldPrefix)) {
            log.trace("Already moved: "+path);
        }
        else {
            if (!path.startsWith(centralDir)) {
                log.warn("Ignoring foreign path: "+path);
            }
            else {
                File oldFileNodeDir = new File(oldPrefix);
                File newFileNodeDir = new File(newPrefix);
                if (!debug) {
                    // Make sure the parent of the target dir exists
                    FileUtil.ensureDirExists(newFileNodeDir.getParent());
                    // Move the child dir
                    FileUtil.moveFileUsingSystemCall(oldFileNodeDir, newFileNodeDir.getParentFile());
                }
                log.info("Moved dir "+oldPrefix+" -> "+newPrefix);
            }
            
            // Keep track of what's been moved, so we don't attempt to move it more than once
            moved.add(oldPrefix);
        }
    }
    
    /**
     * Updates the file paths in the given object with the given set of replacements.
     * @param hasFiles
     * @param nodePaths
     * @return
     * @throws Exception
     */
    private boolean updateFilePaths(HasRelativeFiles hasFiles, Map<String,String> nodePaths) throws Exception {
        boolean updated = false;

        // Update root path first
        String rootPath = hasFiles.getFilepath();
        if (rootPath!=null) {
    
            // Iterate over path changes and see if any need to be applied
            INNER: for (String oldPath : nodePaths.keySet()) {
    
                if (rootPath.startsWith(oldPath)) {
                    String newPath = nodePaths.get(oldPath);
                    String updatedPath = rootPath.replace(oldPath, newPath);
                    hasFiles.setFilepath(updatedPath);
                    log.info("Updated root path: "+rootPath+" -> "+updatedPath);
                    updated = true;
                    
                    break INNER; // Only one path change can apply
                }
                
            }
        }
        
        // Now update relative paths
        for(FileType fileType : hasFiles.getFiles().keySet()) {
            String path = DomainUtils.getFilepath(hasFiles, fileType);

            if (path!=null) {
                // Iterate over path changes and see if any need to be applied
                INNER: for (String oldPath : nodePaths.keySet()) {

                    if (path.startsWith(oldPath)) {
                        String newPath = nodePaths.get(oldPath);
                        String updatedPath = path.replace(oldPath, newPath);
                        DomainUtils.setFilepath(hasFiles, fileType, updatedPath);
                        log.info("Updated relative path: "+path+" -> "+updatedPath);
                        updated = true;
                        
                        break INNER; // Only one path change can apply
                    }
                    
                }
            }
        }
        
        return updated;
    }

    /**
     * Recalculate diskSpaceUsage metrics for every result in the given sample.
     * @param sample
     * @return
     * @throws Exception
     */
    public Sample updateDiskSpaceUsage(Sample sample) throws Exception {
        return updateDiskSpaceUsage(sample, null);
    }
    
    /**
     * Recalculate diskSpaceUsage metrics for every result in the given sample, 
     * optionally constrained by the given pipeline run.
     * @param sample The Sample to update
     * @param runFilter Optional filter. If this is non-null then only the runs with the same id will be updated.
     * @return Updated sample 
     * @throws Exception
     */
    public Sample updateDiskSpaceUsage(Sample sample, SamplePipelineRun runFilter) throws Exception {
        
        Long totalUsage = 0L;
        
        log.info("Updating disk usage statistics for "+sample);
        
        for (ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {
            log.trace("  Checking files for objective "+objectiveSample.getObjective());
            for (SamplePipelineRun samplePipelineRun : objectiveSample.getPipelineRuns()) {

                Long cachedUsage = samplePipelineRun.getDiskSpaceUsage();
                
                if (cachedUsage==null || runFilter==null || runFilter.getId().equals(samplePipelineRun.getId())) {
                    log.trace("    Checking files for run "+samplePipelineRun.getName());
                    for (PipelineResult pipelineResult : samplePipelineRun.getResults()) {
                        totalUsage += updateDiskSpaceUsage(pipelineResult);
                    }
                }
                else {
                    log.trace("    Checking cached usage for run "+samplePipelineRun.getName());
                    log.trace("    "+cachedUsage+" bytes - cached usage for "+samplePipelineRun.getName());
                    continue;
                }
                
            }
        }
        
        sample.setDiskSpaceUsage(totalUsage);
        log.debug("      "+totalUsage+" bytes - "+sample.getName()+" (id="+sample.getId()+")");
        return saveSample(sample);
    }

    private Long updateDiskSpaceUsage(PipelineResult result) throws Exception {

        if (result.getFilepath()==null) return 0L;
        
        log.trace("      Checking files for result "+result.getName());
        Long totalUsage = 0L;
        
        File dir = new File(result.getFilepath());
        Long dirSize = getApparentSizeOfDirectory(dir);
        
        if (dirSize != null) {
            log.debug("      "+dirSize+" bytes - "+result.getFilepath());
            totalUsage += dirSize;    
            result.setDiskSpaceUsage(totalUsage);
        }

        // recurse
        for (PipelineResult pipelineResult : result.getResults()) {
            totalUsage += updateDiskSpaceUsage(pipelineResult);
        }
        
        return totalUsage;
    }
    
    private Long getApparentSizeOfDirectory(File dir) throws Exception {

        List<String> command = new ArrayList<>();
        command.add("du");
        command.add("-bs");
        command.add(dir.getAbsolutePath());
        
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        final Process p = pb.start();
        
        Long size = null;
        
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line = null;
            // Take the last size that is reported
            while((line = reader.readLine()) != null) {
                try {
                    size = new Long(line.split("\\s")[0]);
                }
                catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    log.warn("Unexpected output from du: "+line);
                }
            }
        }
        
        return size;
    }
    
    public void dispatchForProcessing(Sample sample, boolean reuse, PipelineStatus targetStatus) throws Exception {
        dispatchForProcessing(sample, reuse, targetStatus, "GSPS_CompleteSamplePipeline");
    }
    
    /**
     * Dispatch the given sample for reprocessing. The sample's status will be updated to the 
     * target status, and then the sample will be dispatched. The sample transition will also be logged.
     * @param sample
     * @param reuse
     * @param targetStatus
     * @throws Exception
     */
    public void dispatchForProcessing(Sample sample, boolean reuse, PipelineStatus targetStatus, String processName) throws Exception {
        // TODO: reimplement this with JACSv2
        throw new UnsupportedOperationException();
//        if (sample.getBlocked()) {
//            return;
//        }
//
//        log.info("User "+ownerKey+" dispatching "+sample+", owned by "+sample.getOwnerKey()+", with reuse="+reuse);
//
//        if (debug) return;
//
//        // Create reprocessing task
//        ComputeBeanLocal computeBean = EJBFactory.getLocalComputeBean();
//        Set<TaskParameter> taskParameters = new HashSet<>();
//        taskParameters.add(new TaskParameter("sample entity id", sample.getId().toString(), null));
//        taskParameters.add(new TaskParameter("order no", orderNo, null));
//        taskParameters.add(new TaskParameter("reuse summary", ""+reuse, null));
//        taskParameters.add(new TaskParameter("reuse processing", ""+reuse, null));
//        taskParameters.add(new TaskParameter("reuse post", ""+reuse, null));
//        taskParameters.add(new TaskParameter("reuse alignment", ""+reuse, null));
//        Task task = new GenericTask(new HashSet<Node>(), sample.getOwnerKey(),
//                new ArrayList<Event>(), taskParameters, processName, processName);
//        task = computeBean.saveOrUpdateTask(task);
//
//        // Dispatch task
//        Long dispatchId = computeBean.dispatchJob(processName, task.getObjectId());
//
//        // Update sample status
//        domainDao.updateProperty(sample.getOwnerKey(), Sample.class, sample.getId(), "status", targetStatus.toString());
//
//        // Log the state transition
//        logStatusTransition(sample.getId(), PipelineStatus.valueOf(sample.getStatus()), targetStatus);
//
//        // Log the activity
//        ActivityLogHelper.getInstance().logReprocessing(sample.getOwnerKey(), sample.getName());
//        log.info("Marked sample "+sample.getId() + " for reprocessing with dispatchId " + dispatchId);
//        numSamplesReprocessed++;
    }
    
    public void logStatusTransition(Long sampleId, PipelineStatus source, PipelineStatus target) throws Exception {
        domainDao.addPipelineStatusTransition(sampleId, source, target, orderNo, process, null);
    }
    
    private Map<String,DomainObjectAttribute> getSampleAttrs() {
        if (sampleAttrs==null) {
            log.info("Building sample attribute map");
            this.sampleAttrs = new HashMap<>();
            for (DomainObjectAttribute attr : DomainUtils.getSearchAttributes(Sample.class)) {
                log.trace("  "+attr.getLabel()+" -> Sample."+attr.getName());
                sampleAttrs.put(attr.getLabel(), attr);
            }
        }
        return sampleAttrs;
    }  
    
    private Map<String,SageField> getLsmSageFields() {
        if (lsmSageFields==null) {
            log.info("Building LSM SAGE field map");
            this.lsmSageFields = new HashMap<>();
            for (Field field : ReflectionUtils.getAllFields(LSMImage.class)) {
                SAGEAttribute sageAttribute = field.getAnnotation(SAGEAttribute.class);
                if (sageAttribute!=null) {
                    SageField attr = new SageField();
                    attr.cvName = sageAttribute.cvName();
                    attr.termName = sageAttribute.termName();
                    attr.field = field;
                    log.trace("  " + attr.getKey() + " -> LsmImage." + field.getName());
                    lsmSageFields.put(attr.getKey(), attr);
                }
            }
        }
        return lsmSageFields;
    }

    private Map<String,SageField> getSampleSageFields() {
        if (sampleSageFields==null) {
            log.info("Building sample SAGE field map");
            this.sampleSageFields = new HashMap<>();
            for (Field field : ReflectionUtils.getAllFields(Sample.class)) {
                SAGEAttribute sageAttribute = field.getAnnotation(SAGEAttribute.class);
                if (sageAttribute!=null) {
                    SageField attr = new SageField();
                    attr.cvName = sageAttribute.cvName();
                    attr.termName = sageAttribute.termName();
                    attr.field = field;
                    log.trace("  "+field.getName()+" -> Sample."+field.getName());
                    sampleSageFields.put(field.getName(), attr);
                }
            }
        }
        return sampleSageFields;
    }    

    private class SageField {
        String cvName;
        String termName;
        Field field;
        public String getKey() {
            return cvName+"_"+termName;
        }
    }
    
    /**
     * Return the channel specification for the LSM (or create a default one using the number of channels).
     * @param lsm
     * @return
     */
    public String getLSMChannelSpec(LSMImage lsm, int refIndex) {
        
        String chanSpec = lsm.getChanSpec();
        if (!StringUtils.isEmpty(chanSpec)) {
            return chanSpec;
        }
        
        Integer numChannels = lsm.getNumChannels();
        if (numChannels!=null) {
            try {
            	return ChanSpecUtils.createChanSpec(numChannels, refIndex+1);    
            }
            catch (NumberFormatException e) {
                log.warn("Could not parse Num Channels ('"+numChannels+"') on LSM with id="+lsm.getId());
            }
        }
        
        throw new IllegalStateException("LSM has no Channel Specification and no Num Channels");
    }
    
    /**
     * Go through a sample area's tiles and look for a concatenated LSM attribute with a given name. If a consensus can 
     * be reached across all the Tiles in the area, then return that consensus. Otherwise log a warning and return null.
     * @param sampleArea
     * @param attrName
     * @return
     * @throws Exception
     */
    public String getConsensusTileAttributeValue(AnatomicalArea sampleArea, String attrName, String delimiter) throws Exception {
        List<AnatomicalArea> sampleAreas = new ArrayList<>();
        sampleAreas.add(sampleArea);
        return getConsensusTileAttributeValue(sampleAreas, attrName, delimiter);
    }

    /**
     * Go through a set of sample areas' tiles and look for an attribute with a given name. If a consensus
     * can be reached across all the LSM's in the area then return that consensus. Otherwise log a warning and return null.
     * @param attrName
     * @return
     * @throws Exception
     */
    public String getConsensusTileAttributeValue(List<AnatomicalArea> sampleAreas, String attrName, String delimiter) throws Exception {
        Sample sample = null;
        String consensus = null;
        log.trace("Determining consensus for " + attrName + " for sample areas: " + getSampleAreasCSV(sampleAreas));
        for(AnatomicalArea sampleArea : sampleAreas) {
        	log.trace("  Determining consensus for "+attrName+" in "+sampleArea.getName()+" sample area");
		
        	if (sample==null) {
            	sample = domainDao.getDomainObject(null, Sample.class, sampleArea.getSampleId());
        	}
        	else if (!sample.getId().equals(sampleArea.getSampleId())) {
        	    throw new IllegalStateException("All sample areas must come from the same sample");
        	}
        	
        	ObjectiveSample objectiveSample = sample.getObjectiveSample(sampleArea.getObjective());
        	for(SampleTile sampleTile : getTilesForArea(objectiveSample, sampleArea)) {
        	    log.trace("    Determining consensus for "+attrName+" in "+sampleTile.getName()+" tile");
            	List<LSMImage> lsms = domainDao.getDomainObjectsAs(sampleTile.getLsmReferences(), LSMImage.class);
	        	
            	StringBuilder sb = new StringBuilder();
                for(LSMImage image : lsms) {
                    Object value = DomainUtils.getAttributeValue(image, attrName);
                    if (sb.length()>0) sb.append(delimiter);
                    if (value!=null) sb.append(value);
                }
                
                String tileValue = sb.toString();
                if (consensus!=null && !StringUtils.areEqual(consensus,tileValue)) {
                    log.warn("No consensus for attribute '"+attrName+"' can be reached for sample area "+sampleArea.getName());
                    return null;
                }
                else {
                    consensus = tileValue==null?null:tileValue.toString();
                }
        	}
        
        }
        return consensus;
    }
    
    /**
     * Go through a sample area's LSM supporting files and look for an attribute with a given name. If a consensus
     * can be reached across all the LSM's in the area then return that consensus. Otherwise log a warning and return null.
     * @param attrName
     * @return
     * @throws Exception
     */
    public String getConsensusLsmAttributeValue(AnatomicalArea sampleArea, String attrName) throws Exception {
        List<AnatomicalArea> sampleAreas = new ArrayList<>();
        sampleAreas.add(sampleArea);
        return getConsensusLsmAttributeValue(sampleAreas, attrName);
    }

    /**
     * Go through a set of sample areas' LSM supporting files and look for an attribute with a given name. If a consensus
     * can be reached across all the LSM's in the area then return that consensus. Otherwise log a warning and return null.
     * @param attrName
     * @return
     * @throws Exception
     */
    public String getConsensusLsmAttributeValue(List<AnatomicalArea> sampleAreas, String attrName) throws Exception {
        Sample sample = null;
        String consensus = null;
        log.trace("Determining consensus for " + attrName + " for sample areas: " + getSampleAreasCSV(sampleAreas));
        for(AnatomicalArea sampleArea : sampleAreas) {
        	log.trace("  Determining consensus for "+attrName+" in "+sampleArea.getName()+" sample area");
		
        	if (sample==null) {
            	sample = domainDao.getDomainObject(null, Sample.class, sampleArea.getSampleId());
        	}
        	else if (!sample.getId().equals(sampleArea.getSampleId())) {
        	    throw new IllegalStateException("All sample areas must come from the same sample");
        	}
        	
        	ObjectiveSample objectiveSample = sample.getObjectiveSample(sampleArea.getObjective());
            for(SampleTile sampleTile : getTilesForArea(objectiveSample, sampleArea)) {
        	    log.trace("    Determining consensus for "+attrName+" in "+sampleTile.getName()+" tile");
            	List<LSMImage> lsms = domainDao.getDomainObjectsAs(sampleTile.getLsmReferences(), LSMImage.class);
            	
                for(LSMImage image : lsms) {
    	        	log.trace("      Determining consensus for "+attrName+" in "+image.getName()+" LSM");
                    Object value = DomainUtils.getAttributeValue(image, attrName);
                    if (consensus!=null && !StringUtils.areEqual(consensus,value)) {
                        log.warn("No consensus for attribute '"+attrName+"' can be reached for sample area "+sampleArea.getName());
                        return null;
                    }
                    else {
                        consensus = value==null?null:value.toString();
                    }
                }
        	}
        
        }
        return consensus;
    }

    /**
     * Go through a set of sample areas' LSM supporting files and look for first attribute with a given name. Try this
     * only after attempting to get the consensus.  Can return null, if attribute never found.
     * @param attrName attribute to lookup
     * @return first available, non-null value for that attribute.
     * @throws Exception
     */
    public String getFirstLsmAttributeValue(AnatomicalArea sampleArea, String attrName) throws Exception {
        Sample sample = domainDao.getDomainObject(null, Sample.class, sampleArea.getSampleId());
        if (sample == null) {
            log.warn("No sample found for sample area " + sampleArea);
            return null;
        }
        String rtnVal = null;
        log.trace("  Returning first instance of "+attrName+" in "+sampleArea.getName()+" sample area");

        ObjectiveSample objectiveSample = sample.getObjectiveSample(sampleArea.getObjective());
        if (objectiveSample == null) {
            log.warn("No objectiveSample found for sample area " + sampleArea.getObjective());
            return null;
        }
        for(SampleTile sampleTile : getTilesForArea(objectiveSample, sampleArea)) {
            List<LSMImage> lsms = domainDao.getDomainObjectsAs(sampleTile.getLsmReferences(), LSMImage.class);

            for(LSMImage image : lsms) {
                Object value = DomainUtils.getAttributeValue(image, attrName);
                if (value != null) {
                    rtnVal = value.toString();
                    break;
                }
            }
        }
        return rtnVal;
    }

    private String getSampleAreasCSV(List<AnatomicalArea> sampleAreas) {
    	StringBuilder sb = new StringBuilder();
    	for(AnatomicalArea sampleArea : sampleAreas) {
    		if (sb.length()>0) sb.append(",");
    		sb.append(sampleArea.getName());
    	}
    	return sb.toString();
    }
    
    /**
     * Return the data sets for the configured owner.
     * @return
     * @throws Exception
     */
    public Collection<DataSet> getDataSets() throws Exception {
        if (dataSets==null) {
            loadDataSets();
        }
        return dataSets;
    }
    
    public int getNumSamplesCreated() {
        return numSamplesCreated;
    }

    public int getNumSamplesUpdated() {
        return numSamplesUpdated;
    }

    public int getNumSamplesReprocessed() {
        return numSamplesReprocessed;
    }

    public DataSet getDataSetByNameOrIdentifier(String nameOrIdentifier) throws Exception {
        
        if (nameOrIdentifier==null) {
            throw new IllegalArgumentException("Empty data set identifier");
        }
        
        DataSet dataSet = domainDao.getDataSetByIdentifier(getOwnerKey(), nameOrIdentifier);
        if (dataSet != null) {
            return dataSet;
        }
        
        List<DataSet> dataSets = domainDao.getUserDomainObjectsByName(getOwnerKey(), DataSet.class, nameOrIdentifier);
        if (dataSets.size() == 1) {
            return dataSets.get(0);
        } 
        else if (dataSets.size() > 1) {
            throw new IllegalArgumentException("Found " + dataSets.size() + " objects for " + getOwnerKey()
                + " data set '" + nameOrIdentifier + "' when only one is expected");
        }
        return null;
    }
    
    private void loadDataSets() throws Exception {

        if (dataSets!=null) return;

        this.dataSets = domainDao.getUserDomainObjects(getOwnerKey(), DataSet.class);

        if (dataSetNameFilter != null) {
            List<DataSet> filteredDataSets = new ArrayList<>();
            for (DataSet dataSet : dataSets) {
                if (dataSetNameFilter.equals(dataSet.getName()) || dataSetNameFilter.equals(dataSet.getIdentifier())) {
                    filteredDataSets.add(dataSet);
                    break;
                }
            }
            dataSets = filteredDataSets;
        }

        if (dataSets.isEmpty()) {
            log.info("No data sets found for user: "+getOwnerKey());
            return;
        }

        Collections.sort(dataSets, new Comparator<DataSet>() {
			@Override
			public int compare(DataSet o1, DataSet o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
    }

    public List<SampleTile> getTilesForArea(ObjectiveSample objectiveSample, AnatomicalArea area) {
        List<SampleTile> tiles = new ArrayList<>();
        for(SampleTile tile : objectiveSample.getTiles()) {
            if (area.getName().equals(tile.getAnatomicalArea()) && area.getTileNames().contains(tile.getName())) {
                tiles.add(tile);
            }
        }
        return tiles;
    }

    /* --------------------------- */

    public LSMImage saveLsm(LSMImage lsm) throws Exception {
        if (debug) return lsm;
        return domainDao.save(getOwnerKey(), lsm);
    }

    public Sample saveSample(Sample sample) throws Exception {
        if (debug) return sample;
        return domainDao.save(getOwnerKey(), sample);
    }

    public NeuronFragment saveNeuron(NeuronFragment neuron) throws Exception {
        if (debug) return neuron;
        return domainDao.save(getOwnerKey(), neuron);
    }

    /* --------------------------- */

    public <T extends DomainObject> T lockAndRetrieve(Class<T> clazz, Long id) {

        Long lockingTaskId = getServiceId();
        Reference ref = Reference.createFor(clazz, id);
        DomainObjectLock lock = lockingDao.lockObject(getOwnerKey(), ref, lockingTaskId, "SampleHelper", writeLockTimeoutMs);
        if (lock==null) {
            throw new ComputationException("Could not obtain lock for "+ref);
        }
        T object = domainDao.getDomainObject(getOwnerKey(), clazz, id);
        if (object==null) {
            throw new ComputationException("Locked object, but it disappeared: "+ref);
        }
        log.info("Locked {}", object);
        return object;
    }

    public void unlock(DomainObject object) {
        Reference ref = Reference.createFor(object);
        Long lockingTaskId = getServiceId();
        lockingDao.unlockObject(getOwnerKey(), ref, lockingTaskId);
        log.info("Unlocked {}", object);
    }

    /* --------------------------- */

    public SamplePipelineRun addNewPipelineRun(String name, String pipelineProcess, int pipelineVersion) {
        SamplePipelineRun run = new SamplePipelineRun();
        run.setId(domainDao.getNewId());
        run.setCreationDate(new Date());
        run.setName(name);
        run.setPipelineProcess(pipelineProcess);
        run.setPipelineVersion(pipelineVersion);
        return run;
    }

    public LSMSummaryResult createLSMSummaryResult(String resultName) {
        LSMSummaryResult result = new LSMSummaryResult();
        result.setId(domainDao.getNewId());
        result.setCreationDate(new Date());
        result.setName(resultName);
        result.setFiles(new HashMap<FileType,String>());
        return result;
    }
    
    public SampleProcessingResult addNewSampleProcessingResult(String resultName) {
        SampleProcessingResult result = new SampleProcessingResult();
        result.setId(domainDao.getNewId());
        result.setCreationDate(new Date());
        result.setName(resultName);
        result.setFiles(new HashMap<FileType, String>());
        return result;
    }

    public SampleAlignmentResult addNewAlignmentResult(String resultName) {
        SampleAlignmentResult result = new SampleAlignmentResult();
        result.setId(domainDao.getNewId());
        result.setCreationDate(new Date());
        result.setName(resultName);
        result.setFiles(new HashMap<FileType,String>());
        return result;
    }
    
    public SamplePostProcessingResult addNewSamplePostProcessingResult(String resultName) {
        SamplePostProcessingResult result = new SamplePostProcessingResult();
        result.setId(domainDao.getNewId());
        result.setCreationDate(new Date());
        result.setName(resultName);
        result.setFiles(new HashMap<FileType,String>());
        return result;
    }
    
    public NeuronSeparation addNewNeuronSeparation(String resultName) {
        NeuronSeparation separation = new NeuronSeparation();
        separation.setId(domainDao.getNewId());
        separation.setCreationDate(new Date());
        separation.setName(resultName);
        separation.setFiles(new HashMap<FileType,String>());
        
        ReverseReference fragmentsReference = new ReverseReference();
        fragmentsReference.setReferringClassName(NeuronFragment.class.getSimpleName());
        fragmentsReference.setReferenceAttr("separationId");
        fragmentsReference.setReferenceId(separation.getId());
        fragmentsReference.setCount(0L); // default to zero so that indexing works
        separation.setFragmentsReference(fragmentsReference);

        inheritSampleCompression(separation);
        return separation;
    }

    public NeuronFragment addNewNeuronFragment(NeuronSeparation separation, Integer index) {
        Sample sample = separation.getParentRun().getParent().getParent();
        NeuronFragment neuron = new NeuronFragment();
        neuron.setOwnerKey(sample.getOwnerKey());
        neuron.setReaders(sample.getReaders());
        neuron.setWriters(sample.getWriters());
        neuron.setCreationDate(new Date());
        neuron.setName("Neuron Fragment "+index);
        neuron.setNumber(index);
        neuron.setSample(Reference.createFor(sample));
        neuron.setSeparationId(separation.getId());
        neuron.setFilepath(separation.getFilepath());
        neuron.setFiles(new HashMap<FileType, String>());
        return neuron;
    }
    
    public PipelineError setPipelineRunError(SamplePipelineRun run, String filepath, String operation, String description, String classification) {
        PipelineError error = new PipelineError();
        error.setCreationDate(new Date());
        error.setFilepath(filepath);
        error.setOperation(operation);
        error.setDescription(description);
        error.setClassification(classification);
        run.setError(error);
        return error;
    }

    public PipelineResult addResult(SamplePipelineRun run, PipelineResult result) {
        run.addResult(result);
        Collections.sort(run.getResults(), new SampleResultComparator());
        return result;
    }

    private void inheritSampleCompression(NeuronSeparation separation) {

        if (separation.getParentRun()==null) {
            throw new IllegalArgumentException("Separation has no parent run");
        }
        
        if (separation.getParentRun().getParent()==null) {
            throw new IllegalArgumentException("Separation's parent run has no objective sample parent");
        }

        if (separation.getParentRun().getParent().getParent()==null) {
            throw new IllegalArgumentException("Separation's objective sample has no sample parent");
        }
        
        Sample sample = separation.getParentRun().getParent().getParent();
        if (DomainConstants.VALUE_COMPRESSION_VISUALLY_LOSSLESS.equals(sample.getSeparationCompressionType())) {
            separation.setCompressionType(DomainConstants.VALUE_COMPRESSION_VISUALLY_LOSSLESS);
        }
        else {
            // default
            separation.setCompressionType(DomainConstants.VALUE_COMPRESSION_LOSSLESS);
        }
    }
    
    public List<FileGroup> createFileGroups(String rootPath, List<String> filepaths) throws Exception {

        Map<String,FileGroup> groups = new HashMap<>();
    
        for(String filepath : filepaths) {
            
            File file = new File(filepath);
            String filename = file.getName();
            int d = filename.lastIndexOf('.');
            String name = filename.substring(0, d);
            String ext = filename.substring(d+1);
            
            FileType fileType = null;

            String key = null;
            if (filename.endsWith(".lsm.json")) {
            	key = FilenameUtils.getBaseName(name);
                fileType = FileType.LsmMetadata;
            }
            else if (filename.endsWith(".lsm.metadata")) {
                // Ignore, to get rid of the old-style Perl metadata files
                continue;
            }
            else if ("properties".equals(ext)) {
                // Ignore properties files here, they should be specifically processed, not sucked into a file group
                continue;
            }
            else {
                int u = name.lastIndexOf('_');
                if (u<0) {
                    log.debug("  Ignoring file with no underscores: "+name);
                    continue;
                }
                key = name.substring(0, u);
                String type = name.substring(u+1);
                if ("png".equals(ext)) {
                    if ("all".equals(type)) {
                        fileType = FileType.AllMip; 
                    }
                    else if ("reference".equals(type)) {
                        fileType = FileType.ReferenceMip;   
                    }
                    else if ("signal".equals(type)) {
                        fileType = FileType.SignalMip;  
                    }
                    else if ("signal1".equals(type)) {
                        fileType = FileType.Signal1Mip; 
                    }
                    else if ("signal2".equals(type)) {
                        fileType = FileType.Signal2Mip; 
                    }
                    else if ("signal3".equals(type)) {
                        fileType = FileType.Signal3Mip; 
                    }
                    else if ("refsignal1".equals(type)) {
                        fileType = FileType.RefSignal1Mip;  
                    }
                    else if ("refsignal2".equals(type)) {
                        fileType = FileType.RefSignal2Mip;  
                    }
                    else if ("refsignal3".equals(type)) {
                        fileType = FileType.RefSignal3Mip;  
                    }
                }
                else if ("mp4".equals(ext)) {
                    if ("all".equals(type) || "movie".equals(type)) {
                        fileType = FileType.AllMovie;   
                    }
                    else if ("reference".equals(type)) {
                        fileType = FileType.ReferenceMovie; 
                    }
                    else if ("signal".equals(type)) {
                        fileType = FileType.SignalMovie;    
                    }
                }
            }
            
            if (fileType==null) {
                log.debug("  Could not determine file type for: "+filename);
                continue;
            }
            
            FileGroup group = groups.get(key);
            if (group==null) {
                group = new FileGroup(key);
                group.setFilepath(rootPath);
                group.setFiles(new HashMap<FileType,String>());
                groups.put(key, group);
            }
            
            DomainUtils.setFilepath(group, fileType, filepath);
        }

        return new ArrayList<>(groups.values());
    }
    
    /**
     * Given a list of FileGroups produced by createFileGroups, and a list of inputImages, update the FilrGroup
     * key to the correct inputImage key, and create a map with the inputImage keys mapping to each set of outputs. 
     * @param inputImages
     */
    public Map<String,FileGroup> getGroupsByKey(List<FileGroup> fileGroups, List<Void> inputImages) {
        // TODO: reimplement this with JACSv2
        throw new UnsupportedOperationException();

//        Map<String,String> keyToCorrectedKeyMap = new HashMap<>();
//        for(InputImage inputImage : inputImages) {
//            log.debug("Will replace output prefix "+inputImage.getOutputPrefix()+" with key "+inputImage.getKey());
//            keyToCorrectedKeyMap.put(inputImage.getOutputPrefix(), inputImage.getKey());
//        }
//
//        Map<String,FileGroup> groups = new HashMap<>();
//        for(FileGroup fileGroup : fileGroups) {
//            String key = fileGroup.getKey();
//            String correctedKey = keyToCorrectedKeyMap.get(key);
//            if (correctedKey==null) {
//                log.warn("Unrecognized output prefix: "+key);
//                continue;
//            }
//            fileGroup.setKey(correctedKey);
//            groups.put(correctedKey, fileGroup);
//        }
//
//        return groups;
    }
    
    public void copyFileGroupToHasFiles(FileGroup fileGroup, HasFiles result) {
        Map<FileType, String> files = fileGroup.getFiles();
        for(FileType fileType : files.keySet()) {
            DomainUtils.setFilepath(result, fileType, DomainUtils.getFilepath(fileGroup, fileType));
        }
    }
    
    public String getProcess() {
        return process;
    }

    public void setProcess(String process) {
        this.process = process;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    private enum UpdateType {
        ADD,
        REMOVE,
        CHANGE,
        SAME
    }

    /**
     * Filters the given set of domain objects and returns only the ones which are not annotated, 
     * or referenced by tree nodes (i.e. contained in folders).
     * @param domainObjects
     * @return
     * @throws Exception
     */
    public <T extends DomainObject> Collection<T> getUnannotatedAndUnreferenced(Collection<T> domainObjects) throws Exception {
        
        List<Reference> neuronRefs = DomainUtils.getReferences(domainObjects);
        List<Annotation> annotations = domainDao.getAnnotations(null, neuronRefs);
        ListMultimap<Long,Annotation> annotationMap = DomainUtils.getAnnotationsByDomainObjectId(annotations);
        
        Set<Long> referencedIds = new HashSet<>();
        List<TreeNode> containers = domainDao.getContainers(null, neuronRefs);
        for (TreeNode treeNode : containers) {
            for (Reference reference : treeNode.getChildren()) {
                referencedIds.add(reference.getTargetId());
            }
        }
        
        List<T> unannotatedAndUnreferenced = new ArrayList<>();
        for(T domainObject : domainObjects) {
            
            if (!annotationMap.get(domainObject.getId()).isEmpty()) {
                log.trace(domainObject+" is annotated");
                continue;
            }
            
            if (referencedIds.contains(domainObject.getId())) {
                log.trace(domainObject+" is referenced by tree nodes");
                continue;
            }
            
            unannotatedAndUnreferenced.add(domainObject);
        }
        
        return unannotatedAndUnreferenced;
    }
    

    public void getInputImage(Sample sample, String objective, String mode, String area, String filepath,
            String chanSpec, String colorSpec, String divSpec, String key, String prefix) {

        // TODO: reimplement this with JACSv2
        throw new UnsupportedOperationException();

//        if (colorSpec==null) {
//            if (chanSpec.length()>4) {
//                throw new IllegalArgumentException("Channel specification contains too many channels: "+chanSpec);
//            }
//            else if (chanSpec.length()==4) {
//                // Default to RGB for MCFO images
//                colorSpec = ChanSpecUtils.getDefaultColorSpec(chanSpec, "RGB", "1");
//            }
//            else {
//                // On 3 channel images (e.g. polarity), use cyan for second channel, for backwards compatibility with older MIPs
//                // which were created with the neusep MIP creator.
//                // For 2 channel images, this will use R1.
//                // For single reference channel images, the reference will be gray.
//                colorSpec = ChanSpecUtils.getDefaultColorSpec(chanSpec, "RCM", "1");
//            }
//        }
//
//        log.info("Input file: "+filepath);
//        log.info("  Area: "+area);
//        log.info("  Channel specification: "+chanSpec);
//        log.info("  Color specification: "+colorSpec);
//        log.info("  Output prefix: "+prefix);
//
//        InputImage inputImage = new InputImage();
//        inputImage.setFilepath(filepath);
//        inputImage.setArea(area);
//        inputImage.setChanspec(chanSpec);
//        inputImage.setColorspec(colorSpec);
//        inputImage.setDivspec(divSpec);
//        inputImage.setOutputPrefix(prefix);
//        inputImage.setKey(key);
//
//        return inputImage;
    }

    public String sanitize(String s) {
        if (s==null) return null;
        return s.replaceAll("\\s+", "_").replaceAll("-", "_");
    }

    public String dashedSanitized(Object... strs) {
     
        StringBuilder sb = new StringBuilder();
        
        for(Object s : strs) {
            if (sb.length()>0) sb.append("-");
            sb.append(sanitize(s.toString()));
        }
        
        return sb.toString();
    }
    
    /**
     * Returns a divisor spec which determines how bright the reference channel will be in combined Signal+Ref MIPs.
     * @param chanSpec
     * @return
     */
    public String getDivSpec(String chanSpec) {
        return chanSpec.replaceAll("r", "2").replaceAll("s", "1");
    }
}

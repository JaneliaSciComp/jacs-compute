package org.janelia.jacs2.dataservice.sample;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.sample.NeuronFragment;
import org.janelia.it.jacs.model.domain.sample.PipelineResult;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.jacs2.dao.LSMImageDao;
import org.janelia.jacs2.dao.NeuronFragmentDao;
import org.janelia.jacs2.dao.SampleDao;
import org.janelia.it.jacs.model.domain.Reference;
import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.ObjectiveSample;
import org.janelia.it.jacs.model.domain.sample.SampleTile;
import org.janelia.it.jacs.model.domain.sample.TileLsmPair;
import org.janelia.jacs2.dataservice.subject.SubjectService;
import org.janelia.jacs2.model.DataInterval;
import org.janelia.jacs2.model.EntityFieldValueHandler;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;
import org.janelia.jacs2.dataservice.DomainObjectService;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SampleDataService {

    private final DomainObjectService domainObjectService;
    private final SubjectService subjectService;
    private final SampleDao sampleDao;
    private final LSMImageDao lsmImageDao;
    private final NeuronFragmentDao neuronFragmentDao;
    private final Logger logger;

    @Inject
    public SampleDataService(DomainObjectService domainObjectService,
                             SubjectService subjectService,
                             SampleDao sampleDao,
                             LSMImageDao lsmImageDao,
                             NeuronFragmentDao neuronFragmentDao,
                             Logger logger) {
        this.domainObjectService = domainObjectService;
        this.subjectService = subjectService;
        this.sampleDao = sampleDao;
        this.lsmImageDao = lsmImageDao;
        this.neuronFragmentDao = neuronFragmentDao;
        this.logger = logger;
    }

    public Sample getSampleById(String subjectName, Number sampleId) {
        Subject subject = subjectService.getSubjectByName(subjectName);
        return getSampleById(subject, sampleId);
    }

    private Sample getSampleById(Subject subject, Number sampleId) {
        Sample sample = sampleDao.findById(sampleId);
        if (subject != null) {
            if (sample != null && subject.canRead(sample)) {
                return sample;
            } else {
                throw new SecurityException("Subject " + subject.getKey() + " does not have read access to sample: "+ sampleId);
            }
        } else {
            return sample;
        }
    }

    public List<LSMImage> getLSMsByIds(String subjectName, List<Number> lsmIds) {
        Subject subject = subjectService.getSubjectByName(subjectName);
        return lsmImageDao.findSubtypesByIds(subject, lsmIds, LSMImage.class);
    }

    public List<ObjectiveSample> getObjectivesBySampleIdAndObjective(String subjectName, Number sampleId, String objective) {
        Preconditions.checkArgument(sampleId != null, "Sample ID must be specified retrieving sample objectives");
        Subject currentSubject = subjectService.getSubjectByName(subjectName);
        Sample sample = getSampleById(currentSubject, sampleId);
        if (sample == null) {
            logger.info("Invalid sampleId {} or subject {} has no access", sampleId, subjectName);
            return Collections.emptyList();
        }
        Predicate<ObjectiveSample> objectiveFilter = objectiveSample -> StringUtils.isBlank(objective) || objective.equals(objectiveSample.getObjective());
        return sample.getObjectiveSamples().stream()
                .filter(objectiveFilter)
                .map(os -> {
                    os.setParent(sample);
                    return os;
                })
                .collect(Collectors.toList());
    }

    public List<AnatomicalArea> getAnatomicalAreasBySampleIdAndObjective(String subjectName, Number sampleId, String objective) {
        return getAnatomicalAreasBySampleIdAndObjective(subjectName, sampleId, objective, Optional.<String>empty());
    }

    public List<AnatomicalArea> getAnatomicalAreasBySampleIdObjectiveAndArea(String subjectName, Number sampleId, String objective, String anatomicalAreaName) {
        return getAnatomicalAreasBySampleIdAndObjective(subjectName, sampleId, objective, StringUtils.isBlank(anatomicalAreaName) ? Optional.<String>empty() : Optional.of(anatomicalAreaName.trim()));
    }

    private List<AnatomicalArea> getAnatomicalAreasBySampleIdAndObjective(String subjectName, Number sampleId, String objective, Optional<String> anatomicalAreaName) {
        Preconditions.checkArgument(sampleId != null, "Sample ID must be specified for anatomical area retrieval");
        Subject currentSubject = subjectService.getSubjectByName(subjectName);
        Sample sample = getSampleById(currentSubject, sampleId);
        if (sample == null) {
            logger.info("Invalid sampleId {} or subject {} has no access", sampleId, subjectName);
            return Collections.emptyList();
        }
        Map<String, LSMImage> indexedLsms = new LinkedHashMap<>();
        Predicate<ObjectiveSample> objectiveFilter = objectiveSample -> StringUtils.isBlank(objective) || objective.equals(objectiveSample.getObjective());
        sample.getObjectiveSamples().stream()
                .filter(objectiveFilter)
                .flatMap(objectiveSample -> streamAllLSMs(currentSubject, objectiveSample))
                .filter(lsm -> {
                    if (anatomicalAreaName.isPresent()) {
                        return anatomicalAreaName.get().equals(lsm.getAnatomicalArea());
                    } else {
                        return true;
                    }
                })
                .forEach(lsm -> indexedLsms.put(lsm.getEntityRefId(), lsm));

        Map<String, AnatomicalArea> anatomicalAreaMap = new LinkedHashMap<>();
        sample.getObjectiveSamples().stream()
                .filter(objectiveFilter)
                .forEach(objectiveSample -> {
                    objectiveSample.getTiles().stream()
                            .filter(t -> !anatomicalAreaName.isPresent() || anatomicalAreaName.get().equals(t.getAnatomicalArea()))
                            .forEach(t -> {
                                Reference firstLsmRef = t.getLsmReferenceAt(0);
                                if (firstLsmRef == null) {
                                    logger.error("No LSMs set for tile {} in sample {} objective {}", t, sample, objectiveSample);
                                    throw new IllegalStateException("No LSMs set for tile " + t + " in sample " + sample + " objective " + objectiveSample);
                                }
                                LSMImage firstLSM = indexedLsms.get(firstLsmRef.getTargetRefId());
                                if (firstLSM == null) {
                                    logger.error("No LSM found for {} - first LSM for tile {} in sample {} objective {}",
                                            firstLsmRef.getTargetRefId(), t, sample, objectiveSample);
                                    throw new IllegalStateException("No LSM found for " + firstLsmRef.getTargetRefId());
                                }
                                Reference secondLsmRef = t.getLsmReferenceAt(1);
                                LSMImage secondLSM = null;
                                if (secondLsmRef != null) {
                                    secondLSM = indexedLsms.get(secondLsmRef.getTargetRefId());
                                    if (secondLSM == null) {
                                        logger.error("No LSM found for {} - second LSM for tile {} in sample {} objective {}",
                                                secondLsmRef.getTargetRefId(), t, sample, objectiveSample);
                                        throw new IllegalStateException("No LSM found for " + secondLsmRef.getTargetRefId());
                                    }
                                    if (Optional.ofNullable(firstLSM.getNumChannels()).orElse(0) == 2) {
                                        // Switch the LSMs so that the 3 channel image always comes first
                                        LSMImage tmp = firstLSM;
                                        firstLSM = secondLSM;
                                        secondLSM = tmp;
                                    }
                                }
                                String tileAnatomicalArea = getTileAnatomicalArea(t, firstLSM);
                                AnatomicalArea anatomicalArea = anatomicalAreaMap.get(tileAnatomicalArea);
                                if (anatomicalArea == null) {
                                    anatomicalArea = new AnatomicalArea();
                                    anatomicalArea.setDatasetName(sample.getDataSet());
                                    anatomicalArea.setName(tileAnatomicalArea);
                                    anatomicalArea.setSampleId(sample.getId());
                                    anatomicalArea.setSampleName(sample.getName());
                                    anatomicalArea.setSampleEffector(sample.getEffector());
                                    anatomicalArea.setObjective(objectiveSample.getObjective());
                                    anatomicalArea.setDefaultChanSpec(objectiveSample.getChanSpec());
                                    anatomicalAreaMap.put(tileAnatomicalArea, anatomicalArea);
                                }
                                anatomicalArea.addLsmPair(getTileLsmPair(t.getName(), firstLSM, secondLSM));
                            });
                });
        return ImmutableList.copyOf(anatomicalAreaMap.values());
    }

    private Stream<LSMImage> streamAllLSMs(Subject subject, ObjectiveSample sampleObjective) {
        return domainObjectService
                .streamAllReferences(subject, sampleObjective.getTiles().stream().flatMap(t -> t.getLsmReferences().stream()))
                .map(dobj -> (LSMImage) dobj);
    }

    private String getTileAnatomicalArea(SampleTile tile, LSMImage lsmImage) {
        return StringUtils.defaultIfBlank(lsmImage.getAnatomicalArea(),
                StringUtils.defaultIfBlank(tile.getAnatomicalArea(), ""));
    }

    private TileLsmPair getTileLsmPair(String name, LSMImage firstLSM, LSMImage secondLSM) {
        TileLsmPair lsmPair = new TileLsmPair();
        lsmPair.setTileName(name);
        lsmPair.setFirstLsm(firstLSM);
        lsmPair.setSecondLsm(secondLSM);
        return lsmPair;
    }

    public PageResult<LSMImage> searchLsms(String subjectName, LSMImage pattern, PageRequest pageRequest) {
        Subject subject = subjectService.getSubjectByName(subjectName);
        return lsmImageDao.findMatchingLSMs(subject, pattern, pageRequest);
    }

    public PageResult<Sample> searchSamples(String subjectName, Sample pattern, DataInterval<Date> tmogInterval, PageRequest pageRequest) {
        Subject subject = subjectService.getSubjectByName(subjectName);
        return sampleDao.findMatchingSamples(subject, pattern, tmogInterval, pageRequest);
    }

    public void createLSM(LSMImage lsmImage) {
        lsmImageDao.save(lsmImage);
    }

    public void createNeuronFragments(Collection<NeuronFragment> neuronFragments) {
        neuronFragments.forEach(neuronFragmentDao::save);
    }

    public void createSample(Sample sample) {
        sampleDao.save(sample);
    }

    public void updateLSM(LSMImage lsmImage, Map<String, EntityFieldValueHandler<?>> updatedFields) {
        lsmImageDao.update(lsmImage, updatedFields);
    }

    public void updateSample(Sample sample, Map<String, EntityFieldValueHandler<?>> updatedFields) {
        sampleDao.update(sample, updatedFields);
    }

    public void addSampleObjectivePipelineRun(Sample sample, String objective, SamplePipelineRun samplePipelineRun) {
        sampleDao.addObjectivePipelineRun(sample, objective, samplePipelineRun);
    }

    public void addSampleObjectivePipelineRunResult(Sample sample, String objective, Number runId, Number parentResultId, PipelineResult pipelineResult) {
        sampleDao.addSampleObjectivePipelineRunResult(sample, objective, runId, parentResultId, pipelineResult);
    }

    public void updateSampleObjectivePipelineRunResult(Sample sample, String objective, Number runId, PipelineResult pipelineResult) {
        sampleDao.updateSampleObjectivePipelineRunResult(sample, objective, runId, pipelineResult);
    }
}

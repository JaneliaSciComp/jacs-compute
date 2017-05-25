package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.Reference;
import org.janelia.it.jacs.model.domain.sample.DataSet;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.ObjectiveSample;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.SampleTile;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.dataset.DatasetService;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.DataInterval;
import org.janelia.jacs2.model.SampleUtils;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;
import org.janelia.jacs2.model.page.SortCriteria;
import org.janelia.jacs2.model.page.SortDirection;
import org.janelia.jacs2.model.sage.ImageLine;
import org.janelia.jacs2.model.sage.SlideImage;
import org.janelia.jacs2.model.sage.SlideImageGroup;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Named("lsmImport")
public class LSMImportProcessor extends AbstractBasicLifeCycleServiceProcessor<List<LSMImportProcessor.SageLoaderResult>, Void> {

    static class LSMImportArgs extends ServiceArgs {
        @Parameter(names = "-dataset", description = "Data set name or identifier", required = false)
        String dataset;
        @Parameter(names = "-imageLine", description = "Image line name", required = false)
        String imageLine;
        @Parameter(names = "-slideCodes", description = "Slide codes", required = false)
        List<String> slideCodes;
        @Parameter(names = "-lsmNames", description = "LSM names", required = false)
        List<String> lsmNames;
    }

    static class SageLoaderResult {

        private final Number sageLoaderService;
        private final String lab;
        private final DataSet dataSet;
        private final String imageLine;
        private final List<SlideImage> slideImages;
        private final List<LSMImage> lsmImages = new ArrayList<>();

        SageLoaderResult(Number sageLoaderService, String lab, DataSet dataSet, String imageLine, List<SlideImage> slideImages) {
            this.sageLoaderService = sageLoaderService;
            this.lab = lab;
            this.dataSet = dataSet;
            this.imageLine = imageLine;
            this.slideImages = slideImages;
        }

        void addLsm(LSMImage lsm) {
            lsmImages.add(lsm);
        }
    }

    private final SageLoaderProcessor sageLoaderProcessor;
    private final SampleDataService sampleDataService;
    private final DatasetService datasetService;
    private final SageDataService sageDataService;

    @Inject
    LSMImportProcessor(ServiceComputationFactory computationFactory,
                       JacsServiceDataPersistence jacsServiceDataPersistence,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       SageLoaderProcessor sageLoaderProcessor,
                       SampleDataService sampleDataService,
                       DatasetService datasetService,
                       SageDataService sageDataService,
                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sageLoaderProcessor = sageLoaderProcessor;
        this.sampleDataService = sampleDataService;
        this.datasetService = datasetService;
        this.sageDataService = sageDataService;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new LSMImportArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Void>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public Void collectResult(JacsServiceResult<?> depResults) {
                return null;
            }

            @Override
            public Void getServiceDataResult(JacsServiceData jacsServiceData) {
                return null;
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        LSMImportArgs args = getArgs(jacsServiceData);
        if (StringUtils.isBlank(args.dataset) &&
                StringUtils.isBlank(args.imageLine) &&
                CollectionUtils.isEmpty(args.slideCodes) &&
                CollectionUtils.isEmpty(args.lsmNames)) {
            throw new IllegalArgumentException("No filtering parameter has been specified for the LSM import from Sage.");
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<List<SageLoaderResult>> submitServiceDependencies(JacsServiceData jacsServiceData) {
        LSMImportArgs args = getArgs(jacsServiceData);
        List<SlideImage> slideImages = retrieveSageImages(jacsServiceData.getOwner(), args);
        Map<ImageLine, List<SlideImage>> labImages =
                slideImages.stream().collect(Collectors.groupingBy(
                        si -> new ImageLine(si.getLab(), si.getDataset(), si.getLineName()),
                        Collectors.mapping(Function.identity(), Collectors.toList()))
                );
        List<SageLoaderResult> sageLoaderResults = labImages.entrySet().stream()
                .map(lineEntries -> {
                    ImageLine imageLine = lineEntries.getKey();
                    List<String> slideImageNames = lineEntries.getValue().stream().map(SlideImage::getName).collect(Collectors.toList());
                    DataSet ds = datasetService.getDatasetByNameOrIdentifier(jacsServiceData.getOwner(), imageLine.getDataset());
                    if (ds == null) {
                        logger.error("No dataset record found for {}", lineEntries.getKey());
                        return null;
                    }
                    JacsServiceData sageLoaderService = sageLoaderProcessor.createServiceData(new ServiceExecutionContext(jacsServiceData),
                            new ServiceArg("-lab", imageLine.getLab()),
                            new ServiceArg("-line", imageLine.getLab()),
                            new ServiceArg("-configFile", ds.getSageConfigPath()),
                            new ServiceArg("-grammarFile", ds.getSageGrammarPath()),
                            new ServiceArg("-sampleFiles", String.join(",", slideImageNames))
                    );
                    sageLoaderService = submitDependencyIfNotPresent(jacsServiceData, sageLoaderService);
                    return new SageLoaderResult(sageLoaderService.getId(), imageLine.getLab(), ds, imageLine.getName(), lineEntries.getValue());
                })
                .filter(r -> r != null)
                .collect(Collectors.toList());

        return new JacsServiceResult<>(jacsServiceData, sageLoaderResults);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<List<SageLoaderResult>>> processing(JacsServiceResult<List<SageLoaderResult>> depResults) {
        JacsServiceData jacsServiceData = depResults.getJacsServiceData();
        LSMImportArgs args = getArgs(jacsServiceData);
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    List<SageLoaderResult> lsmImages = pd.getResult().stream()
                            .map(sageLoaderResult -> {
                                sageLoaderResult.slideImages.stream()
                                        .map(SampleUtils::createLSMFromSlideImage)
                                        .map(lsm -> importLsm(jacsServiceData.getOwner(), lsm))
                                        .forEach(lsm -> sageLoaderResult.addLsm(lsm));
                                groupLsmsIntoSamples(jacsServiceData.getOwner(), sageLoaderResult.dataSet, sageLoaderResult.lsmImages);
                                return sageLoaderResult;
                            })
                            .collect(Collectors.toList())
                            ;
                    // find if the samples exist and create them or update them.
                    // TODO
                    return pd;
                });
    }

    private LSMImportArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new LSMImportArgs());
    }

    private List<SlideImage> retrieveSageImages(String subject, LSMImportArgs args) {
        String datasetIdentifer = null;
        if (StringUtils.isNotBlank(args.dataset)) {
            DataSet ds = datasetService.getDatasetByNameOrIdentifier(subject, args.dataset);
            if (ds == null) {
                throw new IllegalArgumentException ("Invalid dataset: " + args.dataset);
            }
            datasetIdentifer = ds.getIdentifier();
        }
        return sageDataService.getMatchingImages(datasetIdentifer, args.imageLine, args.slideCodes, args.lsmNames, new PageRequest());
    }

    private LSMImage importLsm(String owner, LSMImage lsmImage) {
        PageResult<LSMImage> matchingLsms = sampleDataService.searchLsms(owner, lsmImage, new PageRequest());
        if (matchingLsms.isEmpty()) {
            sampleDataService.createLSM(lsmImage);
            return lsmImage;
        } else if (matchingLsms.getResultList().size() == 1) {
            LSMImage existingLsm = matchingLsms.getResultList().get(0);
            sampleDataService.updateLSM(existingLsm, SampleUtils.updateLsmAttributes(lsmImage, existingLsm));
            return existingLsm;
        } else {
            // there is a potential clash or duplication here
            logger.warn("Multiple candidates found for {}", lsmImage);
            LSMImage existingLsm = matchingLsms.getResultList().get(0); // FIXME this probably needs to be changed
            sampleDataService.updateLSM(existingLsm, SampleUtils.updateLsmAttributes(lsmImage, existingLsm));
            return existingLsm;
        }
    }

    private List<Sample> groupLsmsIntoSamples(String owner, DataSet dataSet, List<LSMImage> lsmImages) {
        List<Sample> samples = new ArrayList<>();
        Map<String, List<LSMImage>> imagesBySlideCode =
                lsmImages.stream().collect(Collectors.groupingBy(
                                lsm -> lsm.getSlideCode(),
                                Collectors.mapping(Function.identity(), Collectors.toList()))
                );
        for (String slideCode : imagesBySlideCode.keySet()) {
            processSlideGroup(owner, dataSet, slideCode, imagesBySlideCode.get(slideCode));
        }
        // TODO
        return samples;
    }

    private void processSlideGroup(String owner, DataSet dataSet, String slideCode, List<LSMImage> lsmImages) {
        logger.info("Creating or updating sample {} : {} ", dataSet.getIdentifier(), slideCode);

        Multimap<String, SlideImageGroup> objectiveGroups = groupLsmImagesByObjective(lsmImages);

        logger.debug("Sample objectives: {}", objectiveGroups.keySet());

        Sample sampleRef = new Sample();
        sampleRef.setDataSet(dataSet.getIdentifier());
        sampleRef.setSlideCode(slideCode);

        PageRequest pageRequest = new PageRequest();
        pageRequest.setSortCriteria(ImmutableList.of(new SortCriteria("creationDate", SortDirection.DESC)));
        PageResult<Sample> sampleCandidates = sampleDataService.searchSamples(owner, sampleRef, new DataInterval<Date>(null, null), pageRequest);

        Sample sample = null;
        boolean newSample = false;
        boolean dirtySample = false;
        if (sampleCandidates.isEmpty()) {
            sample = sampleRef;
            newSample = true;
            dirtySample = true;
        } else if (sampleCandidates.getResultList().size() == 1) {
            // there's no conflict
            sample = sampleCandidates.getResultList().get(0);
        } else {
            // retrieve first synced sample(i.e. latest sync-ed since the candidates should be ordered by creationDate desc)
            // or if none was synced get the latest sample created
            sample = sampleCandidates.getResultList().stream()
                    .filter(s -> s.isSageSynced())
                    .findFirst()
                    .orElse(sampleCandidates.getResultList().get(0))
                    ;
            final Sample selectedSample = sample;
            sampleCandidates.getResultList().stream()
                    .filter(s -> s != selectedSample)
                    .forEach(s -> {
                        s.setSageSynced(false);
                        sampleDataService.updateSample(s, ImmutableMap.of("sageSynced", s.isSageSynced()));
                    });
        }

//        try {
//            sample = getOrCreateSample(slideCode, dataSet);
//            sampleNew = sample.getId()==null;
//
//            boolean sampleDirty = sampleNew;

//            if (SampleUtils.updateSampleAttributes(sample, objectiveGroups.values())) {
//                sampleDirty = true;
//            }

//            if (lsmAdded && !sampleNew) {
//                logger.info("  LSMs modified significantly, will mark sample for reprocessing");
//            }

//            // First, remove all tiles/LSMSs from objectives which are no longer found in SAGE
//            for(ObjectiveSample objectiveSample : new ArrayList<>(sample.getObjectiveSamples())) {
//                if (!objectiveGroups.containsKey(objectiveSample.getObjective())) {
//
//                    if ("".equals(objectiveSample.getObjective()) && objectiveGroups.size()==1) {
//                        logger.warn("  Leaving empty objective alone, because it is the only one");
//                        continue;
//                    }
//
//                    if (!objectiveSample.hasPipelineRuns()) {
//                        logger.warn("  Removing existing '"+objectiveSample.getObjective()+"' objective sample");
//                        sample.removeObjectiveSample(objectiveSample);
//                    }
//                    else {
//                        logger.warn("  Resetting tiles for existing "+objectiveSample.getObjective()+" objective sample");
//                        objectiveSample.setTiles(new ArrayList<SampleTile>());
//                    }
//                    sampleDirty = true;
//                }
//            }

//            List<String> objectives = new ArrayList<>(objectiveGroups.keySet());
//            Collections.sort(objectives);
//            for(String objective : objectives) {
//                Collection<SlideImageGroup> subTileGroupList = objectiveGroups.get(objective);
//
//                // Figure out the number of channels that should be in the final merged/stitched sample
//                int sampleNumSignals = getNumSignalChannels(subTileGroupList);
//                int sampleNumChannels = sampleNumSignals+1;
//                String channelSpec = ChanSpecUtils.createChanSpec(sampleNumChannels, sampleNumChannels);
//
//                logger.info("  Processing objective '"+objective+"', signalChannels="+sampleNumSignals+", chanSpec="+channelSpec+", tiles="+subTileGroupList.size());
//
//                // Find the sample, if it exists, or create a new one.
//                UpdateType ut = createOrUpdateObjectiveSample(sample, objective, channelSpec, subTileGroupList);
//                if (ut!=UpdateType.SAME) {
//                    sampleDirty = true;
//                }
//                if (ut==UpdateType.CHANGE && !sampleNew) {
//                    logger.info("  Objective sample '"+objective+"' changed, will mark sample for reprocessing");
//                    needsReprocessing = true;
//                }
//            }

//            if (sampleDirty) {
//                sample = domainDao.save(ownerKey, sample);
//                domainDao.addSampleToOrder(orderNo, sample.getId());
//                logger.info("  Saving sample: "+sample.getName()+" (id="+sample.getId()+")");
//                numSamplesUpdated++;
//            }
//            else if (!sample.getSageSynced()) {
//                logger.info("Resynchronizing sample "+sample.getId());
//                domainDao.updateProperty(ownerKey, Sample.class, sample.getId(), "sageSynced", true);
//            }

//            // Update all back-references from the sample's LSMs
//            Set<Long> includedLsmIds = new HashSet<>();
//            Reference sampleRef = Reference.createFor(sample);
//            List<Reference> lsmRefs = sample.getLsmReferences();
//            for(Reference lsmRef : lsmRefs) {
//                includedLsmIds.add(lsmRef.getTargetId());
//                LSMImage lsm = lsmCache.get(lsmRef.getTargetId());
//                if (lsm==null) {
//                    logger.warn("LSM (id="+lsmRef.getTargetId()+") not found in cache. This should never happen and indicates a bug.");
//                    continue;
//                }
//                if (!StringUtils.areEqual(lsm.getSample(),sampleRef)) {
//                    lsm.setSample(sampleRef);
//                    saveLsm(lsm);
//                    logger.info("  Updated sample reference for LSM#"+lsm.getId());
//                }
//            }
//
//            // Desync all other LSMs that point to this sample
//            for(LSMImage lsm : domainDao.getActiveLsmsBySampleId(sample.getOwnerKey(), sample.getId())) {
//                if (!includedLsmIds.contains(lsm.getId())) {
//                    logger.info("Desynchronized obsolete LSM "+lsm.getId());
//                    domainDao.updateProperty(ownerKey, LSMImage.class, lsm.getId(), "sageSynced", false);
//                }
//            }
//
//            if (sampleDirty) {
//                // Update the permissions on the Sample and its LSMs and neuron fragments
//                domainDao.addPermissions(dataSet.getOwnerKey(), Sample.class.getSimpleName(), sample.getId(), dataSet, true);
//            }
//        }
//        finally {
//            if (!sampleNew) {
//                unlockSample(sample.getId());
//            }
//        }
//
//        return sample;

    }

    private Multimap<String, SlideImageGroup> groupLsmImagesByObjective(List<LSMImage> lsmImages) {
        Multimap<String, SlideImageGroup> objectiveGroups = LinkedHashMultimap.create();
        int tileNum = 0;
        for(LSMImage lsm : lsmImages) {
            // Extract LSM metadata
            String objective = StringUtils.defaultIfBlank(lsm.getObjective(), "");
            String tag = StringUtils.defaultIfBlank(lsm.getTile(), "Tile "+(tileNum+1));
            String area = StringUtils.defaultIfBlank(lsm.getAnatomicalArea(), "");

            // Group LSMs by objective, tile and area
            Collection<SlideImageGroup> subTileGroupList = objectiveGroups.get(objective);
            SlideImageGroup group = null;
            for (SlideImageGroup slideImageGroup : subTileGroupList) {
                if (StringUtils.equals(slideImageGroup.getTag(), tag) && StringUtils.equals(slideImageGroup.getAnatomicalArea(), area)) {
                    group = slideImageGroup;
                    break;
                }
            }
            if (group==null) {
                group = new SlideImageGroup(area, tag);
                objectiveGroups.put(objective, group);
            }
            group.addImage(lsm);
            tileNum++;
        }
        return objectiveGroups;
    }

}

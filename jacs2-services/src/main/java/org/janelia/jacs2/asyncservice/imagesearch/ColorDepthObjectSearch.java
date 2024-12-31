package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.DoublePropertyValue;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.access.domain.dao.ColorDepthImageQuery;
import org.janelia.model.access.domain.dao.ColorDepthLibraryDao;
import org.janelia.model.domain.AbstractDomainObject;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibrary;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibraryUtils;
import org.janelia.model.domain.gui.cdmip.ColorDepthMask;
import org.janelia.model.domain.gui.cdmip.ColorDepthMaskResult;
import org.janelia.model.domain.gui.cdmip.ColorDepthMatch;
import org.janelia.model.domain.gui.cdmip.ColorDepthParameters;
import org.janelia.model.domain.gui.cdmip.ColorDepthResult;
import org.janelia.model.domain.gui.cdmip.ColorDepthSearch;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.ProcessingLocation;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Wraps the ColorDepthFileSearch service with integration with the Workstation via the domain model.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthObjectSearch")
public class ColorDepthObjectSearch extends AbstractServiceProcessor<Reference> {

    private static final String DISPLAY_VARIANT = "display";
    private static final String GRADIENT_VARIANT = "gradient";
    private static final String ZGAPMASK_VARIANT = "zgap";
    private static final String DISPLAY_VARIANT_SUFFIX = "gamma1_4";
    private static final int DEFAULT_MAX_SEARCH_RESULTS = 200;

    private static class MaskData {
        final String filename;
        final int threshhold;
        final int count;

        MaskData(String filename, int threshhold, int count) {
            this.filename = filename;
            this.threshhold = threshhold;
            this.count = count;
        }
    }

    static class IntegratedColorDepthSearchArgs extends ServiceArgs {
        @Parameter(names = "-searchId", description = "GUID of the ColorDepthSearch object to use", required = true)
        Long searchId;
        @Parameter(names = "-maskId", description = "GUID of the ColorDepthMask object to use. If this is empty, all listed masks are searched.")
        Long maskId;
        @Parameter(names = "-runMasksWithoutResults", description = "If a mask id is provided, should other masks also be run if they don't have results yet?")
        boolean runAllOtherMasksWithoutResults = true;
        @Parameter(names = "-use-java-process", description = "If set it uses java process based search; the default is spark based search", arity = 0)
        boolean useJavaProcess = false;
    }

    private final WrappedServiceProcessor<SparkColorDepthFileSearch, List<File>> sparkColorDepthFileSearch;
    private final WrappedServiceProcessor<JavaProcessColorDepthFileSearch, List<File>> javaProcessColorDepthFileSearch;
    private final LegacyDomainDao legacyDomainDao;
    private final ColorDepthImageDao colorDepthImageDao;
    private final ColorDepthLibraryDao colorDepthLibraryDao;
    private final ObjectMapper objectMapper;
    private final int memPerCoreInGB;
    private final int minWorkers;
    private final int maxWorkers;
    private final double partitionSizePerCoreFactor;
    private final boolean filterByPctPixels;

    @Inject
    ColorDepthObjectSearch(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           LegacyDomainDao legacyDomainDao,
                           @IntPropertyValue(name = "service.cluster.memPerCoreInGB", defaultValue = 15) Integer memPerCoreInGB,
                           @IntPropertyValue(name = "service.colorDepthSearch.minWorkers", defaultValue = 1) Integer minWorkers,
                           @IntPropertyValue(name = "service.colorDepthSearch.maxWorkers", defaultValue = -1) Integer maxWorkers,
                           @DoublePropertyValue(name = "service.colorDepthSearch.partitionSizePerCoreFactor", defaultValue = 5) Double partitionSizePerCoreFactor,
                           @BoolPropertyValue(name = "service.colorDepthSearch.filterByPctPixels") Boolean filterByPctPixels,
                           SparkColorDepthFileSearch sparkColorDepthFileSearch,
                           JavaProcessColorDepthFileSearch javaProcessColorDepthFileSearch,
                           ColorDepthImageDao colorDepthImageDao,
                           ColorDepthLibraryDao colorDepthLibraryDao,
                           ObjectMapper objectMapper,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.legacyDomainDao = legacyDomainDao;
        this.memPerCoreInGB = memPerCoreInGB;
        this.minWorkers = minWorkers;
        this.maxWorkers = maxWorkers;
        this.partitionSizePerCoreFactor = partitionSizePerCoreFactor;
        this.filterByPctPixels = filterByPctPixels;
        this.sparkColorDepthFileSearch = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, sparkColorDepthFileSearch);
        this.javaProcessColorDepthFileSearch = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, javaProcessColorDepthFileSearch);
        this.colorDepthImageDao = colorDepthImageDao;
        this.colorDepthLibraryDao = colorDepthLibraryDao;
        this.objectMapper = objectMapper;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthObjectSearch.class, new IntegratedColorDepthSearchArgs());
    }

    @Override
    public ServiceResultHandler<Reference> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Reference>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public Reference getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<Reference>() {});
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public ServiceComputation<JacsServiceResult<Reference>> process(JacsServiceData jacsServiceData) {
        IntegratedColorDepthSearchArgs args = getArgs(jacsServiceData);

        logger.info("Executing ColorDepthSearch#{} with ColorDepthMask#{}", args.searchId, args.maskId);

        ColorDepthSearch search = legacyDomainDao.getDomainObject(jacsServiceData.getOwnerKey(),
                ColorDepthSearch.class, args.searchId);

        if (search == null) {
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#" + args.searchId + " not found");
        }

        jacsServiceDataPersistence.addServiceEvent(
                jacsServiceData,
                JacsServiceData.createServiceEvent(JacsServiceEventTypes.PREPARE_SERVICE_DATA, search.toString()));
        List<CDMMetadata> targets = getTargetColorDepthImages(search.getAlignmentSpace(),
                search.getCDSTargets(), search.useSegmentation(), search.useGradientScores());
        int ntargets = targets.size();
        logger.info("Searching {} total targets", ntargets);

        // Create temporary file with paths to search
        JacsServiceFolder workingDirectory = getWorkingDirectory(jacsServiceData);
        File colorDepthTargetsFile = workingDirectory.getServiceFolder().resolve("colorDepthTargets.json").toFile();
        try {
            objectMapper.writeValue(org.apache.commons.io.FileUtils.openOutputStream(colorDepthTargetsFile), targets);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }

        Set<Reference> masksToRun = new LinkedHashSet<>();
        if (args.maskId != null) {
            masksToRun.add(Reference.createFor(ColorDepthMask.class, args.maskId));
            if (args.runAllOtherMasksWithoutResults) {
                masksToRun.addAll(getMasksFromCurrentSearchWithoutResults(search));
            }
        } else {
            masksToRun.addAll(search.getMasks());
        }
        List<ColorDepthMask> masks = legacyDomainDao.getDomainObjectsAs(Lists.newArrayList(masksToRun.iterator()), ColorDepthMask.class);

        List<MaskData> maskDataList = getMaskData(workingDirectory.getServiceFolder(), masks);
        List<ServiceComputation<?>> cdsComputations = maskDataList.stream()
                .map(maskData -> createColorDepthServiceInvocationParams(
                        maskData.filename,
                        maskData.threshhold,
                        maskData.count,
                        colorDepthTargetsFile.getAbsolutePath(),
                        search.getDataThreshold(),
                        ntargets,
                        workingDirectory.getServiceFolder().resolve("cdsMatches").toString(),
                        search.getParameters().getNegativeRadius(),
                        search.getPixColorFluctuation(),
                        search.getXyShift(),
                        search.getMirrorMask(),
                        search.getPctPositivePixels(),
                        search.useGradientScores()
                ))
                .map(serviceArgList -> {
                    ServiceComputation<JacsServiceResult<List<File>>> cdsComputation;
                    Map<String, String> colorDepthProcessingResources = new LinkedHashMap<>();
                    ProcessorHelper.setHardJobDurationLimitInSeconds(colorDepthProcessingResources, 3599); // set job duration to under 1h
                    if (args.useJavaProcess) {
                        int ncores;
                        if (ntargets < JavaProcessColorDepthFileSearch.TARGETS_PER_JOB / 4) {
                            ncores = 16;
                        } else if (ntargets < JavaProcessColorDepthFileSearch.TARGETS_PER_JOB / 2) {
                            ncores = 20;
                        } else {
                            ncores = 32;
                        }
                        ProcessorHelper.setRequiredSlots(colorDepthProcessingResources, ncores);
                        int processingPartitionSize;
                        if (partitionSizePerCoreFactor > 1) {
                            // just an empirical way to calculate the size of the targets partition
                            // knowing that partitions are processed concurrently and everything inside a partition is processed serially
                            processingPartitionSize = (int) (Math.min(ntargets, JavaProcessColorDepthFileSearch.TARGETS_PER_JOB) / (ncores * partitionSizePerCoreFactor));
                        } else {
                            processingPartitionSize = 100;
                        }
                        serviceArgList.add(new ServiceArg("-partitionSize", processingPartitionSize));
                        int memInGB = calculateRequiredMemInGB(jacsServiceData.getProcessingLocation(),
                                ncores,
                                masks.size(), // this is not quite right
                                ntargets,
                                processingPartitionSize);
                        ProcessorHelper.setRequiredMemoryInGB(colorDepthProcessingResources, memInGB);
                        cdsComputation = runJavaProcessBasedColorDepthSearch(jacsServiceData, serviceArgList, colorDepthProcessingResources);
                    } else {
                        // Curve fitting using https://www.desmos.com/calculator
                        // This equation was found using https://mycurvefit.com
                        int desiredWorkers = (int)Math.round(Math.pow(ntargets, 0.32));
                        // only cap the number of workers if maxWorkers is >= 0
                        int numWorkers = Math.max(Math.min(desiredWorkers, maxWorkers > 0 ? maxWorkers : desiredWorkers), minWorkers);
                        int filesPerWorker = (int)Math.round(ntargets / (double)numWorkers);
                        logger.info("Using {} workers, with {} files per worker", numWorkers, filesPerWorker);

                        serviceArgList.add(new ServiceArg("-useSpark"));
                        serviceArgList.add(new ServiceArg("-numWorkers", numWorkers));
                        cdsComputation = runSparkBasedColorDepthSearch(jacsServiceData, serviceArgList, colorDepthProcessingResources);
                    }
                    return cdsComputation;
                })
                .collect(Collectors.toList());
        return computationFactory.newCompletedComputation(null)
                .thenCombineAll(cdsComputations, (Object ignored, List<?> results) -> (List<JacsServiceResult<List<File>>>) results)
                .thenApply(cdsMatchesResults -> cdsMatchesResults.stream()
                        .flatMap(r -> r.getResult().stream())
                        .collect(Collectors.toList()))
                .thenApply(cdsMatches -> {
                    ColorDepthResult colorDepthResult = collectColorDepthResults(
                            jacsServiceData,
                            search.getParameters(),
                            masks.stream().map(AbstractDomainObject::getId).map(Object::toString).collect(Collectors.toSet()),
                            cdsMatches);
                    legacyDomainDao.addColorDepthSearchResult(jacsServiceData.getOwnerKey(), search.getId(), colorDepthResult);
                    logger.info("Updated search {} with new result {}", search, colorDepthResult);
                    return updateServiceResult(jacsServiceData, Reference.createFor(colorDepthResult));
                });
    }

    private ColorDepthResult collectColorDepthResults(JacsServiceData jacsServiceData, ColorDepthParameters searchParameters, Set<String> maskIds, List<File> cdMatchFiles) {
        jacsServiceDataPersistence.addServiceEvent(
                jacsServiceData,
                JacsServiceData.createServiceEvent(JacsServiceEventTypes.COLLECT_SERVICE_RESULTS, cdMatchFiles.toString()));
        ColorDepthResult colorDepthResult = new ColorDepthResult();
        colorDepthResult.setParameters(searchParameters);
        int maxResultsPerMask = searchParameters.getMaxResultsPerMask() == null || searchParameters.getMaxResultsPerMask() <= 0
                ? DEFAULT_MAX_SEARCH_RESULTS
                : searchParameters.getMaxResultsPerMask();
        cdMatchFiles.stream()
                .filter(cdsMatchesFile -> maskIds.contains(FileUtils.getFileNameOnly(cdsMatchesFile.toPath())))
                .map(cdsMatchesFile -> {
                    try {
                        logger.info("Read color depth search matches from {}", cdsMatchesFile);
                        return objectMapper.readValue(cdsMatchesFile, CDMaskMatches.class);
                    } catch (IOException e) {
                        logger.error("Error reading results from {}", cdsMatchesFile, e);
                        return new CDMaskMatches();
                    }
                })
                .filter(CDMaskMatches::hasResults)
                .collect(Collectors.collectingAndThen(Collectors.groupingBy(CDMaskMatches::getMaskId, Collectors.toList()), // if there are multiple results for the same mask merge them
                        allResByMaskIds -> allResByMaskIds.entrySet().stream()
                                .map(e -> e.getValue().stream()
                                        .reduce(new CDMaskMatches().setMaskId(e.getKey()), (r1, r2) -> r1.addResults(r2.getResults())))
                                .collect(Collectors.toList())
                ))
                .stream()
                .peek(cdMaskMatches -> {
                    Boolean useGradientScores = searchParameters.getUseGradientScores();
                    // if no gradient scores were computed results should have already been "normalized"
                    if (useGradientScores != null && useGradientScores) {
                        logger.info("Normalize results using gradient scores for mask {}", cdMaskMatches.getMaskId());
                        long maxNegativeScore = cdMaskMatches.getResults().stream()
                                .map(cdsMatchResult -> CDScoreUtils.calculateNegativeScore(cdsMatchResult.getGradientAreaGap(), cdsMatchResult.getHighExpressionArea()))
                                .max(Long::compare)
                                .orElse(-1L);
                        int maxMatchingPixels = cdMaskMatches.getResults().stream().parallel()
                                .map(CDSMatchResult::getMatchingPixels)
                                .max(Integer::compare)
                                .orElse(0);
                        logger.info("Values used for normalizing scores: maxNegativeScore:{}, maxMatchingPixels:{}", maxNegativeScore, maxMatchingPixels);
                        cdMaskMatches.getResults().stream().parallel()
                                .forEach(cdsMatchResult -> {
                                    cdsMatchResult.setNormalizedScore(CDScoreUtils.calculateNormalizedScore(
                                            cdsMatchResult.getMatchingPixels(),
                                            cdsMatchResult.getGradientAreaGap(),
                                            cdsMatchResult.getHighExpressionArea(),
                                            maxMatchingPixels,
                                            maxNegativeScore));
                                });
                    }
                })
                .forEach(cdMaskMatches -> {
                    ColorDepthMaskResult maskResult = new ColorDepthMaskResult();
                    maskResult.setMaskRef(Reference.createFor("ColorDepthMask" + "#" + cdMaskMatches.getMaskId()));
                    // re-sort the results in case multiple results for the same mask were merged together
                    CDScoreUtils.sortCDSResults(cdMaskMatches.getResults());
                    cdMaskMatches.getResults().stream()
                            .limit(maxResultsPerMask)
                            .map(cdsMatchResult -> {
                                ColorDepthMatch match = new ColorDepthMatch();
                                Reference matchingImageRef = Reference.createFor(getColorDepthImage(cdsMatchResult.getImageName()));
                                match.setMatchingImageRef(matchingImageRef);
                                ColorDepthImage displayVariantMIP;
                                if (cdsMatchResult.hasVariant(DISPLAY_VARIANT)) {
                                    displayVariantMIP = getColorDepthImage(cdsMatchResult.getVariant(DISPLAY_VARIANT));
                                } else {
                                    displayVariantMIP = null;
                                }
                                if (displayVariantMIP == null) {
                                    match.setImageRef(Reference.createFor(getColorDepthImage(cdsMatchResult.getCdmPath())));
                                } else {
                                    match.setImageRef(Reference.createFor(displayVariantMIP));
                                }
                                match.setMatchingPixels(cdsMatchResult.getMatchingPixels());
                                match.setMatchingPixelsRatio(cdsMatchResult.getMatchingRatio());
                                match.setGradientAreaGap(cdsMatchResult.getGradientAreaGap());
                                match.setHighExpressionArea(cdsMatchResult.getHighExpressionArea());
                                match.setScore(cdsMatchResult.getMatchingPixels());
                                match.setScorePercent(cdsMatchResult.getMatchingRatio());
                                match.setNormalizedScore(cdsMatchResult.getNormalizedScore());
                                match.setMirrored(cdsMatchResult.getMirrored());
                                return match;
                            })
                            .forEach(maskResult::addMatch);
                    colorDepthResult.getMaskResults().add(maskResult);
                });
        try {
            ColorDepthResult persistedColorDepthResult = legacyDomainDao.save(jacsServiceData.getOwnerKey(), colorDepthResult);
            logger.info("Saved {} with {} mask results", persistedColorDepthResult, persistedColorDepthResult.getMaskResults().size());
            return persistedColorDepthResult;
        } catch (Exception e) {
            logger.error("Error saving {}", colorDepthResult, e);
            throw new IllegalStateException(e);
        }
    }

    private Set<Reference> getMasksFromCurrentSearchWithoutResults(ColorDepthSearch colorDepthSearch) {
        Set<Reference> masksWithResults = legacyDomainDao.getDomainObjectsAs(colorDepthSearch.getResults(), ColorDepthResult.class).stream()
                .flatMap(cdsResult -> cdsResult.getMaskResults().stream())
                .map(ColorDepthMaskResult::getMaskRef)
                .collect(Collectors.toSet());
        return Sets.difference(ImmutableSet.copyOf(colorDepthSearch.getMasks()), masksWithResults);
    }

    private List<ServiceArg> createColorDepthServiceInvocationParams(String masksFile,
                                                                     Integer masksThreshold,
                                                                     int nmasks,
                                                                     String targetsFile,
                                                                     Integer targetsThreshold,
                                                                     int ntargets,
                                                                     String cdMatchesDirname,
                                                                     Integer negativeRadius,
                                                                     Double pixColorFluctuation,
                                                                     Integer xyShift,
                                                                     Boolean mirrorMask,
                                                                     Double pctPositivePixels,
                                                                     Boolean withGradScores) {
        List<ServiceArg> serviceArgList = new ArrayList<>();
        serviceArgList.add(new ServiceArg("-masksFiles", masksFile));
        serviceArgList.add(new ServiceArg("-maskThreshold", masksThreshold));
        serviceArgList.add(new ServiceArg("-nmasks", nmasks));
        serviceArgList.add(new ServiceArg("-dataThreshold", targetsThreshold));
        serviceArgList.add(new ServiceArg("-targetsFiles", targetsFile));
        serviceArgList.add(new ServiceArg("-ntargets", ntargets));
        serviceArgList.add(new ServiceArg("-cdMatchesDir", cdMatchesDirname));
        serviceArgList.add(new ServiceArg("-negativeRadius", negativeRadius));
        serviceArgList.add(new ServiceArg("-pixColorFluctuation", pixColorFluctuation));
        serviceArgList.add(new ServiceArg("-xyShift", xyShift != null ? xyShift / 2 : xyShift)); // the client save the actual pixel shift in the Search
        serviceArgList.add(new ServiceArg("-mirrorMask", mirrorMask));
        if (filterByPctPixels) serviceArgList.add(new ServiceArg("-pctPositivePixels", pctPositivePixels));
        serviceArgList.add(new ServiceArg("-withGradientScores", withGradScores));
        return serviceArgList;
    }

    private int calculateRequiredMemInGB(ProcessingLocation processingLocation,
                                         int ncores,
                                         int nQueries,
                                         int nTargets,
                                         int processingPartitionSize) {
        if (processingLocation == ProcessingLocation.LSF_JAVA) {
            return ncores * memPerCoreInGB;
        } else {
            // each MIP requires about 2.6M so for memory per color depth search we multiply 2.6 by an empirical factor (3.5 for example)
            return (int) Math.ceil(Math.min(nQueries, JavaProcessColorDepthFileSearch.MASKS_PER_JOB)
                    * 2.6 * 3.5 * ncores *
                    (double) Math.min(nTargets, JavaProcessColorDepthFileSearch.TARGETS_PER_JOB) / processingPartitionSize / 1024.
            );
        }
    }

    private ServiceComputation<JacsServiceResult<List<File>>> runJavaProcessBasedColorDepthSearch(JacsServiceData jacsServiceData,
                                                                                                  List<ServiceArg> serviceArgList,
                                                                                                  Map<String, String> serviceResources) {
        return javaProcessColorDepthFileSearch.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Java process based color depth search")
                        .addResources(serviceResources)
                        .build(),
                serviceArgList);
    }

    private ServiceComputation<JacsServiceResult<List<File>>> runSparkBasedColorDepthSearch(JacsServiceData jacsServiceData,
                                                                                            List<ServiceArg> serviceArgList,
                                                                                            Map<String, String> serviceResources) {
        return sparkColorDepthFileSearch.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Spark based color depth search")
                        .addResources(serviceResources)
                        .build(),
                serviceArgList);
    }

    private List<MaskData> getMaskData(Path masksFolder, List<ColorDepthMask> masks) {
        Map<Integer, List<CDMMetadata>> masksPerFiles = masks.stream()
                .map(mask -> {
                    Reference sampleRef = mask.getSample();
                    CDMMetadata maskMetadata = new CDMMetadata();
                    maskMetadata.setId(mask.getId().toString());
                    maskMetadata.setCdmPath(mask.getFilepath());
                    maskMetadata.setImageName(mask.getFilepath());
                    maskMetadata.setSampleRef(sampleRef != null ? sampleRef.toString() : null);
                    return ImmutablePair.of(mask.getMaskThreshold(), maskMetadata);
                })
                .collect(Collectors.groupingBy(
                        ImmutablePair::getLeft,
                        Collectors.mapping(ImmutablePair::getRight, Collectors.toList())
                ));
        return masksPerFiles.entrySet().stream()
                .map(masksPerFilesEntry -> {
                    File colorDepthMasksFile = masksFolder
                            .resolve("colorDepthMasks-" + masksPerFilesEntry.getKey() + ".json").toFile();
                    try {
                        objectMapper.writeValue(org.apache.commons.io.FileUtils.openOutputStream(colorDepthMasksFile), masksPerFilesEntry.getValue());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return new MaskData(
                            colorDepthMasksFile.getAbsolutePath(),
                            masksPerFilesEntry.getKey(),
                            masksPerFilesEntry.getValue().size()
                    );
                })
                .collect(Collectors.toList());
    }

    private List<CDMMetadata> getTargetColorDepthImages(String alignmentSpace,
                                                        List<String> cdsTargets,
                                                        boolean useSegmentation,
                                                        boolean useGradientScores) {
        logger.info("Collecting target mips from {} libraries", cdsTargets.size());
        return cdsTargets.stream()
                .flatMap(targetLibraryIdentifier -> colorDepthLibraryDao.getLibraryWithVariants(targetLibraryIdentifier).stream())
                .filter(targetLibrary -> ColorDepthLibraryUtils.isSearchableVariant(targetLibrary.getVariant()))
                .flatMap(targetLibrary -> {
                    List<ColorDepthImage> cdmips;
                    Map<Reference, ColorDepthImage> indexedLibraryMIPs;
                    List<ColorDepthImage> libraryMIPs = colorDepthImageDao.streamColorDepthMIPs(
                            new ColorDepthImageQuery()
                                    .withAlignmentSpace(alignmentSpace)
                                    .withLibraryIdentifiers(Collections.singleton(targetLibrary.getIdentifier())))
                            .collect(Collectors.toList());
                    if (useSegmentation && ColorDepthLibraryUtils.hasSearchableVariants(targetLibrary)) {
                        cdmips = colorDepthImageDao.streamColorDepthMIPs(
                                new ColorDepthImageQuery()
                                        .withAlignmentSpace(alignmentSpace)
                                        .withLibraryIdentifiers(ColorDepthLibraryUtils.getSearchableVariants(targetLibrary).stream()
                                                .map(ColorDepthLibrary::getIdentifier)
                                                .collect(Collectors.toSet())))
                                .collect(Collectors.toList());
                        indexedLibraryMIPs = libraryMIPs.stream()
                                .collect(Collectors.toMap(Reference::createFor, Function.identity()));
                    } else {
                        logger.info("No segmentation variant set for {}", targetLibrary.getIdentifier());
                        cdmips = libraryMIPs;
                        indexedLibraryMIPs = Collections.emptyMap();
                    }
                    Set<String> otherRelatedMIPLibraries = libraryMIPs.stream().flatMap(mip -> mip.getLibraries().stream())
                            .map(l -> l + "_" + DISPLAY_VARIANT_SUFFIX)
                            .collect(Collectors.toSet());
                    Set<String> cdmipDisplayMIPs;
                    if (!otherRelatedMIPLibraries.isEmpty()) {
                        cdmipDisplayMIPs = colorDepthImageDao.streamColorDepthMIPs(
                                new ColorDepthImageQuery()
                                        .withAlignmentSpace(alignmentSpace)
                                        .withLibraryIdentifiers(otherRelatedMIPLibraries)
                        )
                                .map(Image::getFilepath)
                                .collect(Collectors.toSet());
                    } else {
                        cdmipDisplayMIPs = Collections.emptySet();
                    }
                    Set<String> cdmipGradients;
                    Set<String> cdmipZgapMasks;
                    if (useGradientScores) {
                        // retrieve gradient and zgapmask mips
                        cdmipGradients = colorDepthImageDao.streamColorDepthMIPs(
                                new ColorDepthImageQuery()
                                        .withAlignmentSpace(alignmentSpace)
                                        .withLibraryIdentifiers(ColorDepthLibraryUtils.selectVariantCandidates(targetLibrary, ImmutableSet.of("grad", "gradient")).stream()
                                                .map(ColorDepthLibrary::getIdentifier)
                                                .collect(Collectors.toSet())))
                                .map(Image::getFilepath)
                                .collect(Collectors.toSet());
                        cdmipZgapMasks = colorDepthImageDao.streamColorDepthMIPs(
                                new ColorDepthImageQuery()
                                        .withAlignmentSpace(alignmentSpace)
                                        .withLibraryIdentifiers(ColorDepthLibraryUtils.selectVariantCandidates(targetLibrary, ImmutableSet.of("zgap", "zgapmask")).stream()
                                                .map(ColorDepthLibrary::getIdentifier)
                                                .collect(Collectors.toSet())))
                                .map(Image::getFilepath)
                                .collect(Collectors.toSet());
                    } else {
                        cdmipGradients = Collections.emptySet();
                        cdmipZgapMasks = Collections.emptySet();
                    }
                    return cdmips.stream()
                            .map(cdmi -> {
                                Reference sampleRef = cdmi.getSampleRef();
                                Reference sourceImageRef = cdmi.getSourceImageRef();
                                ColorDepthImage sourceMIP = indexedLibraryMIPs.get(sourceImageRef);
                                CDMMetadata targetMetadata = new CDMMetadata();
                                targetMetadata.setId(cdmi.getId().toString());
                                targetMetadata.setLibraryName(cdmi.getLibraries().stream().findFirst().orElse(null));
                                targetMetadata.setAlignmentSpace(cdmi.getAlignmentSpace());
                                Set<String> cdmipLibraries;
                                if (sourceMIP == null) {
                                    targetMetadata.setCdmPath(cdmi.getFilepath());
                                    cdmipLibraries = cdmi.getLibraries();
                                } else {
                                    targetMetadata.setCdmPath(sourceMIP.getFilepath());
                                    cdmipLibraries = ImmutableSet.<String>builder()
                                            .addAll(sourceMIP.getLibraries())
                                            .addAll(cdmi.getLibraries())
                                            .build();
                                }
                                targetMetadata.setImageName(cdmi.getFilepath());
                                targetMetadata.setSampleRef(sampleRef != null ? sampleRef.toString() : null);
                                targetMetadata.setRelatedImageRefId(sourceImageRef != null ? sourceImageRef.toString() : null);
                                ColorDepthLibraryUtils.selectVariantCandidates(targetLibrary, ImmutableSet.of(DISPLAY_VARIANT_SUFFIX)).stream()
                                        .map(ColorDepthLibrary::getVariant)
                                        .flatMap(variantName -> CDMMetadataUtils.variantPaths(
                                                variantName,
                                                Paths.get(cdmi.getFilepath()),
                                                cdmi.getAlignmentSpace(),
                                                cdmipLibraries,
                                                vp -> cdmipDisplayMIPs.contains(vp.toString())).stream())
                                        .findFirst()
                                        .ifPresent(variantPath -> targetMetadata.addVariant(DISPLAY_VARIANT, variantPath));
                                if (useGradientScores) {
                                    // select a gradient variant and a zgap variant and add those to the mip metadata
                                    // in order to use them for gradient score
                                    ColorDepthLibraryUtils.selectVariantCandidates(targetLibrary, ImmutableSet.of("grad", "gradient")).stream()
                                            .map(ColorDepthLibrary::getVariant)
                                            .flatMap(variantName -> CDMMetadataUtils.variantPaths(
                                                    variantName,
                                                    Paths.get(cdmi.getFilepath()),
                                                    cdmi.getAlignmentSpace(),
                                                    cdmipLibraries,
                                                    vp -> cdmipGradients.contains(vp.toString())).stream())
                                            .findFirst()
                                            .ifPresent(variantPath -> targetMetadata.addVariant(GRADIENT_VARIANT, variantPath));
                                    ColorDepthLibraryUtils.selectVariantCandidates(targetLibrary, ImmutableSet.of("zgap", "zgapmask")).stream()
                                            .map(ColorDepthLibrary::getVariant)
                                            .flatMap(variantName -> CDMMetadataUtils.variantPaths(
                                                    variantName,
                                                    Paths.get(cdmi.getFilepath()),
                                                    cdmi.getAlignmentSpace(),
                                                    cdmipLibraries,
                                                    vp -> cdmipZgapMasks.contains(vp.toString())).stream())
                                            .findFirst()
                                            .ifPresent(variantPath -> targetMetadata.addVariant(ZGAPMASK_VARIANT, variantPath));
                                }
                                return targetMetadata;
                            });
                })
                .collect(Collectors.toList());
    }

    private ColorDepthImage getColorDepthImage(String filepath) {
        if (StringUtils.isNotBlank(filepath)) {
            return colorDepthImageDao.findColorDepthImageByPath(filepath)
                    .orElse(null);
        } else {
            return null;
        }
    }

    private IntegratedColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new IntegratedColorDepthSearchArgs());
    }
}

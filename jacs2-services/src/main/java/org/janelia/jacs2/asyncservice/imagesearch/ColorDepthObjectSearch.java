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
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.commons.compress.utils.Lists;
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
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Wraps the ColorDepthFileSearch service with integration with the Workstation via the domain model.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthObjectSearch")
public class ColorDepthObjectSearch extends AbstractServiceProcessor<Reference> {

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
    private final int minNodes;
    private final int maxNodes;
    private final int cdsPartitionSize;

    @Inject
    ColorDepthObjectSearch(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           LegacyDomainDao legacyDomainDao,
                           @IntPropertyValue(name = "service.colorDepthSearch.minNodes", defaultValue = 1) Integer minNodes,
                           @IntPropertyValue(name = "service.colorDepthSearch.maxNodes", defaultValue = 8) Integer maxNodes,
                           @IntPropertyValue(name = "service.colorDepthSearch.partitionSize", defaultValue = 100) Integer cdsPartitionSize,
                           SparkColorDepthFileSearch sparkColorDepthFileSearch,
                           JavaProcessColorDepthFileSearch javaProcessColorDepthFileSearch,
                           ColorDepthImageDao colorDepthImageDao,
                           ColorDepthLibraryDao colorDepthLibraryDao,
                           ObjectMapper objectMapper,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.legacyDomainDao = legacyDomainDao;
        this.minNodes = minNodes;
        this.maxNodes = maxNodes;
        this.cdsPartitionSize = cdsPartitionSize;
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
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#"+args.searchId+" not found");
        }

        List<CDMMetadata> targets = getTargetColorDepthImages(search.getAlignmentSpace(),
                search.getCDSTargets(), search.useSegmentation(), search.useGradientScores());
        logger.info("Searching {} total targets", targets.size());

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
                        targets.size(),
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
                    if (args.useJavaProcess) {
                        int processingParitionSize;
                        if (cdsPartitionSize > 0) {
                            serviceArgList.add(new ServiceArg("-partitionSize", cdsPartitionSize));
                            processingParitionSize = cdsPartitionSize;
                        } else {
                            processingParitionSize = 100;
                        }
                        // number of parallel searches is (nmasks * ntargets) / partitionSize
                        // each MIP requires about 2.6M and typicall we count the approx. # of images
                        // that need to be resident in mem + 1 for the future
                        // if gradient search is required we request more memory
                        double memPerCDS;
                        if (search.useGradientScores()) {
                            memPerCDS = 2.6 * 7;
                        } else {
                            memPerCDS = 2.6 * 3;
                        }
                        double memInMB = ((double) masks.size() * targets.size()  / processingParitionSize) * memPerCDS;
                        ProcessorHelper.setRequiredMemoryInGB(colorDepthProcessingResources, (int)Math.ceil(memInMB / 1024.));
                        cdsComputation = runJavaProcessBasedColorDepthSearch(jacsServiceData, serviceArgList, colorDepthProcessingResources);
                    } else {
                        // Curve fitting using https://www.desmos.com/calculator
                        // This equation was found using https://mycurvefit.com
                        int desiredNodes = (int)Math.round(0.2 * Math.pow(targets.size(), 0.32));

                        int numNodes = Math.max(Math.min(desiredNodes, maxNodes), minNodes);
                        int filesPerNode = (int)Math.round(targets.size() / (double)numNodes);
                        logger.info("Using {} worker nodes, with {} files per node", numNodes, filesPerNode);

                        serviceArgList.add(new ServiceArg("-useSpark"));
                        serviceArgList.add(new ServiceArg("-numNodes", numNodes));
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
                    if (!colorDepthResult.getMaskResults().isEmpty()) {
                        legacyDomainDao.addColorDepthSearchResult(jacsServiceData.getOwnerKey(), search.getId(), colorDepthResult);
                        logger.info("Updated search {} }with new result {}", search, colorDepthResult);
                        return updateServiceResult(jacsServiceData, Reference.createFor(colorDepthResult));
                    } else  {
                        return updateServiceResult(jacsServiceData, null);
                    }
                });
    }

    private ColorDepthResult collectColorDepthResults(JacsServiceData jacsServiceData, ColorDepthParameters searchParameters, Set<String> maskIds, List<File> cdMatchFiles) {
        ColorDepthResult colorDepthResult = new ColorDepthResult();
        colorDepthResult.setParameters(searchParameters);
        int maxResultsPerMask = searchParameters.getMaxResultsPerMask() == null
                ? 200
                : searchParameters.getMaxResultsPerMask();
        cdMatchFiles.stream()
                .filter(cdsMatchesFile -> maskIds.contains(FileUtils.getFileNameOnly(cdsMatchesFile.toPath())))
                .map(cdsMatchesFile -> {
                    try {
                        return objectMapper.readValue(cdsMatchesFile, CDMaskMatches.class);
                    } catch (IOException e) {
                        logger.error("Error reading results from {}", cdsMatchesFile, e);
                        return new CDMaskMatches();
                    }
                })
                .filter(CDMaskMatches::hasResults)
                .forEach(cdMaskMatches -> {
                    ColorDepthMaskResult maskResult = new ColorDepthMaskResult();
                    maskResult.setMaskRef(Reference.createFor("ColorDepthMask" + "#" + cdMaskMatches.getMaskId()));
                    cdMaskMatches.getResults().stream()
                            .limit(maxResultsPerMask)
                            .map(cdsMatchResult -> {
                                ColorDepthMatch match = new ColorDepthMatch();
                                match.setMatchingImageRef(Reference.createFor(getColorDepthImage(jacsServiceData.getOwnerKey(), cdsMatchResult.getImageName())));
                                match.setImageRef(Reference.createFor(getColorDepthImage(jacsServiceData.getOwnerKey(), cdsMatchResult.getCdmPath())));
                                match.setMatchingPixels(cdsMatchResult.getMatchingPixels());
                                match.setMatchingPixelsRatio(cdsMatchResult.getMatchingRatio());
                                match.setGradientAreaGap(cdsMatchResult.getGradientAreaGap());
                                match.setHighExpressionArea(cdsMatchResult.getHighExpressionArea());
                                match.setScore(cdsMatchResult.getMatchingPixels());
                                match.setScorePercent(cdsMatchResult.getMatchingRatio());
                                return match;
                            })
                            .forEach(maskResult::addMatch);
                    colorDepthResult.getMaskResults().add(maskResult);
                });
        try {
            if (colorDepthResult.getMaskResults().isEmpty()) {
                return colorDepthResult;
            } else {
                ColorDepthResult persistedColorDepthResult = legacyDomainDao.save(jacsServiceData.getOwnerKey(), colorDepthResult);
                logger.info("Saved {}", persistedColorDepthResult);
                return persistedColorDepthResult;
            }
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
        serviceArgList.add(new ServiceArg("-cdMatchesDir",  cdMatchesDirname));
        serviceArgList.add(new ServiceArg("-negativeRadius", negativeRadius));
        serviceArgList.add(new ServiceArg("-pixColorFluctuation", pixColorFluctuation));
        serviceArgList.add(new ServiceArg("-xyShift", xyShift));
        serviceArgList.add(new ServiceArg("-mirrorMask", mirrorMask));
        serviceArgList.add(new ServiceArg("-pctPositivePixels", pctPositivePixels));
        serviceArgList.add(new ServiceArg("-withGradientScores", withGradScores));
        return serviceArgList;
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
                    Reference sampleRef =  mask.getSample();
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
        return cdsTargets.stream()
                .flatMap(targetLibraryIdentifier -> colorDepthLibraryDao.getLibraryWithVariants(targetLibraryIdentifier).stream())
                .flatMap(targetLibrary -> {
                    List<ColorDepthImage> cdmips;
                    Map<Reference, ColorDepthImage> indexedLibraryMIPs;
                    List<ColorDepthImage> libraryMIPs = colorDepthImageDao.streamColorDepthMIPs(
                            new ColorDepthImageQuery()
                                    .withAlignmentSpace(alignmentSpace)
                                    .withLibraryIdentifiers(Collections.singleton(targetLibrary.getIdentifier())))
                            .collect(Collectors.toList());
                    if (useSegmentation) {
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
                                .map(mip -> mip.getFilepath())
                                .collect(Collectors.toSet());
                        cdmipZgapMasks = colorDepthImageDao.streamColorDepthMIPs(
                                new ColorDepthImageQuery()
                                        .withAlignmentSpace(alignmentSpace)
                                        .withLibraryIdentifiers(ColorDepthLibraryUtils.selectVariantCandidates(targetLibrary, ImmutableSet.of("zgap", "zgapmask")).stream()
                                                .map(ColorDepthLibrary::getIdentifier)
                                                .collect(Collectors.toSet())))
                                .map(mip -> mip.getFilepath())
                                .collect(Collectors.toSet());
                    } else {
                        cdmipGradients = Collections.emptySet();
                        cdmipZgapMasks = Collections.emptySet();
                    }
                    return cdmips.stream()
                            .map(cdmi -> {
                                Reference sampleRef =  cdmi.getSampleRef();
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
                                            .ifPresent(variantPath -> targetMetadata.addVariant("gradient", variantPath));
                                    ColorDepthLibraryUtils.selectVariantCandidates(targetLibrary, ImmutableSet.of("zgap", "zgapmask")).stream()
                                            .map(ColorDepthLibrary::getVariant)
                                            .flatMap(variantName -> CDMMetadataUtils.variantPaths(
                                                    variantName,
                                                    Paths.get(cdmi.getFilepath()),
                                                    cdmi.getAlignmentSpace(),
                                                    cdmipLibraries,
                                                    vp -> cdmipZgapMasks.contains(vp.toString())).stream())
                                            .findFirst()
                                            .ifPresent(variantPath -> targetMetadata.addVariant("zgap", variantPath));
                                }
                                return targetMetadata;
                            });
                })
                .collect(Collectors.toList());
    }

    private ColorDepthImage getColorDepthImage(String ownerKey, String filepath) {
        ColorDepthImage colorDepthImage = legacyDomainDao.getColorDepthImageByPath(ownerKey, filepath);
        if (colorDepthImage == null) {
            throw new IllegalStateException("Could not find result file in database:"+ filepath);
        }
        return colorDepthImage;
    }

    private IntegratedColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new IntegratedColorDepthSearchArgs());
    }
}

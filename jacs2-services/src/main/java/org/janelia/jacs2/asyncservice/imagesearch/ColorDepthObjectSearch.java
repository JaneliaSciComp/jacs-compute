package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
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
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.access.domain.dao.ColorDepthImageQuery;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.gui.cdmip.CDSLibraryParam;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthMask;
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
public class ColorDepthObjectSearch extends AbstractServiceProcessor<Boolean> {

    private final int minNodes;
    private final int maxNodes;

    static class IntegratedColorDepthSearchArgs extends ServiceArgs {
        @Parameter(names = "-searchId", description = "GUID of the ColorDepthSearch object to use", required = true)
        Long searchId;
        @Parameter(names = "-maskId", description = "GUID of the ColorDepthMask object to use. If this is empty, all listed masks are searched.")
        Long maskId;
        @Parameter(names = "-runMasksWithoutResults", description = "If a mask id is provided, should other masks also be run if they don't have results yet?")
        boolean runMasksWithoutResults = true;
        @Parameter(names = "-use-java-process", description = "If set it uses java process based search; the default is spark based search")
        boolean useJavaProcess = false;
    }

    private final WrappedServiceProcessor<SparkColorDepthFileSearch, List<File>> sparkColorDepthFileSearch;
    private final WrappedServiceProcessor<JavaProcessColorDepthFileSearch, List<File>> javaProcessColorDepthFileSearch;
    private final LegacyDomainDao legacyDomainDao;
    private final ColorDepthImageDao colorDepthImageDao;
    private final ObjectMapper objectMapper;


    @Inject
    ColorDepthObjectSearch(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           LegacyDomainDao legacyDomainDao,
                           @IntPropertyValue(name = "service.colorDepthSearch.minNodes", defaultValue = 1) Integer minNodes,
                           @IntPropertyValue(name = "service.colorDepthSearch.maxNodes", defaultValue = 8) Integer maxNodes,
                           SparkColorDepthFileSearch sparkColorDepthFileSearch,
                           JavaProcessColorDepthFileSearch javaProcessColorDepthFileSearch,
                           ColorDepthImageDao colorDepthImageDao,
                           ObjectMapper objectMapper,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.legacyDomainDao = legacyDomainDao;
        this.minNodes = minNodes;
        this.maxNodes = maxNodes;
        this.sparkColorDepthFileSearch = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, sparkColorDepthFileSearch);
        this.javaProcessColorDepthFileSearch = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, javaProcessColorDepthFileSearch);
        this.colorDepthImageDao = colorDepthImageDao;
        this.objectMapper = objectMapper;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthObjectSearch.class, new IntegratedColorDepthSearchArgs());
    }

    @Override
    public ServiceResultHandler<Boolean> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Boolean>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public Boolean getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleProcessorResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<Boolean>> process(JacsServiceData jacsServiceData) {
        IntegratedColorDepthSearchArgs args = getArgs(jacsServiceData);

        Stopwatch sparkAppWatch = Stopwatch.createStarted();
        logger.info("Executing ColorDepthSearch#{} with ColorDepthMask#{}", args.searchId, args.maskId);

        ColorDepthSearch search = legacyDomainDao.getDomainObject(jacsServiceData.getOwnerKey(),
                ColorDepthSearch.class, args.searchId);

        if (search == null) {
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#"+args.searchId+" not found");
        }

        List<CDMMetadata> targets = getTargetColorDepthImages(search.getAlignmentSpace(), search.getLibraries());
        logger.info("Searching {} total targets", targets.size());

        // Curve fitting using https://www.desmos.com/calculator
        // This equation was found using https://mycurvefit.com
        int desiredNodes = (int)Math.round(0.2 * Math.pow(targets.size(), 0.32));

        int numNodes = Math.max(Math.min(desiredNodes, maxNodes), minNodes);
        int filesPerNode = (int)Math.round(targets.size() / (double)numNodes);
        logger.info("Using {} worker nodes, with {} files per node", numNodes, filesPerNode);

        // Create temporary file with paths to search
        JacsServiceFolder workingDirectory = getWorkingDirectory(jacsServiceData);
        File colorDepthTargetsFile = workingDirectory.getServiceFolder().resolve("colorDepthTargets.json").toFile();
        try {
            objectMapper.writeValue(FileUtils.openOutputStream(colorDepthTargetsFile), targets);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }

        Map<Integer, List<CDMMetadata>> masksWithThresholds = getMasksWithThresholds(search.getMasks());
        Pair<String, String> maskFilesAndThresholds = masksWithThresholds.entrySet().stream().
                map(maskEntry -> {
                    File colorDepthMasksFile = workingDirectory.getServiceFolder()
                            .resolve("colorDepthMasks-" + maskEntry.getKey() + ".json").toFile();
                    try {
                        objectMapper.writeValue(FileUtils.openOutputStream(colorDepthMasksFile), maskEntry.getValue());
                    } catch (IOException e) {
                        throw new ComputationException(jacsServiceData, e);
                    }
                    return ImmutablePair.of(colorDepthMasksFile.toString(), maskEntry.getKey().toString());
                })
                .reduce(ImmutablePair.of("", ""),
                        (me1, me2) -> {
                            if (StringUtils.isBlank(me1.getLeft())) {
                                return me2;
                            } else {
                                return ImmutablePair.of(
                                        me1.getLeft() + "," + me2.getLeft(),
                                        me1.getRight() + "," + me2.getRight());
                            }
                        });

        List<ServiceArg> serviceArgList = new ArrayList<>();
        serviceArgList.add(new ServiceArg("-masksFiles", maskFilesAndThresholds.getLeft()));
        serviceArgList.add(new ServiceArg("-masksThresholds", maskFilesAndThresholds.getRight()));
        serviceArgList.add(new ServiceArg("-targetsFile", colorDepthTargetsFile.getAbsolutePath()));
        serviceArgList.add(new ServiceArg("-cdMatchesDir", workingDirectory.getServiceFolder().resolve("cdsMatches").toString()));
        serviceArgList.add(new ServiceArg("-numNodes", numNodes));
        serviceArgList.add(new ServiceArg("-negativeRadius", search.getParameters().getNegativeRadius()));
        if (search.getDataThreshold() != null) {
            serviceArgList.add(new ServiceArg("-dataThreshold", search.getDataThreshold()));
        }
        if (search.getPixColorFluctuation() != null) {
            serviceArgList.add(new ServiceArg("-pixColorFluctuation", search.getPixColorFluctuation()));
        }
        if (search.getXyShift() != null) {
            serviceArgList.add(new ServiceArg("-xyShift", search.getXyShift()));
        }

        if (search.getMirrorMask() != null && search.getMirrorMask()) {
            serviceArgList.add(new ServiceArg("-mirrorMask"));
        }

        // Fallback for older searches without max results
        final Integer maxResultsPerMask = search.getParameters().getMaxResultsPerMask() == null
                ? 200 : search.getParameters().getMaxResultsPerMask();

        ServiceComputation<JacsServiceResult<List<File>>> cdsComputation;
        if (args.useJavaProcess) {
            cdsComputation = runJavaProcessBasedColorDepthSearch(jacsServiceData, serviceArgList);
        } else {
            serviceArgList.add(new ServiceArg("-useSpark"));
            cdsComputation = runSparkBasedColorDepthSearch(jacsServiceData, serviceArgList);
        }
        return cdsComputation
                .thenApply((JacsServiceResult<List<File>> result) -> {
                    ColorDepthResult colorDepthResult = new ColorDepthResult();
                    colorDepthResult.setParameters(search.getParameters());
                    result.getResult().forEach(cdsMatchesFile -> {

                    });
                    return new JacsServiceResult<>(jacsServiceData, Boolean.TRUE);
                });
    }

    private ServiceComputation<JacsServiceResult<List<File>>> runJavaProcessBasedColorDepthSearch(JacsServiceData jacsServiceData, List<ServiceArg> serviceArgList) {
        return javaProcessColorDepthFileSearch.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Java process based color depth search")
                        .build(),
                serviceArgList.toArray(new ServiceArg[0]));
    }

    private ServiceComputation<JacsServiceResult<List<File>>> runSparkBasedColorDepthSearch(JacsServiceData jacsServiceData,
                                                                                            List<ServiceArg> serviceArgList) {
        return sparkColorDepthFileSearch.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Spark based color depth search")
                        .build(),
                serviceArgList.toArray(new ServiceArg[0]));
    }

    private Map<Integer, List<CDMMetadata>> getMasksWithThresholds(List<Reference> maskRefs) {
        List<ColorDepthMask> masks = legacyDomainDao.getDomainObjectsAs(maskRefs, ColorDepthMask.class);
        return masks.stream()
                .map(mask -> {
                    Reference sampleRef =  mask.getSample();
                    CDMMetadata maskMetadata = new CDMMetadata();
                    maskMetadata.setId(mask.getId().toString());
                    maskMetadata.setCdmPath(mask.getFilepath());
                    maskMetadata.setImagePath(mask.getFilepath());
                    maskMetadata.setSampleRef(sampleRef != null ? sampleRef.toString() : null);
                    return ImmutablePair.of(mask.getMaskThreshold(), maskMetadata);
                })
                .collect(Collectors.groupingBy(
                        ImmutablePair::getLeft,
                        Collectors.mapping(ImmutablePair::getRight, Collectors.toList())));
    }

    private List<CDMMetadata> getTargetColorDepthImages(String alignmentSpace, List<CDSLibraryParam> targetLibraries) {
        return targetLibraries.stream()
                .flatMap(targetLibrary -> {
                    Stream<ColorDepthImage> cdmiStream;
                    Map<Reference, ColorDepthImage> indexedLibraryMIPs;
                    List<ColorDepthImage> libraryMIPs = colorDepthImageDao.streamColorDepthMIPs(
                            new ColorDepthImageQuery()
                                    .withAlignmentSpace(alignmentSpace)
                                    .withLibraryIdentifiers(Collections.singleton(targetLibrary.getLibraryName())))
                            .collect(Collectors.toList());
                    if (targetLibrary.hasSearchableVariant()) {
                        cdmiStream = colorDepthImageDao.streamColorDepthMIPs(
                                new ColorDepthImageQuery()
                                        .withAlignmentSpace(alignmentSpace)
                                        .withLibraryIdentifiers(Collections.singleton(targetLibrary.getLibraryName() + "_" + targetLibrary.getSearchableVariant())));
                        indexedLibraryMIPs = libraryMIPs.stream()
                                .collect(Collectors.toMap(Reference::createFor, Function.identity()));
                    } else {
                        logger.info("No segmentation variant set for {}", targetLibrary.getLibraryName());
                        cdmiStream = libraryMIPs.stream();
                        indexedLibraryMIPs = Collections.emptyMap();
                    }
                    return cdmiStream
                            .map(cdmi -> {
                                Reference sampleRef =  cdmi.getSampleRef();
                                Reference sourceImageRef = cdmi.getSourceImageRef();
                                ColorDepthImage sourceMIP = indexedLibraryMIPs.get(sourceImageRef);
                                CDMMetadata targetMetadata = new CDMMetadata();
                                targetMetadata.setId(cdmi.getId().toString());
                                targetMetadata.setLibraryName(cdmi.getLibraries().stream().findFirst().orElse(null));
                                targetMetadata.setAlignmentSpace(cdmi.getAlignmentSpace());
                                Set<String> mipLibraries;
                                if (sourceMIP == null) {
                                    targetMetadata.setCdmPath(cdmi.getFilepath());
                                    mipLibraries = cdmi.getLibraries();
                                } else {
                                    targetMetadata.setCdmPath(sourceMIP.getFilepath());
                                    mipLibraries = ImmutableSet.<String>builder()
                                            .addAll(sourceMIP.getLibraries())
                                            .addAll(cdmi.getLibraries())
                                            .build();
                                }
                                targetMetadata.setCdmPath(cdmi.getFilepath());
                                targetMetadata.setImagePath(cdmi.getFilepath());
                                targetMetadata.setSampleRef(sampleRef != null ? sampleRef.toString() : null);
                                targetMetadata.setRelatedImageRefId(sourceImageRef != null ? sourceImageRef.toString() : null);
                                if (targetLibrary.hasGradientVariant()) {
                                    Set<Path> gradientVariantPaths = CDMMetadataUtils.variantPaths(
                                            Paths.get(targetLibrary.getGradientVariant()),
                                            Paths.get(cdmi.getFilepath()),
                                            cdmi.getAlignmentSpace(),
                                            mipLibraries);
                                    targetMetadata.addVariant(
                                            "gradient",
                                            CDMMetadataUtils.variantCandidatesStream(
                                                    gradientVariantPaths,
                                                    targetMetadata.getImagePath()).findFirst().orElse(null)
                                    );
                                }
                                if (targetLibrary.hasZgapMaskVariant()) {
                                    Set<Path> zgapMasksVariantPaths = CDMMetadataUtils.variantPaths(
                                            Paths.get(targetLibrary.getZgapMaskVariant()),
                                            Paths.get(cdmi.getFilepath()),
                                            cdmi.getAlignmentSpace(),
                                            mipLibraries);
                                    targetMetadata.addVariant(
                                            "zgap",
                                            CDMMetadataUtils.variantCandidatesStream(
                                                    zgapMasksVariantPaths,
                                                    targetMetadata.getImagePath()).findFirst().orElse(null)
                                    );
                                }
                                return targetMetadata;
                            });
                })
                .collect(Collectors.toList());
    }

    private ColorDepthImage getSourceColorDepthImage(String ownerKey, String filepath) {
        ColorDepthImage colorDepthImage = legacyDomainDao.getColorDepthImageByPath(ownerKey, filepath);
        if (colorDepthImage == null) {
            throw new IllegalStateException("Could not find result file in database:"+ filepath);
        }
        ColorDepthImage sourceImage;
        if (colorDepthImage.getSourceImageRef() != null) {
            sourceImage = legacyDomainDao.getDomainObject(ownerKey, ColorDepthImage.class, colorDepthImage.getSourceImageRef().getTargetId());
            if (sourceImage == null) {
                throw new IllegalStateException("Could not find source image " + colorDepthImage.getSourceImageRef() + " referenced by " + colorDepthImage +
                        " while retrieving the color depth image entity for path " + filepath);
            }
        } else {
            sourceImage = null;
        }
        return sourceImage == null ? colorDepthImage : sourceImage;
    }

    private IntegratedColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new IntegratedColorDepthSearchArgs());
    }
}

package org.janelia.jacs2.asyncservice.alignservices;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
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
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("cmtkAlignment")
public class CMTKAlignmentProcessor extends AbstractServiceProcessor<List<String>> {

    private final WrappedServiceProcessor<SingleCMTKAlignmentProcessor, CMTKAlignmentResultFiles> singleCMTKAlignmentProcessor;

    @Inject
    CMTKAlignmentProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           SingleCMTKAlignmentProcessor singleCMTKAlignmentProcessor,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.singleCMTKAlignmentProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, singleCMTKAlignmentProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(CMTKAlignmentProcessor.class, new CMTKAlignmentArgs());
    }

    @Override
    public ServiceResultHandler<List<String>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<String>>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public List<String> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<String>>() {});
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public ServiceComputation<JacsServiceResult<List<String>>> process(JacsServiceData jacsServiceData) {
        CMTKAlignmentArgs args = getArgs(jacsServiceData);
        Map<String, List<Path>> groupedImages = getImageFileNamesStream(args).collect(Collectors.groupingBy((Path imagePath) -> {
            String imageFileName = FileUtils.getFileNameOnly(imagePath);
            int lastSeparatorIndex = imageFileName.lastIndexOf('_');
            Preconditions.checkArgument(lastSeparatorIndex != -1);
            return imageFileName.substring(0, lastSeparatorIndex);
        }, Collectors.toList()));
        Preconditions.checkArgument(StringUtils.isNotBlank(args.outputDir), "An output directory is required");
        Path outputDir = Paths.get(args.outputDir);
        List<ServiceComputation<JacsServiceResult<CMTKAlignmentResultFiles>>> cmtkAlignments =
                groupedImages.entrySet().stream()
                        .map((Map.Entry<String, List<Path>> group) -> {
                            Path groupInputDir = outputDir.resolve(group.getKey()).resolve("images");
                            Path groupOutputDir = outputDir.resolve(group.getKey());
                            if (groupInputDir.equals(groupOutputDir)) {
                                groupInputDir = outputDir.resolve(group.getKey()).resolve("inputImages");
                            }
                            String groupInputDirName = groupInputDir.toString();
                            try {
                                Files.createDirectories(groupInputDir);
                                Files.createDirectories(groupOutputDir);
                                group.getValue().forEach(inputImage -> {
                                    String inputImageName = inputImage.toFile().getName();
                                    Path inputImageLink = Paths.get(groupInputDirName, inputImageName);
                                    try {
                                        if (Files.exists(inputImageLink) && args.overrideGroupedInputs) {
                                            Files.delete(inputImageLink);
                                        }
                                    } catch (IOException deleteExc) {
                                        throw new UncheckedIOException(deleteExc);
                                    }
                                    try {
                                        Files.createSymbolicLink(inputImageLink, inputImage);
                                    } catch (IOException linkExc) {
                                        throw new UncheckedIOException("You can try to use <overrideGroupedInputs> flag or remove previous inputs", linkExc);
                                    }
                                });
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            return singleCMTKAlignmentProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                            .description("Run CMTK alignment for " + group.getKey())
                                            .build(),
                                    new ServiceArg("-inputDir", groupInputDirName),
                                    new ServiceArg("-outputDir", groupOutputDir.toString()),
                                    new ServiceArg("-template", args.template),
                                    new ServiceArg("-a", args.runAffine.toString()),
                                    new ServiceArg("-w", args.runWarp.toString()),
                                    new ServiceArg("-r", args.reformattingChannels),
                                    new ServiceArg("-X", args.exploration),
                                    new ServiceArg("-C", args.coarsest),
                                    new ServiceArg("-G", args.gridSpacing),
                                    new ServiceArg("-R", args.refine),
                                    new ServiceArg("-A", args.affineOptions),
                                    new ServiceArg("-W", args.warpOptions),
                                    new ServiceArg("-nthreads", args.numThreads),
                                    new ServiceArg("-verbose", args.verbose));
                        })
                        .collect(Collectors.toList());
        List<ServiceComputation<?>> composableCmtkAlignments = ImmutableList.copyOf(cmtkAlignments);
        return computationFactory.newCompletedComputation(null)
                .thenCombineAll(composableCmtkAlignments, (Object ignored, List<?> results) -> (List<JacsServiceResult<CMTKAlignmentResultFiles>>) results)
                .thenApply(cmkAlignmentResults -> {
                    List<String> cmtkAlignmentResultDirs = cmkAlignmentResults.stream().map(r -> r.getResult().getResultDir()).collect(Collectors.toList());
                    return updateServiceResult(jacsServiceData, cmtkAlignmentResultDirs);
                });
    }

    private CMTKAlignmentArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new CMTKAlignmentArgs());
    }

    private Stream<Path> getImageFileNamesStream(CMTKAlignmentArgs args) {
        if (CollectionUtils.isNotEmpty(args.inputImageFileNames)) {
            return args.inputImageFileNames.stream().map(imageName -> Paths.get(imageName));
        } else {
            return FileUtils.lookupFiles(Paths.get(args.inputDir), 1, "glob:**/*.nrrd");
        }
    }

}

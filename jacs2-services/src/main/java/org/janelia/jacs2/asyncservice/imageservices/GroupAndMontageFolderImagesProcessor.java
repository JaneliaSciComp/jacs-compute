package org.janelia.jacs2.asyncservice.imageservices;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
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

/**
 * Create a square montage from PNGs in a given directory.
 */
@Dependent
@Named("groupAndMontageImages")
public class GroupAndMontageFolderImagesProcessor extends AbstractServiceProcessor<Map<String, String>> {

    static class MontageFolderIntermediateResult {
        final String groupedFilesType;
        final String montageResult;

        MontageFolderIntermediateResult(String groupedFilesType, String montageResult) {
            this.groupedFilesType = groupedFilesType;
            this.montageResult = montageResult;
        }
    }

    static class MontageFolderImagesArgs extends ServiceArgs {
        @Parameter(names = "-input", description = "The name of the input directory.", required = true)
        String inputDir;
        @Parameter(names = "-output", description = "The name of the results directory - if not specified it will put the results in the same directory.")
        String outputDir;
        @Parameter(names = "-montageBaseName", description = "The base name of the montage results")
        String montageBaseName = "montage";
        @Parameter(names = "-imageFilePattern", description = "The extension of the image files from the input directory")
        String imageFilePattern = "glob:**/*.png";
    }

    private final WrappedServiceProcessor<MontageImagesProcessor, File> montageImagesProcessor;

    @Inject
    GroupAndMontageFolderImagesProcessor(ServiceComputationFactory computationFactory,
                                         JacsServiceDataPersistence jacsServiceDataPersistence,
                                         @Any Instance<ExternalProcessRunner> serviceRunners,
                                         @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                         MontageImagesProcessor montageImagesProcessor,
                                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.montageImagesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, montageImagesProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(GroupAndMontageFolderImagesProcessor.class, new MontageFolderImagesArgs());
    }

    @Override
    public ServiceResultHandler<Map<String, String>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Map<String, String>>() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public Map<String, String> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<Map<String, String>>() {});
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public ServiceComputation<JacsServiceResult<Map<String, String>>> process(JacsServiceData jacsServiceData) {
        MontageFolderImagesArgs args = getArgs(jacsServiceData);
        try {
            Path outputDir = getOutputDir(args);
            Files.createDirectories(outputDir);
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
        List<Path> inputFiles = getInputFiles(args.inputDir, args.imageFilePattern);
        Multimap<String, Path> imageGroups = groupFilesBySuffix(inputFiles);
        int tilesPerSide = getMaxTilesPerSide(imageGroups);
        List<ServiceComputation<?>> montageComputations = imageGroups.asMap().entrySet().stream()
                .map(group -> {
                    Path montageOutput = getMontageOutput(args, group.getKey());
                    return montageImagesProcessor.process(
                            new ServiceExecutionContext.Builder(jacsServiceData)
                                    .build(),
                            new ServiceArg("-inputFiles", group.getValue().stream().map(p -> p.toString()).collect(Collectors.joining(","))),
                            new ServiceArg("-tilesPerSide", tilesPerSide),
                            new ServiceArg("-output", montageOutput.toString())
                    ).thenApply(montageResult -> new JacsServiceResult<>(montageResult.getJacsServiceData(), new MontageFolderIntermediateResult(group.getKey(), montageResult.getResult().getAbsolutePath())));
                })
                .collect(Collectors.toList())
                ;
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCombineAll(montageComputations, (sd, montageComputationsResults) -> {
                    List<JacsServiceResult<MontageFolderIntermediateResult>> montageResults = (List<JacsServiceResult<MontageFolderIntermediateResult>>) montageComputationsResults;
                    return montageResults.stream()
                            .map(JacsServiceResult::getResult)
                            .collect(Collectors.toMap(
                                    mr -> mr.groupedFilesType,
                                    mr -> mr.montageResult));
                })
                .thenApply(montageResults -> updateServiceResult(jacsServiceData, montageResults))
                ;
    }

    private MontageFolderImagesArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new MontageFolderImagesArgs());
    }

    private Path getOutputDir(MontageFolderImagesArgs args) {
        String outputDir = args.outputDir;
        if (StringUtils.isBlank(outputDir)) {
            outputDir = args.inputDir;
        }
        return Paths.get(outputDir);
    }

    private List<Path> getInputFiles(String inputDir, String imageFilePattern) {
        Path inputPath = Paths.get(inputDir);
        List<Path> inputFiles = new ArrayList<>();
        try {
            PathMatcher inputFileMatcher =
                    FileSystems.getDefault().getPathMatcher(imageFilePattern);
            Files.find(inputPath, 1, (p, a) -> inputFileMatcher.matches(p)).forEach(inputFiles::add);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("No image file found in " + inputDir + " that matches the given pattern: " + imageFilePattern);
        }
        return inputFiles;
    }

    /**
     * Groups the files by the suffix which is the last group separated by '_' from the file name.
     * @param files input file list
     * @return the list of files grouped by suffix for example all files ending in '_signal{.someextension}' will be in one group and '_reference{.extension}'
     * will be in another group.
     */
    private Multimap<String, Path> groupFilesBySuffix(List<Path> files) {
        ImmutableListMultimap.Builder<String, Path> builder = ImmutableListMultimap.builder();
        files.stream()
                .map(fp -> new ImmutablePair<>(FileUtils.getFileNameOnly(fp), fp))
                .filter(namePathPair -> namePathPair.getLeft().indexOf('_') >= 0)
                .map(namePathPair -> {
                    int separatorIndex = namePathPair.getLeft().lastIndexOf('_');
                    String suffix = namePathPair.getLeft().substring(separatorIndex);
                    return new ImmutablePair<>(suffix, namePathPair.getRight());
                })
                .forEach(namePathPair -> builder.put(namePathPair.getLeft(), namePathPair.getRight()));
        return builder.build();
    }

    private int getMaxTilesPerSide(Multimap<String, Path> imageGroups) {
        int maxTilesPerGroup = imageGroups.asMap().entrySet().stream().reduce(0, (max, entry) -> entry.getValue().size(), Math::max);
        return (int)Math.ceil(Math.sqrt(maxTilesPerGroup));
    }

    private Path getMontageOutput(MontageFolderImagesArgs args, String suffix) {
        String prefix = StringUtils.defaultIfBlank(args.montageBaseName, "");
        if ("montage".equalsIgnoreCase(prefix)) {
            prefix = "";
        }
        return FileUtils.getFilePath(getOutputDir(args), prefix, "montage", suffix, ".png");
    }

}

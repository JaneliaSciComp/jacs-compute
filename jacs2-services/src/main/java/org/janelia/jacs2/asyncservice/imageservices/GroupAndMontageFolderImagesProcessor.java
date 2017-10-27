package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
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
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
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

/**
 * Create a square montage from PNGs in a given directory.
 */
@Named("groupAndMontageImages")
public class GroupAndMontageFolderImagesProcessor extends AbstractBasicLifeCycleServiceProcessor<Map<String, String>, List<GroupAndMontageFolderImagesProcessor.MontageFolderIntermediateResult>> {

    static class MontageFolderIntermediateResult {
        private final Number montageServiceId;
        private final String groupedFilesType;
        private final String montageName;

        public MontageFolderIntermediateResult(Number montageServiceId, String groupedFilesType, String montageName) {
            this.montageServiceId = montageServiceId;
            this.groupedFilesType = groupedFilesType;
            this.montageName = montageName;
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

    private final MontageImagesProcessor montageImagesProcessor;

    @Inject
    GroupAndMontageFolderImagesProcessor(ServiceComputationFactory computationFactory,
                                         JacsServiceDataPersistence jacsServiceDataPersistence,
                                         @Any Instance<ExternalProcessRunner> serviceRunners,
                                         @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                         MontageImagesProcessor montageImagesProcessor,
                                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.montageImagesProcessor = montageImagesProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(GroupAndMontageFolderImagesProcessor.class, new MontageFolderImagesArgs());
    }

    @Override
    public ServiceResultHandler<Map<String, String>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Map<String, String>>() {

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public Map<String, String> collectResult(JacsServiceResult<?> depResults) {
                List<MontageFolderIntermediateResult> results = (List<MontageFolderIntermediateResult>) depResults.getResult();
                return results.stream().collect(Collectors.toMap(r -> r.groupedFilesType, r -> r.montageName));
            }

            @Override
            public Map<String, String> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<Map<String, String>>() {});
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            MontageFolderImagesArgs args = getArgs(jacsServiceData);
            Path outputDir = getOutputDir(args);
            Files.createDirectories(outputDir);
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<List<MontageFolderIntermediateResult>> submitServiceDependencies(JacsServiceData jacsServiceData) {
        MontageFolderImagesArgs args = getArgs(jacsServiceData);

        List<Path> inputFiles = getInputFiles(args.inputDir, args.imageFilePattern);
        Multimap<String, Path> imageGroups = groupFilesBySuffix(inputFiles);
        int tilesPerSide = getMaxTilesPerSide(imageGroups);
        List<MontageFolderIntermediateResult> montageServiceResults = imageGroups.asMap().entrySet().stream()
                .map(group -> {
                    Path montageOutput = getMontageOutput(args, group.getKey());
                    JacsServiceData montageServiceRef = montageImagesProcessor.createServiceData(
                            new ServiceExecutionContext.Builder(jacsServiceData)
                                    .build(),
                            new ServiceArg("-inputFiles", group.getValue().stream().map(p -> p.toString()).collect(Collectors.joining(","))),
                            new ServiceArg("-tilesPerSide", tilesPerSide),
                            new ServiceArg("-output", montageOutput.toString())
                    );
                    JacsServiceData montageService = submitDependencyIfNotFound(montageServiceRef);
                    return new MontageFolderIntermediateResult(montageService.getId(), group.getKey(), montageOutput.toString());
                })
                .collect(Collectors.toList());
        return new JacsServiceResult<>(jacsServiceData, montageServiceResults);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<List<MontageFolderIntermediateResult>>> processing(JacsServiceResult<List<MontageFolderIntermediateResult>> depResults) {
        return computationFactory.newCompletedComputation(depResults);
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

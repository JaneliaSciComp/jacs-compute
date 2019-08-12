package org.janelia.jacs2.asyncservice.imagesearch;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.spark.AbstractSparkProcessor;
import org.janelia.jacs2.asyncservice.spark.LSFSparkClusterLauncher;
import org.janelia.jacs2.asyncservice.spark.SparkApp;
import org.janelia.jacs2.asyncservice.spark.SparkCluster;
import org.janelia.jacs2.asyncservice.utils.DataHolder;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service for searching color depth mask projections at scale by using a Spark service. Multiple directories can be
 * searched. You can perform multiple searches on the same images already in memory by specifying multiple mask files
 * as input.
 *
 * The results are a set of tab-delimited files, one per input mask. The first line of each output file is the
 * filepath of the mask that was used to generated the results. The rest of the lines list matching images in this
 * format;
 * <score>\t<filepath>
 *
 * Depends on a compiled jar from the colordepthsearch project.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthFileSearch")
public class ColorDepthFileSearch extends AbstractSparkProcessor<List<File>> {

    private static final String RESULTS_FILENAME_SUFFIX = "_results.txt";

    private final ComputeAccounting clusterAccounting;
    private final long searchTimeoutInMillis;
    private final long searchIntervalCheckInMillis;
    private final String jarPath;

    static class ColorDepthSearchArgs extends ServiceArgs {
        @Parameter(names = {"-inputFiles"}, description = "Comma-delimited list of mask files", required = true)
        String inputFiles;

        @Parameter(names = {"-searchDirs"}, description = "Comma-delimited list of directories containing the color depth MIPs to search")
        String searchDirs;

        @Parameter(names = {"-searchImageFile"}, description = "Filepath to a text file containing all a list of paths to search")
        String searchImageFile;

        @Parameter(names = {"-dataThreshold"}, description = "Data threshold")
        Integer dataThreshold;

        @Parameter(names = {"-maskThresholds"}, description = "Mask thresholds", variableArity = true)
        List<Integer> maskThresholds;

        @Parameter(names = {"-pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
        Double pixColorFluctuation;

        @Parameter(names = {"-xyShift"}, description = "Number of pixels to try shifting in XY plane")
        Integer xyShift = 0;

        @Parameter(names = {"-mirrorMask"}, description = "Should the mask be mirrored across the Y axis?")
        Boolean mirrorMask = false;

        @Parameter(names = {"-pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        Double pctPositivePixels;

        @Parameter(names = {"-numNodes"}, description = "Number of worker nodes")
        Integer numNodes;

        @Parameter(names = {"-minWorkerNodes"}, description = "Minimum number of required worker nodes")
        Integer minWorkerNodes;

        @Parameter(names = {"-parallelism"}, description = "Parallelism")
        Integer parallelism;
    }

    @Inject
    ColorDepthFileSearch(ServiceComputationFactory computationFactory,
                         JacsServiceDataPersistence jacsServiceDataPersistence,
                         @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                         LSFSparkClusterLauncher clusterLauncher,
                         ComputeAccounting clusterAccounting,
                         @IntPropertyValue(name = "service.colorDepthSearch.searchTimeoutInSeconds", defaultValue = 1200) int searchTimeoutInSeconds,
                         @IntPropertyValue(name = "service.colorDepthSearch.searchIntervalCheckInMillis", defaultValue = 5000) int searchIntervalCheckInMillis,
                         @IntPropertyValue(name = "service.colorDepthSearch.numNodes", defaultValue = 6) Integer defaultNumNodes,
                         @IntPropertyValue(name = "service.colorDepthSearch.minRequiredWorkers", defaultValue = 1) Integer defaultMinRequiredWorkers,
                         @StrPropertyValue(name = "service.colorDepthSearch.jarPath") String jarPath,
                         Logger log) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, clusterLauncher, defaultNumNodes, defaultMinRequiredWorkers, log);
        this.clusterAccounting = clusterAccounting;
        this.searchTimeoutInMillis = searchTimeoutInSeconds > 0 ? searchTimeoutInSeconds * 1000 : -1;
        this.searchIntervalCheckInMillis = searchIntervalCheckInMillis;
        this.jarPath = jarPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthFileSearch.class, new ColorDepthSearchArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return true;
            }
            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<File>>> process(JacsServiceData jacsServiceData) {
        ColorDepthSearchArgs args = getArgs(jacsServiceData);

        // prepare service directories
        JacsServiceFolder serviceWorkingFolder = prepareSparkJobDirs(jacsServiceData);

        // start the cluster
        DataHolder<SparkCluster> runningClusterState = new DataHolder<>();
        return startCluster(jacsServiceData, args, serviceWorkingFolder)

                // Now run the search
                .thenCompose((SparkCluster cluster) -> {
                    runningClusterState.setData(cluster);
                    jacsServiceDataPersistence.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT,
                                    String.format("Running app using spark job on %s (%s)",
                                            cluster.getMasterURI(),
                                            cluster.getMasterJobId())));
                    return runApp(jacsServiceData, args, cluster); // the computation completes when the app completes
                })

                // This is the "finally" block. We must always kill the cluster no matter what happens above.
                // We don't attempt to extract the cluster from cond, because that may be null if there's an exception.
                // Instead, we use the instance from the surrounding closure, which is guaranteed to work.
                .whenComplete((app, exc) -> {
                    if (runningClusterState.isPresent()) {
                        jacsServiceDataPersistence.addServiceEvent(
                                jacsServiceData,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_STOP_JOB,
                                        String.format("Stop spark cluster on %s (%s)",
                                                runningClusterState.getData().getMasterURI(),
                                                runningClusterState.getData().getMasterJobId())));
                        runningClusterState.getData().stopCluster();
                    }
                })

                // Deal with the results
                .thenApply((app) -> {
                    List<File> resultsFiles = FileUtils.lookupFiles(
                                serviceWorkingFolder.getServiceFolder(), 1, "glob:**/*"+RESULTS_FILENAME_SUFFIX)
                            .map(Path::toFile)
                            .collect(Collectors.toList());

                    return updateServiceResult(jacsServiceData, resultsFiles);
                });
    }

    private ColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ColorDepthSearchArgs());
    }

    private ServiceComputation<SparkCluster> startCluster(JacsServiceData jacsServiceData, ColorDepthSearchArgs args, JacsServiceFolder serviceWorkingFolder) {
        int numNodes;
        if (args.numNodes != null) {
            numNodes = args.numNodes;
        } else {
            numNodes = defaultNumNodes;
        }
        int minRequiredWorkers;
        if (args.minWorkerNodes != null) {
            if (args.minWorkerNodes >= 0 && args.minWorkerNodes <= numNodes) {
                minRequiredWorkers = args.minWorkerNodes;
            } else if (args.minWorkerNodes < 0) {
                minRequiredWorkers = 0;
            } else {
                minRequiredWorkers = numNodes;
            }
        } else {
            if (defaultMinRequiredWorkers <= numNodes) {
                minRequiredWorkers = defaultMinRequiredWorkers;
            } else {
                minRequiredWorkers = numNodes;
            }
        }
        return sparkClusterLauncher.startCluster(
                numNodes,
                minRequiredWorkers,
                serviceWorkingFolder.getServiceFolder(),
                Paths.get(jacsServiceData.getOutputPath()),
                Paths.get(jacsServiceData.getErrorPath()),
                clusterAccounting.getComputeAccount(jacsServiceData),
                getSparkDriverMemory(jacsServiceData.getResources()),
                getSparkExecutorMemory(jacsServiceData.getResources()),
                getSparkLogConfigFile(jacsServiceData.getResources()),
                searchTimeoutInMillis > 0 ? (int) (Duration.ofMillis(searchTimeoutInMillis).toMinutes()+ 1) : -1);
    }

    private ServiceComputation<SparkApp> runApp(JacsServiceData jacsServiceData, ColorDepthSearchArgs args, SparkCluster cluster) {
        logger.trace("Run color depth with {}", args);

        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        prepareDir(jacsServiceData.getOutputPath());
        prepareDir(jacsServiceData.getErrorPath());

        List<String> inputFiles = new ArrayList<>();
        List<String> outputFiles = new ArrayList<>();
        Set<Path> outputPaths = new HashSet<>();

        for(String inputFile : args.inputFiles.split(",")) {
            inputFiles.add(inputFile);
            String name = FileUtils.getFileNameOnly(inputFile);

            int i = 1;
            Path outputPath;
            do {
                String discriminator = i == 1 ? "" : "_" + i;
                outputPath = serviceWorkingFolder.getServiceFolder(name + discriminator + RESULTS_FILENAME_SUFFIX);
                i++;
            } while (outputPaths.contains(outputPath));

            outputPaths.add(outputPath);
            outputFiles.add(outputPath.toFile().getAbsolutePath());
        }

        List<String> appArgs = new ArrayList<>();

        appArgs.add("-m");
        appArgs.addAll(inputFiles);

        if (args.searchDirs != null) {
            appArgs.add("-i");
            appArgs.add(args.searchDirs);
        }

        if (args.maskThresholds != null && !args.maskThresholds.isEmpty()) {
            appArgs.add("--maskThresholds");
            for (Integer maskThreshold : args.maskThresholds) {
                appArgs.add(maskThreshold.toString());
            }
        }

        if (args.dataThreshold != null) {
            appArgs.add("--dataThreshold");
            appArgs.add(args.dataThreshold.toString());
        }

        if (args.pixColorFluctuation != null) {
            appArgs.add("--pixColorFluctuation");
            appArgs.add(args.pixColorFluctuation.toString());
        }

        if (args.xyShift != null) {
            appArgs.add("--xyShift");
            appArgs.add(args.xyShift.toString());
        }

        if (args.mirrorMask != null && args.mirrorMask) {
            appArgs.add("--mirrorMask");
        }

        if (args.pctPositivePixels != null) {
            appArgs.add("--pctPositivePixels");
            appArgs.add(args.pctPositivePixels.toString());
        }

        appArgs.add("-o");
        appArgs.addAll(outputFiles);

        int parallelism;
        if (args.parallelism != null) {
            parallelism = args.parallelism;
        } else {
            parallelism = 0;
        }

        logger.info("Starting Spark application {} with {}", jarPath, appArgs);
        return cluster.runApp(jarPath,
                null,
                parallelism,
                jacsServiceData.getOutputPath(),
                jacsServiceData.getErrorPath(),
                searchIntervalCheckInMillis, searchTimeoutInMillis,
                appArgs);
    }

}

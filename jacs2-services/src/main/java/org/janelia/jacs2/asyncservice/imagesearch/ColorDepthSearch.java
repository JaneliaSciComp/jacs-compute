package org.janelia.jacs2.asyncservice.imagesearch;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.spark.SparkApp;
import org.janelia.jacs2.asyncservice.common.spark.SparkCluster;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Service for searching color depth mask projections.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthSearch")
public class ColorDepthSearch extends AbstractServiceProcessor<ColorDepthSearchResults> {

    private static final long CLUSTER_START_TIMEOUT_MS = 1000 * 60 * 10; // 10 minutes
    private static final long SEARCH_TIMEOUT_MS = 1000 * 60 * 20; // 20 minutes

    static class ColorDepthSearchArgs extends ServiceArgs {
        @Parameter(names = "-inputDir", description = "Input directory containing mask files", required = true)
        String inputDir;
        @Parameter(names = "-searchDirs", description = "Comma-delimited list of directories containing the color depth projects to search", required = true)
        String searchDirs;
    }

    @Inject Instance<SparkCluster> clusterSource;

    @Inject
    ColorDepthSearch(ServiceComputationFactory computationFactory,
                     JacsServiceDataPersistence jacsServiceDataPersistence,
                     @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
//                     @PropertyValue(name = "ColorDepthSearch.Jar.Path") String executable,
                     Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthSearch.class, new ColorDepthSearchArgs());
    }

    @Override
    public ServiceResultHandler<ColorDepthSearchResults> getResultHandler() {
        return new AbstractAnyServiceResultHandler<ColorDepthSearchResults>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return true;
            }

            @Override
            public ColorDepthSearchResults collectResult(JacsServiceResult<?> depResults) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColorDepthSearchResults getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleProcessorResult>>() {});
            }
        };
    }

    private DatedObject<SparkCluster> startCluster(JacsServiceData sd) {
        // TODO: Should cache this somehow so it doesn't need to get recomputed each time
        Path workingDir = getWorkingDirectory(sd);
        try {
            SparkCluster cluster = clusterSource.get();
            cluster.startCluster(workingDir, 6);
            logger.info("Waiting until Spark cluster is ready...");
            return new DatedObject(cluster);
        }
        catch (Exception e) {
            throw new ComputationException(sd, e);
        }
    }

    private DatedObject<SparkApp> runApp(JacsServiceData jacsServiceData, SparkCluster cluster) {
        Path workingDir = getWorkingDirectory(jacsServiceData);
        File resultsFile = workingDir.resolve("results.txt").toFile();
        try {
            logger.info("Starting Spark application");
            return new DatedObject(cluster.runApp(null,
                    "/home/rokickik/dev/colormipsearch/target/colormipsearch-1.0-jar-with-dependencies.jar",
                    "-m", "/home/rokickik/mask.tif",
                    "-i", "/home/rokickik/BrainMIPs/ALL_GAL4_MIPs_Whole_Br_2017_0516",
                    "-p", "300",
                    "-o", resultsFile.getAbsolutePath()));
        }
        catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
    }

    @Override
    public ServiceComputation<JacsServiceResult<ColorDepthSearchResults>> process(JacsServiceData jacsServiceData) {

        ColorDepthSearchArgs args = getArgs(jacsServiceData);
        logger.info("Processing args="+args);

        // Create the working directory
        // TODO: this should be managed by a FileNode interface, which has not yet been ported from JACSv1
        Path workingDir = getWorkingDirectory(jacsServiceData);
        try {
            Files.createDirectories(workingDir);
        }
        catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }

        PeriodicallyCheckableState<JacsServiceData> periodicClusterCheck = new PeriodicallyCheckableState<>(jacsServiceData, 2000);
        PeriodicallyCheckableState<JacsServiceData> periodicAppCheck = new PeriodicallyCheckableState<>(jacsServiceData, 5000);

        DatedObject<SparkCluster> cluster = startCluster(jacsServiceData);

        return computationFactory.newCompletedComputation(cluster)

                .thenSuspendUntil((DatedObject<SparkCluster> datedCluster) -> new TimedCond<>(datedCluster,
                        periodicClusterCheck.updateCheckTime() && datedCluster.getObj().isReady(), CLUSTER_START_TIMEOUT_MS))

                .thenApply((ContinuationCond.Cond<DatedObject<SparkCluster>> cond) -> runApp(jacsServiceData, cond.getState().getObj()))

                .thenSuspendUntil((DatedObject<SparkApp> datedApp) -> new TimedCond<>(datedApp,
                        periodicAppCheck.updateCheckTime() && datedApp.getObj().isDone(), SEARCH_TIMEOUT_MS))

                .whenComplete((cond, exc) -> {
                    // This is the "finally" block. We must always kill the cluster no matter what happens above.
                    cluster.getObj().stopCluster();
                })

                .thenApply((cond) -> {
                    ColorDepthSearchResults results = new ColorDepthSearchResults();

                    File resultsFile = workingDir.resolve("results.txt").toFile();
                    try (Scanner scanner = new Scanner(resultsFile)) {
                        while (scanner.hasNext()) {
                            String line = scanner.nextLine();
                            String[] s = line.split("\t");
                            float score = Float.parseFloat(s[0].trim());
                            String filepath = s[1].trim();
                            results.getResultList().add(new ColorDepthSearchResults.ColorDepthSearchResult(score, filepath));
                        }

                    }
                    catch (IOException e) {
                        throw new ComputationException(jacsServiceData, e);
                    }

                    logger.info("SEARCH COMPLETE:");
                    int c = 0;
                    for(ColorDepthSearchResults.ColorDepthSearchResult result : results.getResultList()) {
                        logger.info(result.getScore()+": "+result.getFilename());
                        if (c++>4) break;
                    }

                    return updateServiceResult(jacsServiceData, results);
                });
    }

    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(jacsServiceData.getId());
        if (jacsServiceDataHierarchy == null) {
            jacsServiceDataHierarchy = jacsServiceData;
        }
        setOutputAndErrorPaths(jacsServiceDataHierarchy);
        return jacsServiceDataHierarchy;
    }

    private ColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ColorDepthSearchArgs());
    }
}

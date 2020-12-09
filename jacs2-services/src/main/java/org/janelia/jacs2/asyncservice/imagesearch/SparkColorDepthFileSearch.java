package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;

import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.DelegateServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.spark.SparkAppProcessor;
import org.janelia.jacs2.asyncservice.spark.SparkAppResourceHelper;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Service for searching color depth mask projections at scale by using a Spark service. Multiple directories can be
 * searched. You can perform multiple searches on the same images already in memory by specifying multiple mask files
 * as input.
 * <p>
 * The results are a set of tab-delimited files, one per input mask. The first line of each output file is the
 * filepath of the mask that was used to generated the results. The rest of the lines list matching images in this
 * format;
 * <score>\t<filepath>
 * <p>
 * Depends on a compiled jar from the colordepthsearch project.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthFileSearch")
public class SparkColorDepthFileSearch extends AbstractServiceProcessor<List<File>> {

    static class SparkColorDepthSearchArgs extends ColorDepthSearchArgs {
        @Parameter(names = {"-numNodes", "-numWorkers"}, description = "Number of workers")
        Integer numWorkers;

        @Parameter(names = {"-minWorkerNodes"}, description = "Minimum number of required worker nodes")
        Integer minWorkerNodes;

        @Parameter(names = {"-parallelism"}, description = "Parallelism")
        Integer parallelism;
    }

    private final DelegateServiceProcessor<SparkAppProcessor, Void> sparkAppProcessor;
    private final long searchTimeoutInMillis;

    @Inject
    SparkColorDepthFileSearch(ServiceComputationFactory computationFactory,
                              JacsServiceDataPersistence jacsServiceDataPersistence,
                              @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                              SparkAppProcessor sparkAppProcessor,
                              @IntPropertyValue(name = "service.colorDepthSearch.searchTimeoutInSeconds", defaultValue = 1200) int searchTimeoutInSeconds,
                              @IntPropertyValue(name = "service.colorDepthSearch.searchIntervalCheckInMillis", defaultValue = 5000) int searchIntervalCheckInMillis,
                              @StrPropertyValue(name = "service.spark.sparkHomeDir") String defaultSparkHomeDir,
                              @IntPropertyValue(name = "service.colorDepthSearch.coresPerSparkWorker", defaultValue = 5) int coresPerSparkWorker,
                              @StrPropertyValue(name = "service.colorDepthSearch.jarPath") String jarPath,
                              Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.searchTimeoutInMillis = searchTimeoutInSeconds > 0 ? searchTimeoutInSeconds * 1000L : -1L;
        this.sparkAppProcessor = new DelegateServiceProcessor<>(sparkAppProcessor,
                jacsServiceData -> {
                    SparkColorDepthSearchArgs args = getArgs(jacsServiceData);

                    List<String> appArgs = new ArrayList<>();
                    appArgs.add("searchFromJSON");
                    appArgs.add("-m");
                    appArgs.addAll(args.masksFiles);

                    appArgs.add("-i");
                    appArgs.addAll(args.targetsFiles);

                    if (args.maskThreshold != null) {
                        appArgs.add("--maskThreshold");
                        appArgs.add(args.maskThreshold.toString());
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

                    if (args.negativeRadius != null) {
                        appArgs.add("--negativeRadius");
                        appArgs.add(args.negativeRadius.toString());
                    }

                    if (args.withGradientScores) {
                        appArgs.add("--with-grad-scores");
                    }

                    appArgs.add("--outputDir");
                    appArgs.add(args.cdMatchesDir);

                    return ImmutableList.<ServiceArg>builder()
                            .add(new ServiceArg("-appName", "colordepthsearch"))
                            .add(new ServiceArg("-appLocation", jarPath))
                            .add(new ServiceArg("-appEntryPoint", "org.janelia.colormipsearch.cmd.SparkMainEntry"))
                            .add(new ServiceArg("-appArgs", appArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")))
                            .build();
                },
                jacsServiceData -> {
                    SparkColorDepthSearchArgs args = getArgs(jacsServiceData);

                    return SparkAppResourceHelper.sparkAppResourceBuilder()
                            .sparkHome(defaultSparkHomeDir)
                            .sparkWorkers(args.numWorkers)
                            .minSparkWorkers(args.minWorkerNodes)
                            .sparkParallelism(args.parallelism)
                            .sparkWorkerCores(coresPerSparkWorker > 0 ?  coresPerSparkWorker : null)
                            .sparkAppIntervalCheckInMillis(searchIntervalCheckInMillis)
                            .sparkAppTimeoutInMillis(searchTimeoutInMillis)
                            .addAll(jacsServiceData.getResources())
                            .build();
                });
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SparkColorDepthFileSearch.class, new ColorDepthSearchArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public List<File> collectResult(JacsServiceData jacsServiceData) {
                return ColorDepthFileSearchProcessingUtils.collectResults(getArgs(jacsServiceData));
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<File>>> process(JacsServiceData jacsServiceData) {
        return sparkAppProcessor.process(jacsServiceData)
                .thenApply(r -> updateServiceResult(jacsServiceData, ColorDepthFileSearchProcessingUtils.collectResults(getArgs(jacsServiceData))));
    }

    private SparkColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SparkColorDepthSearchArgs());
    }

}

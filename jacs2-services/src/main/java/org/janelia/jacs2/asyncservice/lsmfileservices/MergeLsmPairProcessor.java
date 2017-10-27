package org.janelia.jacs2.asyncservice.lsmfileservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.MergeChannelsProcessor;
import org.janelia.jacs2.asyncservice.imageservices.DistortionCorrectionProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Named("mergeLsms")
public class MergeLsmPairProcessor extends AbstractBasicLifeCycleServiceProcessor<File, MergeLsmPairProcessor.MergeLsmIntermediateResult> {

    static class MergeLsmIntermediateResult {
        final Number mergeChannelsServiceId;

        public MergeLsmIntermediateResult(Number mergeChannelsServiceId) {
            this.mergeChannelsServiceId = mergeChannelsServiceId;
        }
    }

    static class MergeLsmPairArgs extends ServiceArgs {
        @Parameter(names = "-lsm1", description = "First LSM input file", required = true)
        String lsm1;
        @Parameter(names = "-lsm2", description = "Second LSM input file", required = true)
        String lsm2;
        @Parameter(names = "-outputFile", description = "Output file", required = true)
        String outputFile;
        @Parameter(names = "-microscope1", description = "Microscope name used for acquiring the first lsm", required = true)
        String microscope1;
        @Parameter(names = "-microscope2", description = "Microscope name used for acquiring the second lsm", required = false)
        String microscope2;
        @Parameter(names = "-multiscanVersion", description = "Multiscan blend version", required = false)
        String multiscanBlendVersion;
        @Parameter(names = "-distortionCorrection", description = "If specified apply distortion correction", required = false)
        boolean applyDistortionCorrection;
    }

    private final DistortionCorrectionProcessor distortionCorrectionProcessor;
    private final MergeChannelsProcessor mergeChannelsProcessor;

    @Inject
    MergeLsmPairProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          DistortionCorrectionProcessor distortionCorrectionProcessor,
                          MergeChannelsProcessor mergeChannelsProcessor,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.distortionCorrectionProcessor = distortionCorrectionProcessor;
        this.mergeChannelsProcessor = mergeChannelsProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(MergeLsmPairProcessor.class, new MergeLsmPairArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                MergeLsmIntermediateResult result = (MergeLsmIntermediateResult) depResults.getResult();
                JacsServiceData mergeChannelsServiceData = jacsServiceDataPersistence.findById(result.mergeChannelsServiceId);
                return mergeChannelsProcessor.getResultHandler().getServiceDataResult(mergeChannelsServiceData);
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        MergeLsmPairArgs args = getArgs(jacsServiceData);
        try {
            Path outputFile = getOutputFile(args);
            Files.createDirectories(outputFile.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<MergeLsmIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        MergeLsmPairArgs args = getArgs(jacsServiceData);

        List<JacsServiceData> distortionCorrectionServiceData;
        Path mergeInput1;
        Path mergeInput2;
        if (args.applyDistortionCorrection) {
            Path lsm1 = getLsm1(args);
            mergeInput1 = getCorrectionResult(lsm1);
            JacsServiceData correctLsm1ServiceData = applyCorrection(
                    lsm1,
                    mergeInput1,
                    args.microscope1,
                    "Apply correction to the first file",
                    jacsServiceData
            );
            Path lsm2 = getLsm1(args);
            mergeInput2 = getCorrectionResult(lsm2);
            JacsServiceData correctLsm2ServiceData = applyCorrection(
                    lsm1,
                    mergeInput1,
                    StringUtils.defaultIfBlank(args.microscope2, args.microscope1),
                    "Apply correction to the second file",
                    jacsServiceData
            );
            distortionCorrectionServiceData = ImmutableList.of(correctLsm1ServiceData, correctLsm2ServiceData);
        } else {
            mergeInput1 = getLsm1(args);
            mergeInput2 = getLsm2(args);
            distortionCorrectionServiceData = ImmutableList.of();
        }
        JacsServiceData mergeChannelsService = mergeChannels(mergeInput1, mergeInput2, getOutputFile(args), args.multiscanBlendVersion,
                "Merge channels",
                jacsServiceData,
                distortionCorrectionServiceData.toArray(new JacsServiceData[distortionCorrectionServiceData.size()]));
        return new JacsServiceResult<>(jacsServiceData, new MergeLsmIntermediateResult(mergeChannelsService.getId()));
    }

    private JacsServiceData applyCorrection(Path input, Path output, String microscope, String description, JacsServiceData jacsServiceData, JacsServiceData... deps) {
        JacsServiceData correctionServiceData = distortionCorrectionProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(deps)
                        .description(description)
                        .build(),
                new ServiceArg("-inputFile", input.toString()),
                new ServiceArg("-outputFile", output.toString()),
                new ServiceArg("-microscope", microscope)
        );
        return submitDependencyIfNotFound(correctionServiceData);
    }

    private JacsServiceData mergeChannels(Path input1, Path input2, Path outputFile, String multiscanBlendVersion, String description, JacsServiceData jacsServiceData, JacsServiceData... deps) {
        JacsServiceData mergeServiceData = mergeChannelsProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(deps)
                        .description(description)
                        .build(),
                new ServiceArg("-chInput1", input1.toString()),
                new ServiceArg("-chInput2", input2.toString()),
                new ServiceArg("-multiscanVersion", multiscanBlendVersion),
                new ServiceArg("-outputFile", outputFile.toString())
        );
        return submitDependencyIfNotFound(mergeServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<MergeLsmIntermediateResult>> processing(JacsServiceResult<MergeLsmIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults);
    }

    private MergeLsmPairArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new MergeLsmPairArgs());
    }

    private Path getLsm1(MergeLsmPairArgs args) {
        return Paths.get(args.lsm1);
    }

    private Path getLsm2(MergeLsmPairArgs args) {
        return Paths.get(args.lsm2);
    }

    private Path getOutputFile(MergeLsmPairArgs args) {
        return Paths.get(args.outputFile);
    }

    private Path getCorrectionResult(Path lsmFile) {
        return FileUtils.replaceFileExt(lsmFile, ".v3draw");
    }
}

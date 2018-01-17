package org.janelia.jacs2.asyncservice.alignservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.fileservices.LinkDataProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dConverterProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dPluginProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * AlignmentVerificationMovieProcessor generates the alignment verification movie.
 */
@Named("alignmentVerificationMovie")
public class AlignmentVerificationMovieProcessor extends AbstractBasicLifeCycleServiceProcessor<File, Void> {

    static class AlignmentVerificationMoviewArgs extends ServiceArgs {
        @Parameter(names = {"-s", "-subject"}, description = "Subject file", required = true)
        String subjectFile;
        @Parameter(names = {"-i", "-target"}, description = "Target file", required = true)
        String targetFile;
        @Parameter(names = {"-r", "-reference"}, description = "Reference channel")
        Integer referenceChannel;
        @Parameter(names = {"-o", "-output"}, description = "Output file")
        String outputFile;
    }

    private final AlignmentServicesInvocationHelper invocationHelper;

    @Inject
    AlignmentVerificationMovieProcessor(ServiceComputationFactory computationFactory,
                                        JacsServiceDataPersistence jacsServiceDataPersistence,
                                        @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                        LinkDataProcessor linkDataProcessor,
                                        Vaa3dConverterProcessor vaa3dConverterProcessor,
                                        Vaa3dPluginProcessor vaa3dPluginProcessor,
                                        Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        invocationHelper = new AlignmentServicesInvocationHelper(jacsServiceDataPersistence,
                linkDataProcessor,
                vaa3dConverterProcessor,
                vaa3dPluginProcessor,
                null, // niftiConverterProcessor
                null, // warpToolProcessor
                logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(AlignmentVerificationMovieProcessor.class, new AlignmentVerificationMoviewArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return getOutputFile(getArgs(depResults.getJacsServiceData())).toFile().exists();
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                return getOutputFile(getArgs(depResults.getJacsServiceData())).toFile();
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            Path outputFile = getOutputFile(getArgs(jacsServiceData));
            Files.createDirectories(outputFile.getParent());
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<Void> submitServiceDependencies(JacsServiceData jacsServiceData) {
        AlignmentVerificationMoviewArgs args = getArgs(jacsServiceData);

        JacsServiceFolder serviceFolder = getWorkingDirectory(jacsServiceData);
        Path workingSubjectFile = serviceFolder.getServiceFolder(FileUtils.getFileName(args.subjectFile)); // => SUB

        JacsServiceData createWorkingSubjectFile = invocationHelper.linkData(getSubjectFile(args), workingSubjectFile,
                "Create link for working subject file",
                jacsServiceData);

        // $Vaa3D -x ireg -f splitColorChannels -i $SUB
        JacsServiceData splitChannelsServiceData =
                invocationHelper.applyPlugin(
                        ImmutableList.of(workingSubjectFile),
                        ImmutableList.of(),
                        "ireg", "splitColorChannels", null,
                        "Split channels",
                        jacsServiceData,
                        createWorkingSubjectFile);

        Path targetFile = getTargetFile(args);
        Path refChannelFile = invocationHelper.getChannelFilePath(
                serviceFolder.getServiceFolder(),
                args.referenceChannel - 1,
                args.subjectFile,
                ".v3draw");
        Path temporaryMergedFile = FileUtils.getFilePath(
                serviceFolder.getServiceFolder(),
                null,
                args.outputFile,
                "temporaryMergedOut",
                ".v3draw");
        // $Vaa3D -x ireg -f mergeColorChannels -i $TAR $SUBR $TAR -o $WORKDIR/out.v3draw
        JacsServiceData mergeChannelsServiceData =
                invocationHelper.applyPlugin(
                        ImmutableList.of(targetFile, refChannelFile, targetFile),
                        ImmutableList.of(temporaryMergedFile),
                        "ireg",
                        "mergeColorChannels",
                        null,
                        "Split channels",
                        jacsServiceData,
                        splitChannelsServiceData);

        Path outputFile = getOutputFile(args);
        // $Vaa3D -cmd image-loader -convert $WORKDIR/out.v3draw $OUTPUT_FILE
        invocationHelper.convertFile(
                        temporaryMergedFile,
                        outputFile,
                        "Generate movie",
                        jacsServiceData,
                        mergeChannelsServiceData);

        return new JacsServiceResult<>(jacsServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depResults) {
        return computationFactory.newCompletedComputation(depResults);
    }

    private AlignmentVerificationMoviewArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new AlignmentVerificationMoviewArgs());
    }

    private Path getSubjectFile(AlignmentVerificationMoviewArgs args) {
        return Paths.get(args.subjectFile);
    }

    private Path getOutputFile(AlignmentVerificationMoviewArgs args) {
        return Paths.get(args.outputFile);
    }

    private Path getTargetFile(AlignmentVerificationMoviewArgs args) {
        return Paths.get(args.targetFile);
    }
}

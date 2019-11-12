package org.janelia.jacs2.asyncservice.imageservices;

import java.io.File;
import java.util.List;
import java.util.Map;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;

import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Create a square montage from given PNGs assuming a tile pattern with the given number of tiles per side. If the number of tiles per side is not specified
 * it tries to form a square from the list of provided images.
 */
@Named("montageImages")
public class MontageImagesProcessor extends AbstractExeBasedServiceProcessor<File> {

    static class MontageImagesArgs extends ServiceArgs {
        @Parameter(names = "-inputFiles", description = "List of input files to be montaged together. As a note this file will not try to group the provided inputs.", required = true)
        List<String> inputFiles;
        @Parameter(names = "-tilesPerSide", description = "Number of tiles per side", required = false)
        int tilesPerSide = 0;
        @Parameter(names = "-output", description = "Name of the output montage")
        String output;
    }

    private final String montageToolLocation;
    private final String montageToolName;
    private final String libraryPath;

    @Inject
    MontageImagesProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           @PropertyValue(name = "ImageMagick.Bin.Path") String montageToolLocation,
                           @PropertyValue(name = "ImageMagick.Montage.Name") String montageToolName,
                           @PropertyValue(name = "ImageMagick.Lib.Path") String libraryPath,
                           JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                           @ApplicationProperties ApplicationConfig applicationConfig,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.montageToolLocation = montageToolLocation;
        this.montageToolName = montageToolName;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(MontageImagesProcessor.class, new MontageImagesArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return getMontageOutput(getArgs(jacsServiceData)).exists();
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                return getMontageOutput(getArgs(jacsServiceData));
            }
        };
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        MontageImagesArgs args = getArgs(jacsServiceData);
        int tilesPerSide = args.tilesPerSide > 0 ? args.tilesPerSide : (int) Math.ceil(Math.sqrt(args.inputFiles.size()));
        logger.info("Montage {}", args.inputFiles);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        externalScriptCode.getCodeWriter()
                .addWithArgs(getExecutable())
                .addArg("-background")
                .addArg("'#000000'")
                .addArg("-geometry")
                .addArg("'300x300>'")
                .addArg("-tile")
                .addArg(String.format("%dx%d", tilesPerSide, tilesPerSide))
                .addArgs(args.inputFiles)
                .endArgs(args.output);
        return externalScriptCode;
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, getFullExecutableName(libraryPath)));
    }

    private MontageImagesArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new MontageImagesArgs());
    }

    private File getMontageOutput(MontageImagesArgs args) {
        return new File(args.output);
    }

    private String getExecutable() {
        return getFullExecutableName(montageToolLocation, montageToolName);
    }

}

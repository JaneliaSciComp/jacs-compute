package org.janelia.jacs2.imageservices;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.service.JacsServiceData;
import org.janelia.jacs2.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.service.impl.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.service.impl.ComputationException;
import org.janelia.jacs2.service.impl.ExternalProcessRunner;
import org.janelia.jacs2.service.impl.JacsServiceDispatcher;
import org.janelia.jacs2.service.impl.ServiceComputationFactory;
import org.janelia.jacs2.service.impl.ServiceDataUtils;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
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

public class MontageImagesProcessor extends AbstractExeBasedServiceProcessor<File> {

    private final String montageToolLocation;
    private final String montageToolName;
    private final String libraryPath;

    @Inject
    MontageImagesProcessor(JacsServiceDispatcher jacsServiceDispatcher,
                           ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           @PropertyValue(name = "Executables.ModuleBase") String executablesBaseDir,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "ImageMagick.Bin.Path") String montageToolLocation,
                           @PropertyValue(name = "ImageMagick.Montage.Name") String montageToolName,
                           @PropertyValue(name = "ImageMagick.Lib.Path") String libraryPath,
                           Logger logger) {
        super(jacsServiceDispatcher, computationFactory, jacsServiceDataPersistence, defaultWorkingDir, executablesBaseDir, serviceRunners, logger);
        this.montageToolLocation = montageToolLocation;
        this.montageToolName = montageToolName;
        this.libraryPath = libraryPath;
    }

    @Override
    public File getResult(JacsServiceData jacsServiceData) {
        return ServiceDataUtils.stringToFile(jacsServiceData.getStringifiedResult());
    }

    @Override
    public void setResult(File result, JacsServiceData jacsServiceData) {
        jacsServiceData.setStringifiedResult(ServiceDataUtils.fileToString(result));
    }

    @Override
    protected boolean isResultAvailable(Object preProcessingResult, JacsServiceData jacsServiceData) {
        MontageImagesServiceDescriptor.MontageImagesArgs args = getArgs(jacsServiceData);
        File targetImage = new File(args.target);
        return targetImage.exists();
    }

    @Override
    protected File retrieveResult(Object preProcessingResult, JacsServiceData jacsServiceData) {
        MontageImagesServiceDescriptor.MontageImagesArgs args = getArgs(jacsServiceData);
        File targetImage = new File(args.target);
        return targetImage;
    }

    @Override
    protected List<String> prepareCmdArgs(JacsServiceData jacsServiceData) {
        MontageImagesServiceDescriptor.MontageImagesArgs args = getArgs(jacsServiceData);
        jacsServiceData.setServiceCmd(getExecutable());
        Path inputPath = Paths.get(args.inputFolder);
        List<String> inputFiles = new ArrayList<>();
        try {
            PathMatcher inputFileMatcher =
                    FileSystems.getDefault().getPathMatcher(args.imageFilePattern);
            Files.find(inputPath, 1, (p, a) -> inputFileMatcher.matches(p)).forEach(p -> {
                inputFiles.add(p.toString());
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("No image file found in " + args.inputFolder + " that matches the given pattern: " + args.imageFilePattern);
        }
        logger.info("Montage {}", inputFiles);
        return new ImmutableList.Builder<String>()
                .add("-background")
                .add("'#000000'")
                .add("-geometry")
                .add("'300x300>'")
                .add("-tile")
                .add(String.format("%dx%d", args.size, args.size))
                .addAll(inputFiles)
                .add(args.target)
                .build();
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, getFullExecutableName(libraryPath)));
    }

    private MontageImagesServiceDescriptor.MontageImagesArgs getArgs(JacsServiceData jacsServiceData) {
        MontageImagesServiceDescriptor.MontageImagesArgs args = new MontageImagesServiceDescriptor.MontageImagesArgs();
        new JCommander(args).parse(jacsServiceData.getArgsArray());
        return args;
    }

    private String getExecutable() {
        return getFullExecutableName(montageToolLocation, montageToolName);
    }

}

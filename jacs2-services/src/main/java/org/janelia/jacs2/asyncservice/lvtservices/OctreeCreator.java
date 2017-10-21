package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for creating an octree representation of an image stack, suitable for loading into the Large Volume Viewer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("octreeCreator")
public class OctreeCreator extends AbstractExeBasedServiceProcessor<List<File>> {

    static class OctreeCreatorArgs extends ServiceArgs {
        @Parameter(names = "-input", description = "Input directory containing TIFF files", required = true)
        String input;
        @Parameter(names = "-output", description = "Output directory for octree", required = true)
        String output;
        @Parameter(names = "-levels", description = "Number of octree levels", required = false)
        Integer levels = 3;
        @Parameter(names = "-voxelSize", description = "Voxel size (in 'x,y,z' format)", required = true)
        String voxelSize;
    }

    private final String executable;

    @Inject
    OctreeCreator(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      @Any Instance<ExternalProcessRunner> serviceRunners,
                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                      @PropertyValue(name = "Octree.Bin.Path") String executable,
                      ThrottledProcessesQueue throttledProcessesQueue,
                      @ApplicationProperties ApplicationConfig applicationConfig,
                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
        this.executable = executable;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(OctreeCreator.class, new OctreeCreatorArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {

            private boolean verifyOctree(File dir) {

                boolean checkChanFile = false;
                for(File file : dir.listFiles((FileFilter)null)) {
                    if (file.isDirectory()) {
                        try {
                            int index = Integer.parseInt(file.getName());
                            if (!verifyOctree(file)) return false;
                        }
                        catch (NumberFormatException e) {
                            // Ignore dirs which are not numbers
                        }
                    }
                    else {
                        // TODO: should check for one file for each channel in the input
                        if ("default.0.tif".equals(file.getName())) {
                            checkChanFile = true;
                        }
                    }
                }
                if (!checkChanFile) return false;
                return true;
            }

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                File outputDir = new File(getArgs(depResults.getJacsServiceData()).output);
                if (!outputDir.exists()) return false;
                if (!verifyOctree(outputDir)) return false;
                return true;
            }

            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                OctreeCreatorArgs args = getArgs(depResults.getJacsServiceData());
                Path outputDir = getOutputDir(args);
                return FileUtils.lookupFiles(outputDir, 1, "glob:transform.txt")
                        .map(Path::toFile)
                        .collect(Collectors.toList());
            }
        };
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        OctreeCreatorArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(OctreeCreatorArgs args, ScriptWriter scriptWriter) {
        scriptWriter.read("INPUT");
        scriptWriter.read("OUTPUT");
        scriptWriter.read("LEVELS");
        scriptWriter.read("VOXEL_SIZE");
        scriptWriter.addWithArgs(getFullExecutableName(executable));
        scriptWriter.addArg("$INPUT");
        scriptWriter.addArg("$OUTPUT");
        scriptWriter.addArg("$LEVELS");
        scriptWriter.addArg("$VOXEL_SIZE");
        scriptWriter.endArgs();
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        OctreeCreatorArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter scriptWriter = externalScriptCode.getCodeWriter();
        scriptWriter.add(args.input);
        scriptWriter.add(args.output);
        scriptWriter.add(args.levels.toString());
        scriptWriter.add(args.voxelSize);
        scriptWriter.close();
        return Arrays.asList(externalScriptCode);
    }

    @Override
    protected void prepareResources(JacsServiceData jacsServiceData) {
        ProcessorHelper.setCPUType(jacsServiceData.getResources(),"broadwell");
        // This should be based on input file size, but we don't currently have enough examples to generalize this.
        // Examples:
        // 12G input -> 66G
        ProcessorHelper.setRequiredMemoryInGB(jacsServiceData.getResources(), 128);
        ProcessorHelper.setSoftJobDurationLimitInSeconds(jacsServiceData.getResources(), 2*60*60); // 2 hours
        ProcessorHelper.setHardJobDurationLimitInSeconds(jacsServiceData.getResources(), 4*60*60); // 4 hours
    }

    private OctreeCreatorArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new OctreeCreatorArgs());
    }

    private Path getOutputDir(OctreeCreatorArgs args) {
        return Paths.get(args.output).toAbsolutePath();
    }
}

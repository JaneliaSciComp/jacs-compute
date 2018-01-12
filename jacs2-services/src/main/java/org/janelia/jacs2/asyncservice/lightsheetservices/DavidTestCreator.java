package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
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
@Named("davidTest")
public class DavidTestCreator extends AbstractExeBasedServiceProcessor<List<File>> {

    static class DavidTestCreatorArgs extends ServiceArgs {
        @Parameter(names = "-jsonDirectory", description = "Directory with JSON files", required = true)
        String jsonDirectory;
    }

    private final String executable;

    @Inject
    DavidTestCreator(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      @Any Instance<ExternalProcessRunner> serviceRunners,
                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                      @PropertyValue(name = "DavidTest.Bin.Path") String executable,
                     JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                      @ApplicationProperties ApplicationConfig applicationConfig,
                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.executable = executable;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DavidTestCreator.class, new DavidTestCreatorArgs());
    }

/*    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {

            private boolean verifyDavidTest(File dir) {

                boolean checkChanFile = false;
                for(File file : dir.listFiles((FileFilter)null)) {
                    if (file.isDirectory()) {
                        try {
                            int index = Integer.parseInt(file.getName());
                            if (!verifyDavidTest(file)) return false;
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
                if (!verifyDavidTest(outputDir)) return false;
                return true;
            }

            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                DavidTestCreatorArgs args = getArgs(depResults.getJacsServiceData());
                Path outputDir = getOutputDir(args);
                return FileUtils.lookupFiles(outputDir, 1, "glob:transform.txt")
                        .map(Path::toFile)
                        .collect(Collectors.toList());
            }
        };
    }*/

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        DavidTestCreatorArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(DavidTestCreatorArgs args, ScriptWriter scriptWriter) {
        scriptWriter.read("JSONDIRECTORY");
        scriptWriter.addWithArgs(getFullExecutableName(executable));
        scriptWriter.addArg("$JSONDIRECTORY");
        scriptWriter.endArgs();
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        DavidTestCreatorArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter scriptWriter = externalScriptCode.getCodeWriter();
        scriptWriter.add(args.jsonDirectory);
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
        ProcessorHelper.setRequiredSlots(jacsServiceData.getResources(), 64);
        ProcessorHelper.setSoftJobDurationLimitInSeconds(jacsServiceData.getResources(), 2*60*60); // 2 hours
        ProcessorHelper.setHardJobDurationLimitInSeconds(jacsServiceData.getResources(), 4*60*60); // 4 hours
    }

    private DavidTestCreatorArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DavidTestCreatorArgs());
    }

   /* private Path getOutputDir(DavidTestCreatorArgs args) {
        return Paths.get(args.jsonDirectory).toAbsolutePath();
    }*/
}

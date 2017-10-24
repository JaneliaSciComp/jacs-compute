package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for creating a KTX representation of an image stack which is represented as an octree,
 * suitable for loading into Horta.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("ktxCreator")
public class KTXCreator extends AbstractExeBasedServiceProcessor<List<File>> {

    static class KTXCreatorArgs extends ServiceArgs {
        @Parameter(names = "-input", description = "Input directory containing octree", required = true)
        String input;
        @Parameter(names = "-output", description = "Output directory for octree", required = true)
        String output;
        @Parameter(names = "-levels", description = "Number of tree levels", required = false)
        Integer levels = 3;
    }

    private final String ktxSrcDir;
    private final String executable;
    private final String anacondaDir;

    @Inject
    KTXCreator(ServiceComputationFactory computationFactory,
               JacsServiceDataPersistence jacsServiceDataPersistence,
               @Any Instance<ExternalProcessRunner> serviceRunners,
               @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
               @PropertyValue(name = "KTX.Src.Path") String ktxSrcDir,
               @PropertyValue(name = "KTX.Script.Path") String executable,
               @PropertyValue(name = "Anaconda.Bin.Path") String anacondaDir,
               ThrottledProcessesQueue throttledProcessesQueue,
               @ApplicationProperties ApplicationConfig applicationConfig,
               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
        this.ktxSrcDir = getFullExecutableName(ktxSrcDir);
        this.executable = getFullExecutableName(executable);
        this.anacondaDir = getFullExecutableName(anacondaDir);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(KTXCreator.class, new KTXCreatorArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {

            private boolean verifyOctree(File dir) {

                boolean checkChanFile = false;
                for(File file : dir.listFiles((FileFilter)null)) {
                    if (file.isDirectory()) {
                        try {
                            Integer.parseInt(file.getName());
                            if (!verifyOctree(file)) return false;
                        }
                        catch (NumberFormatException e) {
                            // Ignore dirs which are not numbers
                        }
                    }
                    else {
                        if (file.getName().startsWith("block") && file.getName().endsWith(".ktx")) {
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
                KTXCreatorArgs args = getArgs(depResults.getJacsServiceData());
                Path outputDir = getOutputDir(args);
                return FileUtils.lookupFiles(outputDir, 100, "glob:*.ktx")
                        .map(Path::toFile)
                        .collect(Collectors.toList());
            }
        };
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        KTXCreatorArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(KTXCreatorArgs args, ScriptWriter scriptWriter) {
        scriptWriter.read("INPUT");
        scriptWriter.read("OUTPUT");
        scriptWriter.read("SUBTREE_PATH");
        scriptWriter.read("SUBTREE_DEPTH");

        scriptWriter.exportVar("PATH", anacondaDir+":$PATH");
        scriptWriter.exportVar("KTXSRC", ktxSrcDir);
        scriptWriter.exportVar("PYTHONPATH", ktxSrcDir);

        scriptWriter.add("source activate pyktx");

        scriptWriter.addWithArgs("python"); // use the default python in the pyktx conda environment
        scriptWriter.addArg(getFullExecutableName(executable));
        scriptWriter.addArg("$INPUT");
        scriptWriter.addArg("$OUTPUT");
        scriptWriter.addArg(StringUtils.wrap("$SUBTREE_PATH", '"'));
        scriptWriter.addArg("$SUBTREE_DEPTH");
        scriptWriter.endArgs();
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        KTXCreatorArgs args = getArgs(jacsServiceData);
        List<ExternalCodeBlock> blocks = new ArrayList<>();
        recurseOctree(blocks, args, args.input, 1);
        return blocks;
    }

    /**
     * Walks the input octree and generates a number of scripts to process it. This was simply
     * transliterated from the Python version (ktx/src/tools/cluster/create_cluster_scripts.py),
     * which was intended for very large MouseLight data. It may not be optimal for smaller data.
     */
    private void recurseOctree(List<ExternalCodeBlock> blocks, KTXCreatorArgs args, String folder, int level) {

        File file = new File(folder);
        if (!file.exists()) return;

        logger.trace("recurse_octree(folder={},level={})", folder, level);

        if (level==1 || level % args.levels == 2) {

            Integer levels;
            List<String> subtree0;
            if (level == 1) {
                subtree0 = Collections.emptyList();
                levels = 1;
            }
            else {
                List<String> path = Arrays.asList(folder.split("\\/"));
                int num = level-1;
                logger.trace("  path={}, pathSize={}, num={}", path, path.size(), num);
                subtree0 = path.subList(path.size() - num, path.size());
                levels = args.levels;
            }

            String subtree = String.join("/", subtree0);
            logger.info("  Creating script for subtree: {} (levels={})", subtree, levels);

            ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
            ScriptWriter scriptWriter = externalScriptCode.getCodeWriter();
            scriptWriter.add(args.input);
            scriptWriter.add(args.output);
            scriptWriter.add(subtree);
            scriptWriter.add(levels.toString());
            scriptWriter.close();

            blocks.add(externalScriptCode);
        }

        for(int i=0; i<8; i++) {
            String subfolder = folder + "/" + (i+1);
            recurseOctree(blocks, args, subfolder, level + 1);
        }
    }

    @Override
    protected void prepareResources(JacsServiceData jacsServiceData) {
        // This doesn't need much memory, because it only processes a single tile at a time.
        ProcessorHelper.setRequiredSlots(jacsServiceData.getResources(),1);
        ProcessorHelper.setSoftJobDurationLimitInSeconds(jacsServiceData.getResources(), 5*60); // 5 minutes
        ProcessorHelper.setHardJobDurationLimitInSeconds(jacsServiceData.getResources(), 60*60); // 1 hour
    }

    private KTXCreatorArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new KTXCreatorArgs());
    }

    private Path getOutputDir(KTXCreatorArgs args) {
        return Paths.get(args.output).toAbsolutePath();
    }
}

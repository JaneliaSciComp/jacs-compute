package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.containerizedservices.PullAndRunSingularityContainerProcessor;
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
public class KTXCreator extends AbstractLVTProcessor<KTXCreator.KTXCreatorArgs, List<File>> {

    static class KTXCreatorArgs extends LVTArgs {
    }

    @Inject
    KTXCreator(ServiceComputationFactory computationFactory,
               JacsServiceDataPersistence jacsServiceDataPersistence,
               @Any Instance<ExternalProcessRunner> serviceRunners,
               @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
               PullAndRunSingularityContainerProcessor pullAndRunContainerProcessor,
               @PropertyValue(name = "service.ktxCreator.containerImage") String defaultContainerImage,
               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, pullAndRunContainerProcessor, defaultContainerImage, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(KTXCreator.class, createToolArgs());
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
                File outputDir = new File(getArgs(depResults.getJacsServiceData()).outputDir);
                if (!outputDir.exists()) return false;
                if (!verifyOctree(outputDir)) return false;
                return true;
            }

            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                KTXCreatorArgs args = getArgs(depResults.getJacsServiceData());
                Path outputDir = getOutputDir(args);
                return FileUtils.lookupFiles(outputDir, 100, "glob:**/*.ktx")
                        .map(Path::toFile)
                        .collect(Collectors.toList());
            }
        };
    }

    @Override
    KTXCreatorArgs createToolArgs() {
        return new KTXCreatorArgs();
    }

    @Override
    StringBuilder serializeToolArgs(KTXCreatorArgs args) {
        return new StringBuilder(); // !!!!! FIXME
    }
}

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
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for creating an octree representation of an image stack, suitable for loading into the Large Volume Viewer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("octreeCreator")
public class OctreeCreator extends AbstractLVTProcessor<OctreeCreator.OctreeCreatorArgs, File> {

    private static final String TRANSFORM_FILENAME = "transform.txt";
    private static final String OUTPUT_FILENAME_PATTERN = "default.%d.tif";

    static class OctreeCreatorArgs extends LVTArgs {
        @Parameter(names = "-inputFilename", description = "Input file name relative to inputDir", required = true)
        String inputFilename;
        @Parameter(names = "-channel", description = "Channel number (Default = 0) used mainly for formatting the output")
        Integer channel = 0;
        @Parameter(names = "-voxelSize", description = "Voxel size (in 'x,y,z' format)", required = true)
        String voxelSize;
    }

    @Inject
    OctreeCreator(ServiceComputationFactory computationFactory,
                  JacsServiceDataPersistence jacsServiceDataPersistence,
                  @Any Instance<ExternalProcessRunner> serviceRunners,
                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                  PullAndRunSingularityContainerProcessor pullAndRunContainerProcessor,
                  @PropertyValue(name = "service.octreeCreator.containerImage") String defaultContainerImage,
                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, pullAndRunContainerProcessor, defaultContainerImage, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(OctreeCreator.class, new OctreeCreatorArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                OctreeCreatorArgs args = getArgs(depResults.getJacsServiceData());
                File outputDir = new File(args.outputDir);
                if (!outputDir.exists()) return false;
                File transformFile = new File(outputDir, TRANSFORM_FILENAME);
                File outputFile = new File(outputDir, String.format(OUTPUT_FILENAME_PATTERN, args.channel));
                return transformFile.exists() && outputFile.exists();
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                return new File(getArgs(depResults.getJacsServiceData()).outputDir, TRANSFORM_FILENAME);
            }
        };
    }

    @Override
    OctreeCreatorArgs createToolArgs() {
        return new OctreeCreatorArgs();
    }

    @Override
    StringBuilder serializeToolArgs(OctreeCreatorArgs args) {
        return new StringBuilder()
                .append(getInputFileName(args)).append(',')
                .append(args.outputDir).append(',')
                .append(args.levels).append(',')
                .append(args.channel).append(',')
                .append('\'').append(args.voxelSize).append('\'')
                ;
    }

    private String getInputFileName(OctreeCreatorArgs args) {
        return Paths.get(args.inputDir, args.inputFilename).toString();
    }

}

package org.janelia.jacs2.asyncservice.lvtservices;

import java.io.File;
import java.nio.file.Paths;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;

import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.containerizedservices.PullAndRunSingularityContainerProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Service for creating an octree representation of an image stack, suitable for loading into the Large Volume Viewer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("octreeCreator")
public class OctreeCreator extends AbstractLVTProcessor<OctreeCreator.OctreeCreatorArgs, OctreeResult> {

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
    public ServiceResultHandler<OctreeResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<OctreeResult>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                OctreeCreatorArgs args = getArgs(jacsServiceData);
                File outputDir = new File(args.outputDir);
                if (!outputDir.exists()) return false;
                File transformFile = new File(outputDir, TRANSFORM_FILENAME);
                File outputFile = new File(outputDir, String.format(OUTPUT_FILENAME_PATTERN, args.channel));
                return transformFile.exists() && outputFile.exists();
            }

            public OctreeResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<OctreeResult>() {});
            }
        };
    }

    @Override
    OctreeCreatorArgs createToolArgs() {
        return new OctreeCreatorArgs();
    }

    @Override
    String getAppArgs(OctreeCreatorArgs args) {
        return new StringBuilder()
                .append(getInputFileName(args)).append(',')
                .append(args.outputDir).append(',')
                .append(args.levels).append(',')
                .append(args.channel).append(',')
                .append('\'').append(args.voxelSize).append('\'')
                .toString()
                ;
    }

    @Override
    OctreeResult collectResult(JacsServiceData jacsServiceData) {
        OctreeCreatorArgs args = getArgs(jacsServiceData);
        OctreeResult octreeResult = new OctreeResult();
        octreeResult.setBasePath(args.outputDir);
        octreeResult.setLevels(args.levels);
        return octreeResult;
    }

    private String getInputFileName(OctreeCreatorArgs args) {
        return Paths.get(args.inputDir, args.inputFilename).toString();
    }

}

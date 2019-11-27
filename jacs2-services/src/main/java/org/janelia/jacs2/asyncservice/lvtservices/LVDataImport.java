package org.janelia.jacs2.asyncservice.lvtservices;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * The LV (Large Volume) Data Import first converts the input data into an Octree using OctreeCreator
 * and then runs KTXCreator to create the KTX tiles for Horta.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("lvDataImport")
public class LVDataImport extends AbstractServiceProcessor<LVResult> {

    static class LVDataImportArgs extends LVArgs {
        @Parameter(names = "-containerProcessor", description = "Container processor: docker or singularity")
        String containerProcessor;
        @Parameter(names = "-tiffOctreeOutputDir", description = "Output directory for octree")
        String tiffOctreeOutputDir;
        @Parameter(names = "-ktxOctreeOutputDir", description = "Output directory for octree")
        String ktxOctreeOutputDir;
        @Parameter(names = "-voxelSize", description = "Voxel size (in 'x,y,z' format)")
        String voxelSize = "1,1,1";
        @Parameter(names = "-channels", description = "Channel number (Default = 0) used mainly for formatting the output")
        List<Integer> channels;
        @Parameter(names = "-inputFilenamePattern",
                description = "Input file name pattern. The pattern must contain a string {channel} that will be replaced with actual channel number",
                required = true)
        String intputFileNamePattern;
        @Parameter(names = "-subtreeLengthForSubjobSplitting", description = "The subtree length considered for job splitting")
        Integer subtreeLengthForSubjobSplitting;
        @Parameter(names = "-tiffOctreeContainerImage", description = "Name of the container image for generating the TIFF octree")
        String tiffOctreeContainerImage;
        @Parameter(names = "-ktxOctreeContainerImage", description = "Name of the container image for generating the KTX octree")
        String ktxOctreeContainerImage;
    }

    private final WrappedServiceProcessor<OctreeCreator, OctreeResult> octreeCreator;
    private final WrappedServiceProcessor<KTXCreator,OctreeResult> ktxCreator;

    @Inject
    LVDataImport(ServiceComputationFactory computationFactory,
                 JacsServiceDataPersistence jacsServiceDataPersistence,
                 @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                 OctreeCreator octreeCreator,
                 KTXCreator ktxCreator,
                 Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.octreeCreator = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, octreeCreator);
        this.ktxCreator = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, ktxCreator);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LVDataImport.class, new LVDataImportArgs());
    }

    @Override
    public ServiceResultHandler<LVResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<LVResult>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public LVResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<LVResult>() {});
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public ServiceComputation<JacsServiceResult<LVResult>> process(JacsServiceData jacsServiceData) {
        LVDataImportArgs args = getArgs(jacsServiceData);

        List<Integer> inputChannels;
        if (CollectionUtils.isEmpty(args.channels)) {
            inputChannels = ImmutableList.of(0);
        } else {
            inputChannels = args.channels;
        }
        final String inputTiffDir = args.inputDir;
        final String octreeDir = StringUtils.defaultIfBlank(args.tiffOctreeOutputDir, args.outputDir);
        final String ktxDir = StringUtils.defaultIfBlank(args.ktxOctreeOutputDir, StringUtils.appendIfMissing(octreeDir, "/") + "ktx");
        final int levels = args.levels;
        final String voxelSize = args.voxelSize;

        List<ServiceComputation<?>> octreeComputations = inputChannels.stream()
                .map(channelNo -> String.valueOf(channelNo))
                .map(channel -> octreeCreator.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                .description("Create octree")
                                .build(),
                        new ServiceArg("-containerProcessor", args.containerProcessor),
                        new ServiceArg("-inputDir", inputTiffDir),
                        new ServiceArg("-outputDir", octreeDir),
                        new ServiceArg("-levels", levels),
                        new ServiceArg("-voxelSize", voxelSize),
                        new ServiceArg("-channel", channel),
                        new ServiceArg("-inputFilename", args.intputFileNamePattern.replace("{channel}", channel)),
                        new ServiceArg("-toolContainerImage", args.tiffOctreeContainerImage)))
                .collect(Collectors.toList());
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenComposeAll(octreeComputations, (sd, results) -> {
                    List<JacsServiceResult<OctreeResult>> octreeResults = (List<JacsServiceResult<OctreeResult>>) results;
                    return ktxCreator.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Create ktx tiles")
                                    .waitFor(octreeResults.stream().map(r -> r.getJacsServiceData()).collect(Collectors.toList()))
                                    .build(),
                            new ServiceArg("-containerProcessor", args.containerProcessor),
                            new ServiceArg("-inputDir", octreeDir),
                            new ServiceArg("-outputDir", ktxDir),
                            new ServiceArg("-levels", levels),
                            new ServiceArg("-subtreeLengthForSubjobSplitting", args.subtreeLengthForSubjobSplitting != null ? args.subtreeLengthForSubjobSplitting.toString() : null),
                            new ServiceArg("-toolContainerImage", args.ktxOctreeContainerImage));
                })
                .thenApply(ktxResult -> {
                    LVResult lvResult = new LVResult();
                    lvResult.setBaseTiffPath(octreeDir);
                    lvResult.setBaseKtxPath(ktxDir);
                    lvResult.setLevels(levels);
                    return updateServiceResult(jacsServiceData, lvResult);
                })
                ;
    }

    private LVDataImportArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LVDataImportArgs());
    }
}

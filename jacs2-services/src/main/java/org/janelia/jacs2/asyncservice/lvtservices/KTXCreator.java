package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
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
public class KTXCreator extends AbstractLVTProcessor<KTXCreator.KTXCreatorArgs, OctreeResult> {

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
    public ServiceResultHandler<OctreeResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<OctreeResult>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            public OctreeResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<OctreeResult>() {});
            }
        };
    }

    @Override
    KTXCreatorArgs createToolArgs() {
        return new KTXCreatorArgs();
    }

    @Override
    StringBuilder serializeToolArgs(KTXCreatorArgs args) {
        // !!!!!!!! FIXME
        return new StringBuilder()
                .append(args.outputDir).append(',')
                .append(args.levels).append(',')
                ;
    }

    @Override
    OctreeResult collectResult(JacsServiceData jacsServiceData) {
        KTXCreatorArgs args = getArgs(jacsServiceData);
        OctreeResult octreeResult = new OctreeResult();
        octreeResult.setBasePath(args.outputDir);
        octreeResult.setLevels(args.levels);
        return octreeResult;
    }

}

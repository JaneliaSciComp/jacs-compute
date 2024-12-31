package org.janelia.jacs2.asyncservice.imageservices;

import java.nio.file.Path;
import java.util.StringJoiner;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Generates MIPs and movies for a single input stacks. Supports customizable colors
 * and other features controlled by chanelSpec and colorSpec parameters.
 *
 * 1) If a second image is specified it is normalized to the first one.
 * 2) It supports legends
 */
@Named("simpleMIPsAndMovies")
public class SimpleMIPsAndMoviesProcessor extends AbstractMIPsAndMoviesProcessor {

    @Inject
    SimpleMIPsAndMoviesProcessor(ServiceComputationFactory computationFactory,
                                 JacsServiceDataPersistence jacsServiceDataPersistence,
                                 @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                 @PropertyValue(name = "Fiji.SimpleMIPsAndMovies") String simpleMIPsAndMoviesMacro,
                                 @PropertyValue(name = "service.DefaultScratchDir") String scratchLocation,
                                 FijiMacroProcessor fijiMacroProcessor,
                                 VideoFormatConverterProcessor mpegConverterProcessor,
                                 Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, simpleMIPsAndMoviesMacro, scratchLocation, fijiMacroProcessor, mpegConverterProcessor, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SimpleMIPsAndMoviesProcessor.class, new MIPsAndMoviesArgs());
    }

    @Override
    protected String getMIPsAndMoviesArgs(MIPsAndMoviesArgs args, Path outputDir) {
        StringJoiner builder = new StringJoiner(",");
        builder.add(outputDir.toString()); // output directory
        builder.add(args.getImageFilePrefix(args.imageFile, args.imageFilePrefix)); // output prefix
        builder.add(args.getImageFileName(args.imageFile)); // input file
        builder.add(StringUtils.defaultIfBlank(args.chanSpec, ""));
        builder.add(StringUtils.defaultIfBlank(args.options, DEFAULT_OPTIONS));
        return builder.toString();
    }

}

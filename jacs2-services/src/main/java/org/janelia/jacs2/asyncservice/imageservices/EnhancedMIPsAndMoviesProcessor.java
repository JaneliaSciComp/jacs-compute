package org.janelia.jacs2.asyncservice.imageservices;

import java.nio.file.Path;
import java.util.StringJoiner;

import jakarta.enterprise.context.Dependent;
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
 * Generates MIPs and movies for a single input stack. Supports customizable colors
 * and other features controlled by chanelSpec and colorSpec parameters.
 *
 * This service differs from the BasicMIPandMovieGenerationService in a number of important ways:
 * 1) Supports different enhancement modes for "mcfo", "polarity", and "none".
 * 2) Does not support legends.
 * 3) Only supports grey reference channels.
 */
@Dependent
@Named("enhancedMIPsAndMovies")
public class EnhancedMIPsAndMoviesProcessor extends AbstractMIPsAndMoviesProcessor {

    @Inject
    EnhancedMIPsAndMoviesProcessor(ServiceComputationFactory computationFactory,
                                   JacsServiceDataPersistence jacsServiceDataPersistence,
                                   @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                   @PropertyValue(name = "Fiji.EnhancedMIPsAndMovies") String enhancedMIPsAndMoviesMacro,
                                   @PropertyValue(name = "service.DefaultScratchDir") String scratchLocation,
                                   FijiMacroProcessor fijiMacroProcessor,
                                   VideoFormatConverterProcessor mpegConverterProcessor,
                                   Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, enhancedMIPsAndMoviesMacro, scratchLocation, fijiMacroProcessor, mpegConverterProcessor, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(EnhancedMIPsAndMoviesProcessor.class, new MIPsAndMoviesArgs());
    }

    protected String getMIPsAndMoviesArgs(MIPsAndMoviesArgs args, Path outputDir) {
        StringJoiner builder = new StringJoiner(",");
        builder.add(outputDir.toString()); // output directory
        builder.add(args.getImageFilePrefix(args.imageFile, args.imageFilePrefix)); // output prefix
        builder.add(args.mode); // mode
        builder.add(args.imageFile); // input file
        builder.add(args.chanSpec);
        builder.add(StringUtils.defaultIfBlank(args.colorSpec, ""));
        builder.add(StringUtils.defaultIfBlank(args.options, DEFAULT_OPTIONS));

        return builder.toString();
    }

}

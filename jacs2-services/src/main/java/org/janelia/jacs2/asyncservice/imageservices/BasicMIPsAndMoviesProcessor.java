package org.janelia.jacs2.asyncservice.imageservices;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Path;
import java.util.StringJoiner;

/**
 * Generates MIPs and movies for a single input stacks. Supports customizable colors
 * and other features controlled by chanelSpec and colorSpec parameters.
 *
 * 1) If a second image is specified it is normalized to the first one.
 * 2) It supports legends
 */
@Named("basicMIPsAndMovies")
public class BasicMIPsAndMoviesProcessor extends AbstractMIPsAndMoviesProcessor {

    @Inject
    BasicMIPsAndMoviesProcessor(ServiceComputationFactory computationFactory,
                                JacsServiceDataPersistence jacsServiceDataPersistence,
                                @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                @PropertyValue(name = "Fiji.BasicMIPsAndMovies") String basicMIPsAndMoviesMacro,
                                @PropertyValue(name = "service.DefaultScratchDir") String scratchLocation,
                                FijiMacroProcessor fijiMacroProcessor,
                                VideoFormatConverterProcessor mpegConverterProcessor,
                                Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, basicMIPsAndMoviesMacro, scratchLocation, fijiMacroProcessor, mpegConverterProcessor, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(BasicMIPsAndMoviesProcessor.class, new MIPsAndMoviesArgs());
    }

    @Override
    protected String getMIPsAndMoviesArgs(MIPsAndMoviesArgs args, Path outputDir) {
        StringJoiner builder = new StringJoiner(",");
        builder.add(outputDir.toString()); // output directory
        builder.add(args.getImageFilePrefix(args.imageFile, args.imageFilePrefix)); // output prefix 1
        builder.add(args.getImageFilePrefix(args.secondImageFile, args.secondImageFilePrefix)); // output prefix 2
        builder.add(args.getImageFileName(args.imageFile)); // input file 1
        builder.add(args.getImageFileName(args.secondImageFile)); // input file 2
        builder.add(args.laser == null ? "" : args.laser.toString());
        builder.add(args.gain == null ? "" : args.gain.toString());
        builder.add(args.chanSpec);
        builder.add(StringUtils.defaultIfBlank(args.colorSpec, ""));
        builder.add(StringUtils.defaultIfBlank(args.divSpec, ""));
        builder.add(StringUtils.defaultIfBlank(args.options, DEFAULT_OPTIONS));
        return builder.toString();
    }

}

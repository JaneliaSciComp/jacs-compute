package org.janelia.jacs2.asyncservice.imagesearch;

import com.beust.jcommander.Parameter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.domain.ChanSpecUtils;
import org.janelia.model.domain.gui.cdmip.ColorDepthFileComponents;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.sample.ObjectiveSample;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.sample.SampleAlignmentResult;
import org.janelia.model.domain.sample.SamplePipelineRun;
import org.janelia.model.service.JacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;

/**
 * Walk existing color depth libraries and validate them against existing samples.
 *
 * Delete rogue images and flag anything that looks suspicious.
 *
 * This service takes care of corner cases such as when a sample's gender is changed, the alignment
 * is recomputed, and a new CDM image is placed in a different folder, but the old one is not cleaned
 * up. This service should run afterwards and clean things up.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthLibraryValidate")
public class ColorDepthLibraryValidator extends AbstractServiceProcessor<Void> {

    private static final String DELIM = "^";

    static class ValidateArgs extends ServiceArgs {
        @Parameter(names = "-alignmentSpace", description = "Alignment space")
        String alignmentSpace;
        @Parameter(names = "-library", description = "Library identifier. This has to be a root library, not a version of some other library")
        String library;
        @Parameter(names = "-dryRun", description = "Runs without making any changes to the database or filesystem. Everything is logged, but nothing is actually changed.", arity = 0)
        boolean dryRun = true;

        ValidateArgs() {
            super("Color depth library validator");
        }
    }

    private final LegacyDomainDao legacyDomainDao;
    private final ColorDepthImageDao colorDepthImageDao;
    private final JacsNotificationDao jacsNotificationDao;

    @Inject
    ColorDepthLibraryValidator(ServiceComputationFactory computationFactory,
                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                  LegacyDomainDao legacyDomainDao,
                                  @AsyncIndex ColorDepthImageDao colorDepthImageDao,
                                  JacsNotificationDao jacsNotificationDao,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.legacyDomainDao = legacyDomainDao;
        this.colorDepthImageDao = colorDepthImageDao;
        this.jacsNotificationDao = jacsNotificationDao;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthLibrarySynchronizer.class, new ColorDepthLibrarySynchronizer.SyncArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        logger.info("Service {} perform color depth library sync", jacsServiceData);
        logCDLibValidateMaintenanceEvent(jacsServiceData.getId());

        ColorDepthLibraryValidator.ValidateArgs args = getArgs(jacsServiceData);
        runValidation(args);

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private void logCDLibValidateMaintenanceEvent(Number serviceId) {
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setEventName("ColorDepthLibraryValidate");
        jacsNotification.addNotificationData("serviceInstance", serviceId.toString());
        jacsNotification.setNotificationStage(JacsServiceLifecycleStage.PROCESSING);
        jacsNotificationDao.save(jacsNotification);
    }

    private ValidateArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ColorDepthLibraryValidator.ValidateArgs());
    }

    private void runValidation(ColorDepthLibraryValidator.ValidateArgs args) {
        logger.info("Running validation with parameters:");
        logger.info("  alignmentSpace={}", args.alignmentSpace);
        logger.info("  library={}", args.library);
        logger.info("  dryRun={}", args.dryRun);

        long numImagesProcessed = 0;
        Map<String, Map<String, Integer>> counts = legacyDomainDao.getColorDepthCounts();

        for(String libraryIdentifier : new TreeSet<>(counts.keySet())) {

            if (args.library != null && !args.library.equals(libraryIdentifier)) {
                continue;
            }

            logger.info("Processing {}", libraryIdentifier);

            Map<Long, Sample> samples = new HashMap<>();
            for(Sample sample : legacyDomainDao.getSamplesByDataSet(null, libraryIdentifier)) {
                samples.put(sample.getId(), sample);
            }

            for(String alignmentSpace : new TreeSet<>(counts.get(libraryIdentifier).keySet())) {

                if (args.alignmentSpace != null && !args.alignmentSpace.equals(alignmentSpace)) {
                    continue;
                }

                // Get all the CDMs for this library/alignmentSpace and group them by sample
                List<ColorDepthImage> images = legacyDomainDao.getColorDepthImages(null, libraryIdentifier, alignmentSpace);
                Multimap<Long, ColorDepthImage> imagesBySample = HashMultimap.create();
                for(ColorDepthImage image : images) {
                    if (image.getSampleRef()==null) {
                        // This is generally fine. Imported CDMs don't have a sampleRef, e.g. everything from flyem
                        // TODO: it would be nice to distinguish between imported and "native" libraries so that we could run this check.
                        continue;
                    }
                    imagesBySample.put(image.getSampleRef().getTargetId(), image);
                    numImagesProcessed++;
                }

                // Ignore older alignments that were deleted from samples
                if (!alignmentSpace.startsWith("JRC2018")) continue;

                long numProcessed = 0;
                long numFixed = 0;

                List<ColorDepthImage> toDelete = new ArrayList<>();

                for(Long sampleId : imagesBySample.keySet()) {
                    Sample sample = samples.get(sampleId);

                    if (sample==null) {
                        // Try fetching the sample directly.
                        // This is necessary e.g. when the library is constructed from samples in other data sets
                        sample = legacyDomainDao.getDomainObject(null, Sample.class, sampleId);
                        if (sample==null) {
                            logger.warn("Sample no longer exists: {}", sampleId);
                            continue;
                        }
                    }

                    // Create set of valid CDMs in this sample
                    Set<String> cdms = new HashSet<>();
                    for(ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {
                        for(SamplePipelineRun run : objectiveSample.getPipelineRuns()) {
                            for(SampleAlignmentResult alignment : run.getAlignmentResults()) {
                                if (alignment.getAlignmentSpace().equals(alignmentSpace)) {
                                    List<Integer> signals = ChanSpecUtils.getSignalChannelIndexList(alignment.getChannelSpec());
                                    for(Integer signalIndex : signals) {
                                        String cdm = objectiveSample.getObjective() + DELIM + alignment.getAlignmentSpace()
                                                + DELIM + alignment.getAnatomicalArea() + DELIM + (signalIndex+1);
                                        cdms.add(cdm);
                                    }
                                }
                            }
                        }
                    }

                    // Compare all the CDMs actually found with the valid CDMs
                    for (ColorDepthImage image : imagesBySample.get(sampleId)) {
                        ColorDepthFileComponents file = ColorDepthFileComponents.fromFilepath(image.getFilepath());
                        String cdm = file.getObjective() + DELIM + file.getAlignmentSpace()
                                + DELIM + file.getAnatomicalArea() + DELIM + file.getChannelNumber();
                        if (!cdms.contains(cdm)) {
                            logger.info("Will delete rogue CDM: {}", image.getFilepath());
                            toDelete.add(image);
                        }
                        else {
                            numProcessed++;
                        }
                    }
                }

                for (ColorDepthImage colorDepthImage : toDelete) {

                    if (!args.dryRun) {
                        // TODO: delete file on disk
                        // TODO: delete object in database
                    }

                    numFixed++;
                }

                long total = images.size();
                long numErrors = total - numProcessed;
                long numRemaining = numErrors - numFixed;
                if (numErrors>0) {
                    logger.info("{} - {} - Processed {} images. Detected {} issues and fixed {}.",
                            libraryIdentifier, alignmentSpace, numProcessed, numErrors, numFixed);
                }
                if (numRemaining>0) {
                    logger.warn("Library has unresolved issues: {} - {}", libraryIdentifier, alignmentSpace);
                }
            }
        }

        logger.info("Processed {} images", numImagesProcessed);
    }
}

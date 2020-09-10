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
        @Parameter(names = "-dryRun", description = "Runs without making any changes to the database or filesystem. Everything is logged, but nothing is actually changed.")
        boolean dryRun = false;
        @Parameter(names = "-force", description = "Forces deletions even if they exceed safety limits. This only has an effect if dryRun is false.")
        boolean force = false;

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
        return ServiceArgs.getMetadata(ColorDepthLibraryValidator.class, new ColorDepthLibraryValidator.ValidateArgs());
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
        logger.info("  force={}", args.force);

        long totalNumLibrariesProcessed = 0;
        long totalNumImagesProcessed = 0;
        long totalNumDeleted = 0;

        Map<String, Map<String, Integer>> counts = legacyDomainDao.getColorDepthCounts();
        TreeSet<String> libraries = new TreeSet<>(counts.keySet());

        for(String libraryIdentifier : libraries) {

            if (args.library != null && !args.library.equals(libraryIdentifier)) {
                continue;
            }
            logger.info("Processing {}", libraryIdentifier);
            totalNumLibrariesProcessed++;

            Map<Long, Sample> samples = new HashMap<>();
            for(Sample sample : legacyDomainDao.getSamplesByDataSet(null, libraryIdentifier)) {
                samples.put(sample.getId(), sample);
            }
            logger.trace("  Found {} samples associated with {}", samples.size(), libraryIdentifier);

            for(String alignmentSpace : new TreeSet<>(counts.get(libraryIdentifier).keySet())) {

                // Ignore older alignments that were deleted from samples
                if (!alignmentSpace.startsWith("JRC2018")) continue;

                if (args.alignmentSpace != null && !args.alignmentSpace.equals(alignmentSpace)) {
                    continue;
                }

                logger.info("  Processing {} - {}", libraryIdentifier, alignmentSpace);

                long numImagesProcessed = 0;
                long numDeleted = 0;

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
                }

                logger.info("    Found {} images mapping to {} samples", images.size(), imagesBySample.keySet().size());

                List<ColorDepthImage> toDelete = new ArrayList<>();

                for(Long sampleId : imagesBySample.keySet()) {
                    Sample sample = samples.get(sampleId);

                    if (sample==null) {
                        // This probably means that we're coming at the sample from a constructed library, and not from
                        // its original data-set-linked library. That's fine, and we'll probably get to it later.
                        // But it could also mean that the sample is gone or is inaccessible,
                        // and this service doesn't currently address that edge case.
                        continue;
                    }

                    // Create set of valid CDMs in this sample
                    Set<String> cdms = new HashSet<>();
                    for(ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {
                        for(SamplePipelineRun run : objectiveSample.getPipelineRuns()) {
                            for(SampleAlignmentResult alignment : run.getAlignmentResults()) {
                                if (alignment.getAlignmentSpace()!=null && alignment.getAlignmentSpace().equals(alignmentSpace)) {
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
                            logger.info("    Will delete rogue CDM: {}", image.getFilepath());
                            toDelete.add(image);
                        }
                        numImagesProcessed++;
                    }
                }

                if (!toDelete.isEmpty()) {
                    double toDeletePct = (double) toDelete.size() / (double) images.size();
                    int toDeletePercent = (int) (toDeletePct * 100);
                    logger.info("    Will delete {} ({}%) of images in {} - {}",
                            toDelete.size(), toDeletePercent, libraryIdentifier, alignmentSpace);

                    // Do not automatically delete if the candidate set is more than 20 images and more than 20% of the library
                    if (toDelete.size() > 20 && toDeletePct > 0.20 && !args.force) {
                        logger.warn("    Percentage of images to delete exceeds safety limit. Resubmit with force=true to continue.");
                        continue;
                    }
                    for (ColorDepthImage image : toDelete) {
                        if (!args.dryRun) {
                            if (deleteColorDepthImage(image)) {
                                numDeleted++;
                            }
                        }
                    }
                    logger.info("    {} - {} - Processed {} images, deleted {} rogue CDMs.",
                            libraryIdentifier, alignmentSpace, numImagesProcessed, numDeleted);
                }

                totalNumImagesProcessed += numImagesProcessed;
                totalNumDeleted += numDeleted;
            }
        }

        logger.info("Processed {} color depth libraries", totalNumLibrariesProcessed);
        logger.info("Processed {} images in total, and deleted {} rogue CDMs.", totalNumImagesProcessed, totalNumDeleted);
    }

    private boolean deleteColorDepthImage(ColorDepthImage image) {
        // TODO: delete file on disk
        // TODO: delete object in database
        return true; // success
    }
}

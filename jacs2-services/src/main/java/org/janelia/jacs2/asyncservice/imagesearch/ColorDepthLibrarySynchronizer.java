package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.gui.cdmip.ColorDepthFileComponents;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibrary;
import org.janelia.model.domain.sample.DataSet;
import org.janelia.model.domain.sample.LineRelease;
import org.janelia.model.security.Subject;
import org.janelia.model.service.JacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Synchronize the filesystem ColorDepthMIPs directory with the database. Can be restricted to only
 * process a single alignmentSpace or library.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthLibrarySync")
public class ColorDepthLibrarySynchronizer extends AbstractServiceProcessor<Void> {

    private static final String DEFAULT_OWNER = "group:flylight";

    static class SyncArgs extends ServiceArgs {
        @Parameter(names = "-alignmentSpace", description = "Alignment space")
        String alignmentSpace;
        @Parameter(names = "-library", description = "Library identifier")
        String library;
        @Parameter(names = "-skipFileDiscovery", description = "If set skips the file system based discovery", arity = 0)
        boolean skipFileSystemDiscovery = false;
        @Parameter(names = "-skipPublishedDiscovery", description = "If set skips the published based discovery", arity = 0)
        boolean skipPublishedDiscovery = false;
        @Parameter(names = "-publishedCollections", description = "list of release names", variableArity = true)
        List<String> publishedCollections;
        @Parameter(names = "-publishingSites", description = "list of publishing web sites", variableArity = true)
        List<String> publishingSites;

        SyncArgs() {
            super("Color depth library synchronization");
        }
    }

    private final Path rootPath;
    private final LegacyDomainDao legacyDomainDao;
    private final ColorDepthImageDao colorDepthImageDao;
    private final LineReleaseDao lineReleaseDao;
    private final JacsNotificationDao jacsNotificationDao;
    private int existing = 0;
    private int created = 0;
    private int deleted = 0;
    private int totalCreated = 0;
    private int totalDeleted = 0;

    @Inject
    ColorDepthLibrarySynchronizer(ServiceComputationFactory computationFactory,
                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                  @StrPropertyValue(name = "service.colorDepthSearch.filepath") String rootPath,
                                  LegacyDomainDao legacyDomainDao,
                                  ColorDepthImageDao colorDepthImageDao,
                                  LineReleaseDao lineReleaseDao,
                                  JacsNotificationDao jacsNotificationDao,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.rootPath = Paths.get(rootPath);
        this.legacyDomainDao = legacyDomainDao;
        this.colorDepthImageDao = colorDepthImageDao;
        this.lineReleaseDao = lineReleaseDao;
        this.jacsNotificationDao = jacsNotificationDao;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthLibrarySynchronizer.class, new SyncArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        logger.info("Service {} perform color depth library sync", jacsServiceData);
        logCDLibSyncMaintenanceEvent(jacsServiceData.getId());

        SyncArgs args = getArgs(jacsServiceData);

        // Get currently existing libraries
        Map<String, ColorDepthLibrary> indexedLibraries = legacyDomainDao.getDomainObjects(null, ColorDepthLibrary.class).stream()
                .filter(cdl -> StringUtils.isBlank(args.library) || StringUtils.equalsIgnoreCase(args.library, cdl.getIdentifier()))
                .collect(Collectors.toMap(ColorDepthLibrary::getIdentifier, Function.identity()));


        if (!args.skipFileSystemDiscovery) runFileSystemBasedDiscovery(args, indexedLibraries);
        if (!args.skipPublishedDiscovery) runPublishedLinesBasedDiscovery(args, indexedLibraries);

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private void logCDLibSyncMaintenanceEvent(Number serviceId) {
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setEventName("ColorDepthLibrarySync");
        jacsNotification.addNotificationData("serviceInstance", serviceId.toString());
        jacsNotification.setNotificationStage(JacsServiceLifecycleStage.PROCESSING);
        jacsNotificationDao.save(jacsNotification);
    }

    private SyncArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SyncArgs());
    }

    private void runFileSystemBasedDiscovery(SyncArgs args, Map<String, ColorDepthLibrary> indexedLibraries) {
        logger.info("Running discovery with parameters:");
        logger.info("  alignmentSpace={}", args.alignmentSpace);
        logger.info("  library={}", args.library);

        // Walk the relevant alignment directories
        walkChildDirs(rootPath.toFile())
                .filter(alignmentDir -> StringUtils.isBlank(args.alignmentSpace) || alignmentDir.getName().equals(args.alignmentSpace))
                .flatMap(alignmentDir -> walkChildDirs(alignmentDir))
                .filter(libraryDir -> StringUtils.isBlank(args.library) || libraryDir.getName().equals(args.library))
                .forEach(libraryDir -> processLibraryDir(libraryDir, libraryDir.getParentFile().getName(), null, indexedLibraries));

        // It's necessary to recalculate all the counts here, because some color depth images may be part of constructed
        // libraries which are not represented explicitly on disk.
        try {
            legacyDomainDao.updateColorDepthCounts(legacyDomainDao.getColorDepthCounts());
        } catch (Exception e) {
           logger.error("Failed to update color depth counts", e);
        }

        logger.info("Completed color depth library synchronization. Imported {} images in total - deleted {}.", totalCreated, totalDeleted);
    }

    private void processLibraryDir(File libraryDir, String alignmentSpace, ColorDepthLibrary parentLibrary, Map<String, ColorDepthLibrary> indexedLibraries) {
        logger.info("Discovering files in {}", libraryDir);
        String libraryIdentifier = parentLibrary == null ? libraryDir.getName() : parentLibrary.getIdentifier() + '_' + libraryDir.getName();
        String libraryVersion = parentLibrary == null ? null : libraryDir.getName();

        // This prefetch is an optimization so that we can efficiency check which images already exist
        // in the database without incurring a huge cost for each image.
        // Figure out the owner of the library and the images
        ColorDepthLibrary library;
        if (indexedLibraries.get(libraryIdentifier) == null) {
            library = createNewLibrary(libraryIdentifier, libraryVersion, parentLibrary);
        } else {
            library = indexedLibraries.get(libraryIdentifier);
        }

        processLibraryFiles(libraryDir, alignmentSpace, library);

        logger.info("  Verified {} existing images, created {} images", existing, created);

        int total = existing + created - deleted;
        totalCreated += created;
        totalDeleted += deleted;

        if (library.getId() != null || total > 0) {
            // If the library exists already, or should be created
            library.getColorDepthCounts().put(alignmentSpace, total);
            try {
                indexedLibraries.put(libraryIdentifier, legacyDomainDao.save(library.getOwnerKey(), library));
                logger.debug("  Saved color depth library {} with count {}", libraryIdentifier, total);
            } catch (Exception e) {
                logger.error("Could not update library file counts for: {}", libraryIdentifier, e);
            }
        }

        processLibraryVersions(libraryDir, alignmentSpace, library, indexedLibraries);
    }

    private Stream<File> walkChildDirs(File dir) {
        if (dir == null) {
            return Stream.of();
        }
        Path dirPath = dir.toPath();
        if (!Files.isDirectory(dirPath)) {
            return Stream.of();
        }
        logger.info("Discovering files in {}", dir);
        return FileUtils.lookupFiles(dirPath, 1, "glob:**/*")
                .filter(p -> !p.toFile().equals(dir))
                .filter(p -> Files.isDirectory(p)).map(Path::toFile);
    }

    private ColorDepthLibrary createNewLibrary(String libraryIdentifier, String libraryVersion, ColorDepthLibrary parentLibrary) {
        ColorDepthLibrary library = new ColorDepthLibrary();
        library.setIdentifier(libraryIdentifier);
        library.setName(libraryIdentifier);
        library.setVersion(libraryVersion);
        library.setParentLibraryRef(parentLibrary == null ? null : Reference.createFor(parentLibrary));

        DataSet dataSet = legacyDomainDao.getDataSetByIdentifier(null, libraryIdentifier);
        if (dataSet != null) {
            // Copy permissions from corresponding data set
            library.setOwnerKey(dataSet.getOwnerKey());
            library.setReaders(dataSet.getReaders());
            library.setWriters(dataSet.getWriters());
        } else {
            String ownerName = libraryIdentifier.split("_")[0];
            Subject subject = legacyDomainDao.getSubjectByName(ownerName);
            if (subject != null) {
                logger.warn("No corresponding data set found. Falling back on owner encoded in library identifier: {}", subject.getKey());
                library.setOwnerKey(subject.getKey());
            } else {
                logger.warn("Falling back on default owner: {}", DEFAULT_OWNER);
                library.setOwnerKey(DEFAULT_OWNER);
            }
        }
        return library;
    }

    private void processLibraryFiles(File libraryDir, String alignmentSpace, ColorDepthLibrary library) {
        // reset the counters
        this.existing = 0;
        this.created = 0;
        this.deleted = 0;

        Map<String, Set<ColorDepthFileComponents>> existingColorDepthFiles = legacyDomainDao.getColorDepthPaths(null, library.getIdentifier(), alignmentSpace).stream()
                .map(this::parseColorDepthFileComponents)
                .collect(Collectors.groupingBy(cdf -> {
                    if (cdf.getSampleRef() == null) {
                        return cdf.getFile().getAbsolutePath();
                    } else {
                        return cdf.getSampleRef().getTargetId().toString();
                    }
                }, Collectors.toSet()));

        if (!existingColorDepthFiles.isEmpty()) {
            logger.info("  Found mips for {} samples", existingColorDepthFiles.size());
        }
        // Walk all images within any structure
        FileUtils.lookupFiles(
                libraryDir.toPath(), 1, "glob:**/*")
                .filter(p -> !p.toFile().equals(libraryDir))
                .map(Path::toFile)
                .filter(File::isFile)
                .filter(f -> {
                    if (accepted(f.getAbsolutePath())) {
                        return true;
                    } else {
                        logger.warn("  File not accepted as color depth MIP: {}", f);
                        return false;
                    }
                })
                .map(f -> parseColorDepthFileComponents(f.getPath()))
                .filter(cdf -> {
                    if (cdf.getSampleRef() == null) {
                        if (existingColorDepthFiles.containsKey(cdf.getFile().getAbsolutePath())) {
                            // this file is already a MIP of this library so no need to do anything else for it
                            existing++;
                            return false;
                        } else {
                            return true;
                        }
                    } else {
                        String sampleId = cdf.getSampleRef().getTargetId().toString();
                        if (existingColorDepthFiles.get(sampleId) == null) {
                            return true;
                        } else {
                            if (existingColorDepthFiles.get(sampleId).contains(cdf)) {
                                // the file exists but it is possible some clean up will be needed in the post phase
                                // anyway for now mark it as existing and don't do anything for this file yet
                                existing++;
                                return false;
                            } else {
                                // the file is not found in the mips set
                                // check if all other files are older
                                Set<String> existingSampleNames = existingColorDepthFiles.get(sampleId).stream()
                                        .map(ColorDepthFileComponents::getSampleName)
                                        .filter(StringUtils::isNotBlank)
                                        .collect(Collectors.toSet());
                                if (existingSampleNames.contains(cdf.getSampleName())) {
                                    // if a sample with the same name is present in the existing set
                                    // it is possible this is a different objective, area or channel
                                    // so continue with the file
                                    return true;
                                } else {
                                    // so the sample name in this cdf is not present in the list
                                    // this is possible if some renaming was done so this is the case
                                    // where we check if this file is newer than all the others
                                    for (ColorDepthFileComponents existingCdf : existingColorDepthFiles.get(sampleId)) {
                                        if (cdf.getObjective().equals(existingCdf.getObjective()) &&
                                                cdf.getAnatomicalArea().equals(existingCdf.getAnatomicalArea()) &&
                                                cdf.getChannelNumber().equals(existingCdf.getChannelNumber())) {
                                            if (cdf.getFile().lastModified() < existingCdf.getFile().lastModified()) {
                                                // the current file is older than one of the existing files
                                                // for the same objective, area and channel so simply skip this
                                                // without incrementing the existing counter
                                                logger.info("Skipping {} because I found {} created for the same sample {} more recently",
                                                        cdf.getFile(), existingCdf.getFile(), existingCdf.getSampleRef());
                                                return false;
                                            }
                                        }
                                    }
                                    // no newer file with the same objective, area and channel was found so process this file
                                    return true;
                                }
                            }
                        }
                    }
                })
                .forEach(cdf -> {
                    if (createColorDepthImage(cdf, alignmentSpace, library)) {
                        created++;
                    }
                });

        // this post phase is for the case when files are already in the library and cleanup is needed.
        legacyDomainDao.getColorDepthPaths(null, library.getIdentifier(), alignmentSpace).stream()
                .map(filepath -> parseColorDepthFileComponents(filepath))
                .filter(cdf -> cdf.getSampleRef() != null)
                .collect(Collectors.groupingBy(cdf -> cdf.getSampleRef().getTargetId().toString(), Collectors.toSet()))
                .entrySet()
                .stream()
                .filter(e -> {
                    if (e.getValue().size() == 1) {
                        return false;
                    } else {
                        Set<String> sampleNames = e.getValue().stream()
                                .map(cdfc -> cdfc.getSampleName())
                                .filter(sn -> StringUtils.isNotBlank(sn))
                                .collect(Collectors.toSet());
                        // if there are 2 or more entries with the same sample ID but different name
                        // there is something wrong and some cleanup it's needed
                        // a possible scenario is that the sample was renamed but the old mips are still there
                        // so we need to "remove" the old mips from the library
                        return sampleNames.size() > 1;
                    }
                })
                .forEach(e -> {
                    Map<String, Set<ColorDepthFileComponents>> cdfByObjectAreaChannel = e.getValue().stream()
                            .collect(Collectors.groupingBy(cdf -> cdf.getObjective() + "-" + cdf.getAnatomicalArea() + "-" + cdf.getChannelNumber(), Collectors.toSet()));
                    cdfByObjectAreaChannel.entrySet().stream().filter(eByOAC -> eByOAC.getValue().size() > 0)
                            .forEach(eByOAC -> {
                                ColorDepthFileComponents latestCDF = eByOAC.getValue().stream().max(Comparator.comparing(cdc -> cdc.getFile().lastModified())).orElse(null);
                                for (ColorDepthFileComponents cdc : eByOAC.getValue()) {
                                    if (cdc != latestCDF) {
                                        // if this is not SAME as the max - remove it from the color depth mips collection
                                        logger.info("Delete color depth image {} created for sample {} - keeping {} for sample {} instead because the sample {} may have been renamed to {}",
                                                cdc.getFile(), cdc.getSampleRef(), latestCDF.getFile(), latestCDF.getSampleRef(),
                                                cdc.getSampleName(), latestCDF.getSampleName());
                                        if (deleteColorDepthImage(cdc)) {
                                            deleted++;
                                            if (existingColorDepthFiles.get(cdc.getSampleRef().getTargetId().toString()) != null &&
                                                !existingColorDepthFiles.get(cdc.getSampleRef().getTargetId().toString()).contains(cdc)) {
                                                // this was a new file so decrement the created count
                                                created--;
                                            }
                                        }
                                    }
                                }
                            });
                });

    }

    private void processLibraryVersions(File libraryDir, String alignmentSpace, ColorDepthLibrary library, Map<String, ColorDepthLibrary> indexedLibraries) {
        // Walk subdirs of the libraryDir
        walkChildDirs(libraryDir)
                .forEach(libraryVersionDir -> processLibraryDir(libraryVersionDir, alignmentSpace, library, indexedLibraries));
    }

    /**
     * Create a ColorDepthImage image for the given file on disk.
     * @param colorDepthImageFileComponents color depth file components
     * @return true if the image was created successfully
     */
    private boolean createColorDepthImage(ColorDepthFileComponents colorDepthImageFileComponents, String alignmentSpace, ColorDepthLibrary library) {
        try {
            ColorDepthImage image = new ColorDepthImage();
            image.getLibraries().add(library.getIdentifier());
            image.setName(colorDepthImageFileComponents.getFile().getName());
            image.setFilepath(colorDepthImageFileComponents.getFile().getPath());
            image.setFileSize(colorDepthImageFileComponents.getFile().length());
            image.setAlignmentSpace(alignmentSpace);
            image.setReaders(library.getReaders());
            image.setWriters(library.getWriters());
            if (colorDepthImageFileComponents.getSampleRef() == null && !alignmentSpace.equals(colorDepthImageFileComponents.getAlignmentSpace())) {
                throw new IllegalStateException("Alignment space does not match path: ("
                        +colorDepthImageFileComponents.getAlignmentSpace()+" != "+alignmentSpace+")");
            }
            image.setSampleRef(colorDepthImageFileComponents.getSampleRef());
            image.setObjective(colorDepthImageFileComponents.getObjective());
            image.setAnatomicalArea(colorDepthImageFileComponents.getAnatomicalArea());
            image.setChannelNumber(colorDepthImageFileComponents.getChannelNumber());
            ColorDepthLibrary sourceLibrary = findSourceLibrary(library);
            if (sourceLibrary != null) {
                Path libraryDir = rootPath.resolve(alignmentSpace).resolve(sourceLibrary.getIdentifier());
                String sourceCDMName = ColorDepthFileComponents.createCDMName(
                        colorDepthImageFileComponents.getSampleName(),
                        colorDepthImageFileComponents.getObjective(),
                        colorDepthImageFileComponents.getAnatomicalArea(),
                        colorDepthImageFileComponents.getAlignmentSpace(),
                        colorDepthImageFileComponents.getSampleRef(),
                        colorDepthImageFileComponents.getChannelNumber(),
                        null
                );
                String sourceMIPFileName = FileUtils.lookupFiles(libraryDir, 1,
                        "glob:**/" + sourceCDMName + "*")
                        .filter(p -> Files.isRegularFile(p))
                        .findFirst()
                        .map(p -> p.toString())
                        .orElse(null);
                if (sourceMIPFileName == null) {
                    logger.warn("Invalid source MIP file name - no file found for {} in {} folder", sourceCDMName, libraryDir);
                } else {
                    ColorDepthImage sourceImage = legacyDomainDao.getColorDepthImageByPath(null, sourceMIPFileName);
                    if (sourceImage != null) {
                        image.setSourceImageRef(Reference.createFor(sourceImage));
                    } else {
                        logger.warn("No color depth image entity found for {}", sourceMIPFileName);
                    }
                }
            }
            legacyDomainDao.save(library.getOwnerKey(), image);
            return true;
        } catch (Exception e) {
            logger.warn("  Could not create image for: {}", colorDepthImageFileComponents.getFile());
        }
        return false;
    }

    private boolean deleteColorDepthImage(ColorDepthFileComponents cdc) {
        ColorDepthImage colorDepthImage = legacyDomainDao.getColorDepthImageByPath(null, cdc.getFile().getAbsolutePath());
        if (colorDepthImage != null) {
            legacyDomainDao.deleteDomainObject(null, colorDepthImage.getClass(), colorDepthImage.getId());
            return true;
        } else {
            return false;
        }
    }

    private ColorDepthLibrary findSourceLibrary(ColorDepthLibrary l) {
        ColorDepthLibrary sourceLibrary = null;
        for (ColorDepthLibrary currentLibrary = l; currentLibrary.hasVersion(); ) {
            ColorDepthLibrary parentLibrary = legacyDomainDao.getDomainObject(null, ColorDepthLibrary.class, currentLibrary.getParentLibraryRef().getTargetId());
            if (parentLibrary == null) {
                logger.error("Invalid parent library reference in {} -> {}", currentLibrary, currentLibrary.getParentLibraryRef());
                throw new IllegalArgumentException("Invalid parent library reference " + currentLibrary.getParentLibraryRef() + " in " + currentLibrary);
            }
            sourceLibrary = parentLibrary;
            currentLibrary = parentLibrary;
        }
        return sourceLibrary;
    }

    private boolean accepted(String filepath) {
        return filepath.endsWith(".png") || filepath.endsWith(".tif");
    }

    private ColorDepthFileComponents parseColorDepthFileComponents(String filepath) {
        return ColorDepthFileComponents.fromFilepath(filepath);
    }

    private void runPublishedLinesBasedDiscovery(SyncArgs args, Map<String, ColorDepthLibrary> indexedLibraries) {
        Map<String, List<LineRelease>> releasesByWebsite = retrieveLineReleases(args.publishedCollections, args.publishingSites);
        releasesByWebsite
                .forEach((site, releases) -> {
                    String libraryIdentifier = "flylight_" + site.toLowerCase().replace(' ', '_') + "_published";
                    ColorDepthLibrary library;
                    boolean libraryCreated;
                    if (indexedLibraries.get(libraryIdentifier) == null) {
                        library = createLibraryForPublishedRelease(libraryIdentifier);
                        libraryCreated = true;
                    } else {
                        library = indexedLibraries.get(libraryIdentifier);
                        libraryCreated = false;
                    }
                    boolean libraryUpdated = releases.stream().map(lr -> library.addReaders(lr.getReaders()) ||
                            library.addWriters(lr.getWriters()) ||
                            library.addSourceRelease(Reference.createFor(lr)))
                            .reduce(false, (r1, r2) -> r1 || r2) && !libraryCreated;

                    AtomicInteger counter = new AtomicInteger();
                    long updatedMips = releases.stream()
                            .flatMap(lr -> lr.getChildren().stream())
                            .collect(Collectors.groupingBy(
                                    ref -> counter.getAndIncrement() / 10000,
                                    Collectors.collectingAndThen(
                                            Collectors.toSet(),
                                            sampleRefs -> colorDepthImageDao.addLibraryBySampleRefs(libraryIdentifier, sampleRefs))))
                            .entrySet().stream()
                            .map(Map.Entry::getValue)
                            .reduce(0L, (v1, v2) -> v1 + v2)
                            ;
                    if (libraryCreated && updatedMips > 0 || libraryUpdated) {
                        if (updatedMips > 0) {
                            library.setColorDepthCounts(
                                    colorDepthImageDao.countColorDepthMIPsByAlignmentSpaceForLibrary(libraryIdentifier)
                            );
                        }
                        try {
                            legacyDomainDao.save(library.getOwnerKey(), library);
                        } catch (Exception e) {
                            logger.error("Could not update library file counts for: {}", libraryIdentifier, e);
                        }
                    }
                });
    }

    private ColorDepthLibrary createLibraryForPublishedRelease(String libraryIdentifier) {
        ColorDepthLibrary library = new ColorDepthLibrary();
        library.setIdentifier(libraryIdentifier);
        library.setName(libraryIdentifier);
        library.setOwnerKey(DEFAULT_OWNER);
        return library;
    }

    private Map<String, List<LineRelease>> retrieveLineReleases(List<String> releaseNames, List<String> publishingSites) {
        List<LineRelease> lineReleases;
        if (CollectionUtils.isEmpty(releaseNames) && CollectionUtils.isEmpty(publishingSites)) {
            lineReleases = lineReleaseDao.findAll(0, -1);
        } else if (CollectionUtils.isNotEmpty(releaseNames)) {
            lineReleases = lineReleaseDao.findReleasesByName(releaseNames);
        } else {
            lineReleases = lineReleaseDao.findReleasesByPublishingSites(publishingSites);
        }
        return lineReleases.stream().collect(Collectors.groupingBy(LineRelease::getTargetWebsite, Collectors.toList()));
    }

}

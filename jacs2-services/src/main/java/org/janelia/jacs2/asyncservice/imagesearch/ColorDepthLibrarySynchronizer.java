package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.janelia.model.access.domain.dao.*;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.flyem.EMBody;
import org.janelia.model.domain.flyem.EMDataSet;
import org.janelia.model.domain.gui.cdmip.ColorDepthFileComponents;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibrary;
import org.janelia.model.domain.sample.DataSet;
import org.janelia.model.domain.sample.Image;
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

    static class SyncArgs extends ServiceArgs {
        @Parameter(names = "-alignmentSpace", description = "Alignment space")
        String alignmentSpace;
        @Parameter(names = "-library", description = "Library identifier. This has to be a root library, not a variant of another library")
        String library;
        @Parameter(names = "-skipFileDiscovery", description = "If set skips the file system based discovery", arity = 0)
        boolean skipFileSystemDiscovery = false;
        @Parameter(names = "-includePublishedDiscovery", description = "If set also perform the published based discovery", arity = 0)
        boolean includePublishedDiscovery = false;
        @Parameter(names = "-publishedCollections", description = "list of release names", variableArity = true)
        List<String> publishedCollections;
        @Parameter(names = "-publishingSites", description = "list of publishing web sites", variableArity = true)
        List<String> publishingSites;

        SyncArgs() {
            super("Color depth library synchronization");
        }
    }

    private final Path rootPath;
    private final SubjectDao subjectDao;
    private final ColorDepthLibraryDao colorDepthLibraryDao;
    private final ColorDepthImageDao colorDepthImageDao;
    private final LineReleaseDao lineReleaseDao;
    private final AnnotationDao annotationDao;
    private final EmDataSetDao emDataSetDao;
    private final EmBodyDao emBodyDao;
    private final DatasetDao datasetDao;
    private final JacsNotificationDao jacsNotificationDao;
    private final String defaultOwnerKey;
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
                                  SubjectDao subjectDao,
                                  ColorDepthLibraryDao colorDepthLibraryDao,
                                  ColorDepthImageDao colorDepthImageDao,
                                  LineReleaseDao lineReleaseDao,
                                  AnnotationDao annotationDao,
                                  EmDataSetDao emDataSetDao,
                                  EmBodyDao emBodyDao,
                                  DatasetDao datasetDao,
                                  JacsNotificationDao jacsNotificationDao,
                                  @StrPropertyValue(name = "user.defaultReadGroups") String defaultOwnerKey,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.rootPath = Paths.get(rootPath);
        this.subjectDao = subjectDao;
        this.colorDepthLibraryDao = colorDepthLibraryDao;
        this.colorDepthImageDao = colorDepthImageDao;
        this.lineReleaseDao = lineReleaseDao;
        this.annotationDao = annotationDao;
        this.emDataSetDao = emDataSetDao;
        this.emBodyDao = emBodyDao;
        this.datasetDao = datasetDao;
        this.jacsNotificationDao = jacsNotificationDao;
        this.defaultOwnerKey = defaultOwnerKey;
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
        Map<String, ColorDepthLibrary> allIndexedLibraries = colorDepthLibraryDao.findAll(0, -1).stream()
                .collect(Collectors.toMap(ColorDepthLibrary::getIdentifier, Function.identity()));

        if (args.includePublishedDiscovery) runPublishedLinesBasedDiscovery(args, allIndexedLibraries);
        if (!args.skipFileSystemDiscovery) runFileSystemBasedDiscovery(args, allIndexedLibraries);

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
                .forEach(libraryDir -> {

                    // Read optional metadata
                    ColorDepthLibraryEmMetadata emMetadata = null;
                    try {
                        emMetadata = ColorDepthLibraryEmMetadata.fromLibraryPath(libraryDir);
                    } catch (Exception e) {
                        logger.error("Error reading EM metadata for "+libraryDir, e);
                    }

                    processLibraryDir(libraryDir, libraryDir.getParentFile().getName(), null, indexedLibraries, emMetadata);
                });

        // It's necessary to recalculate all the counts here, because some color depth images may be part of constructed
        // libraries which are not represented explicitly on disk.
        try {
            colorDepthLibraryDao.updateColorDepthCounts(colorDepthImageDao.countColorDepthMIPsByAlignmentSpaceForAllLibraries());
        } catch (Exception e) {
           logger.error("Failed to update color depth counts", e);
        }

        logger.info("Completed color depth library synchronization. Imported {} images in total - deleted {}.", totalCreated, totalDeleted);
    }

    private void processLibraryDir(File libraryDir, String alignmentSpace, ColorDepthLibrary parentLibrary, Map<String, ColorDepthLibrary> indexedLibraries, ColorDepthLibraryEmMetadata emMetadata) {
        logger.info("Discovering files in {}", libraryDir);
        String libraryIdentifier = parentLibrary == null ? libraryDir.getName() : parentLibrary.getIdentifier() + '_' + libraryDir.getName();
        String libraryVariant = parentLibrary == null ? null : libraryDir.getName();

        ColorDepthLibrary library;
        if (indexedLibraries.get(libraryIdentifier) == null) {
            library = createNewLibrary(libraryIdentifier, libraryVariant, parentLibrary);
        } else {
            library = indexedLibraries.get(libraryIdentifier);
        }

        processLibraryFiles(libraryDir, alignmentSpace, library);
        logger.info("  Verified {} existing images, created {} images", existing, created);

        if (emMetadata != null) {
            processEmMetadata(library, alignmentSpace, emMetadata);
            logger.info("  Associated library with EM data set {}", emMetadata.getDataSetIdentifier());
        }

        int total = existing + created - deleted;
        totalCreated += created;
        totalDeleted += deleted;

        if (library.getId() != null || total > 0) {
            // If the library exists already, or should be created
            library.getColorDepthCounts().put(alignmentSpace, total);
            try {
                indexedLibraries.put(
                        libraryIdentifier,
                        colorDepthLibraryDao.saveBySubjectKey(library, library.getOwnerKey())
                );
                logger.debug("  Saved color depth library {} with count {}", libraryIdentifier, total);
            } catch (Exception e) {
                logger.error("Could not update library file counts for: {}", libraryIdentifier, e);
            }
        }

        // Indirect recursion
        processLibraryVariants(libraryDir, alignmentSpace, library, indexedLibraries, emMetadata);
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
                .filter(Files::isDirectory)
                .map(Path::toFile)
                .filter(p -> !p.equals(dir));
    }

    private ColorDepthLibrary createNewLibrary(String libraryIdentifier, String libraryVariant, ColorDepthLibrary parentLibrary) {
        logger.info("Create new library {} - library variant {}", libraryIdentifier, StringUtils.defaultIfBlank(libraryVariant, "<<NONE>>"));
        ColorDepthLibrary library = new ColorDepthLibrary();
        library.setIdentifier(libraryIdentifier);
        library.setName(libraryIdentifier);
        library.setVariant(libraryVariant);
        library.setParentLibraryRef(parentLibrary == null ? null : Reference.createFor(parentLibrary));

        DataSet dataSet = datasetDao.getDataSetByIdentifier(libraryIdentifier);
        if (dataSet != null) {
            // If data set exists with the same identifier, copy permissions from the data set
            library.setOwnerKey(dataSet.getOwnerKey());
            library.setReaders(dataSet.getReaders());
            library.setWriters(dataSet.getWriters());
        } else {
            // Otherwise, assign the library to the owner indicated by its name
            String ownerName = libraryIdentifier.split("_")[0];
            Subject subject = subjectDao.findSubjectByName(ownerName);
            Set<String> defaultAccessors = Collections.singleton(defaultOwnerKey);
            if (subject != null) {
                logger.warn("No corresponding data set found. Falling back on owner encoded in library identifier: {}", subject.getKey());
                library.setOwnerKey(subject.getKey());
                library.addReaders(defaultAccessors);
                library.addWriters(defaultAccessors);
            } else {
                logger.warn("Falling back on default owner: {}", defaultOwnerKey);
                library.setOwnerKey(defaultOwnerKey);
            }
        }
        return library;
    }

    private void processLibraryFiles(File libraryDir, String alignmentSpace, ColorDepthLibrary library) {
        // reset the counters
        this.existing = 0;
        this.created = 0;
        this.deleted = 0;

        ColorDepthImageQuery mipsQuery = new ColorDepthImageQuery()
                .withLibraryIdentifiers(Collections.singleton(library.getIdentifier()))
                .withAlignmentSpace(alignmentSpace);
        Map<String, Set<ColorDepthFileComponents>> existingColorDepthFiles = colorDepthImageDao.streamColorDepthMIPs(mipsQuery)
                .map(Image::getFilepath)
                .map(this::parseColorDepthFileComponents)
                .collect(Collectors.groupingBy(cdf -> {
                    if (cdf.getSampleRef() == null) {
                        return cdf.getFile().getAbsolutePath();
                    } else {
                        return cdf.getSampleRef().getTargetId().toString();
                    }
                }, Collectors.toSet()));

        if (!existingColorDepthFiles.isEmpty()) {
            logger.info("  Found mips for {} samples in library {}", existingColorDepthFiles.size(), library.getIdentifier());
        }
        // Walk all images within any structure
        FileUtils.lookupFiles(
                libraryDir.toPath(), 1, "glob:**/*")
                .parallel()
                .filter(p -> !p.toFile().equals(libraryDir))
                .map(Path::toFile)
                .filter(File::isFile)
                .filter(f -> {
                    if (accepted(f.getAbsolutePath())) {
                        return true;
                    } else {
                        // Ignore JSON files, which could be added metadata
                        if (!f.getName().endsWith(".json")) {
                            logger.warn("  File not accepted as color depth MIP: {}", f);
                        }
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
                                                logger.debug("Skipping {} because I found {} created for the same sample {} more recently",
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
        colorDepthImageDao.streamColorDepthMIPs(mipsQuery)
                .map(Image::getFilepath)
                .map(this::parseColorDepthFileComponents)
                .filter(cdf -> cdf.getSampleRef() != null)
                .collect(Collectors.groupingBy(cdf -> cdf.getSampleRef().getTargetId().toString(), Collectors.toSet()))
                .entrySet()
                .stream()
                .filter(e -> {
                    if (e.getValue().size() == 1) {
                        return false;
                    } else {
                        Set<String> sampleNames = e.getValue().stream()
                                .map(ColorDepthFileComponents::getSampleName)
                                .filter(StringUtils::isNotBlank)
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

    private void processLibraryVariants(File libraryDir, String alignmentSpace, ColorDepthLibrary library, Map<String, ColorDepthLibrary> indexedLibraries, ColorDepthLibraryEmMetadata emMetadata) {
        // Walk subdirs of the libraryDir
        walkChildDirs(libraryDir)
                .forEach(libraryVariantDir -> processLibraryDir(libraryVariantDir, alignmentSpace, library, indexedLibraries, emMetadata));
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
            if (colorDepthImageFileComponents.hasNameComponents()) {
                if (colorDepthImageFileComponents.getSampleRef() != null && !alignmentSpace.equals(colorDepthImageFileComponents.getAlignmentSpace())) {
                    throw new IllegalStateException("Alignment space does not match path: ("
                            +colorDepthImageFileComponents.getAlignmentSpace()+" != "+alignmentSpace+")");
                }
                image.setSampleRef(colorDepthImageFileComponents.getSampleRef());
                image.setObjective(colorDepthImageFileComponents.getObjective());
                image.setAnatomicalArea(colorDepthImageFileComponents.getAnatomicalArea());
                image.setChannelNumber(colorDepthImageFileComponents.getChannelNumber());
            }
            ColorDepthLibrary variantSourceLibrary = findVariantSource(library);
            if (variantSourceLibrary != null) {
                Set<String> sourceCDMNameCandidates;
                if (colorDepthImageFileComponents.hasNameComponents()) {
                    sourceCDMNameCandidates = ImmutableSet.of(
                            ColorDepthFileComponents.createCDMNameFromNameComponents(
                                    colorDepthImageFileComponents.getSampleName(),
                                    colorDepthImageFileComponents.getObjective(),
                                    colorDepthImageFileComponents.getAnatomicalArea(),
                                    colorDepthImageFileComponents.getAlignmentSpace(),
                                    colorDepthImageFileComponents.getSampleRef(),
                                    colorDepthImageFileComponents.getChannelNumber(),
                                    null)
                    );
                } else {
                    if (library.isVariant()) {
                        // if the mip name does not follow the convention assume the variant is in the file name
                        // remove the variant from the filename
                        String n1 = Pattern.compile("[_-]" + library.getVariant() + "$", Pattern.CASE_INSENSITIVE)
                                .matcher(colorDepthImageFileComponents.getFileName())
                                .replaceAll(StringUtils.EMPTY);
                        String n2 = Pattern.compile("-.*CDM$", Pattern.CASE_INSENSITIVE)
                                .matcher(n1)
                                .replaceFirst(StringUtils.EMPTY);
                        int lastSepIndex = n2.lastIndexOf('_');
                        if (lastSepIndex > 0) {
                            sourceCDMNameCandidates = ImmutableSet.of(n1, n2, n2.substring(0, lastSepIndex));
                        } else {
                            sourceCDMNameCandidates = ImmutableSet.of(n1, n2);
                        }
                    } else {
                        sourceCDMNameCandidates = ImmutableSet.of(colorDepthImageFileComponents.getFileName());
                    }
                }
                logger.debug("Lookup {} in {}, alignmentSpace: {}", sourceCDMNameCandidates, variantSourceLibrary.getIdentifier(), alignmentSpace);
                ColorDepthImage sourceImage = colorDepthImageDao.streamColorDepthMIPs(
                        new ColorDepthImageQuery()
                                .withLibraryIdentifiers(Collections.singletonList(variantSourceLibrary.getIdentifier()))
                                .withAlignmentSpace(alignmentSpace)
                                .withFuzzyNames(sourceCDMNameCandidates)
                                .withFuzzyFilepaths(sourceCDMNameCandidates)
                ).findFirst().orElse(null);
                if (sourceImage != null) {
                    image.setSourceImageRef(Reference.createFor(sourceImage));
                } else {
                    // this is the case when the file exist but the mip entity was deleted because
                    // it actually corresponds to a renamed mip
                    logger.warn("No color depth image entity found for {} in library {}, alignment {}, so no MIP will be created",
                            sourceCDMNameCandidates, variantSourceLibrary.getIdentifier(), alignmentSpace);
                    return false;
                }
            }
            colorDepthImageDao.saveBySubjectKey(image, library.getOwnerKey());
            return true;
        } catch (Exception e) {
            logger.warn("  Could not create image for: {}", colorDepthImageFileComponents.getFile(), e);
        }
        return false;
    }

    private boolean deleteColorDepthImage(ColorDepthFileComponents cdc) {
        return colorDepthImageDao.findColorDepthImageByPath(cdc.getFile().getAbsolutePath())
                .map(colorDepthImage -> {
                    colorDepthImageDao.delete(colorDepthImage);
                    return true;
                })
                .orElse(false);
    }

    private ColorDepthLibrary findVariantSource(ColorDepthLibrary libraryVariant) {
        ColorDepthLibrary sourceLibrary = null;
        for (ColorDepthLibrary currentLibraryVariant = libraryVariant; currentLibraryVariant.isVariant(); ) {
            ColorDepthLibrary parentLibrary = colorDepthLibraryDao.findById(currentLibraryVariant.getParentLibraryRef().getTargetId());
            if (parentLibrary == null) {
                logger.error("Invalid parent library reference in {} -> {}", currentLibraryVariant, currentLibraryVariant.getParentLibraryRef());
                throw new IllegalArgumentException("Invalid parent library reference " + currentLibraryVariant.getParentLibraryRef() + " in " + currentLibraryVariant);
            }
            sourceLibrary = parentLibrary;
            currentLibraryVariant = parentLibrary;
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
                    logger.info("Processing release library {} for {}", libraryIdentifier, site);
                    ColorDepthLibrary library;
                    boolean libraryCreated;
                    if (indexedLibraries.get(libraryIdentifier) == null) {
                        library = createLibraryForPublishedRelease(libraryIdentifier);
                        library.addReaders(releases.stream().flatMap(r -> r.getReaders().stream()).collect(Collectors.toSet()));
                        libraryCreated = true;
                    } else {
                        library = indexedLibraries.get(libraryIdentifier);
                        libraryCreated = false;
                    }
                    // dereference the library first from all mips
                    colorDepthImageDao.removeAllMipsFromLibrary(libraryIdentifier);
                    AtomicInteger counter = new AtomicInteger();
                    long updatedMips = releases.stream()
                            .flatMap(lr -> lr.getChildren().stream())
                            .collect(Collectors.groupingBy(
                                    ref -> counter.getAndIncrement() / 10000,
                                    Collectors.collectingAndThen(
                                            Collectors.toSet(),
                                            sampleRefs -> addLibraryToMipsBySampleRefs(libraryIdentifier, sampleRefs))))
                            .values().stream()
                            .reduce(0L, Long::sum)
                            ;
                    if (updatedMips > 0 || !libraryCreated && updatedMips == 0) {
                        if (updatedMips > 0) {
                            logger.info("Updated {} mips from library {} for {}", updatedMips, libraryIdentifier, site);
                            library.setColorDepthCounts(
                                    colorDepthImageDao.countColorDepthMIPsByAlignmentSpaceForLibrary(libraryIdentifier)
                            );
                        }
                        try {
                            colorDepthLibraryDao.saveBySubjectKey(library, library.getOwnerKey());
                        } catch (Exception e) {
                            logger.error("Could not update library file counts for: {}", libraryIdentifier, e);
                        }
                    } else {
                        logger.info("Nothing was updated for library {}", libraryIdentifier);
                    }
                });
    }

    private long addLibraryToMipsBySampleRefs(String libraryIdentifier, Set<Reference> sampleRefs) {
        Map<String, Set<Reference>> publishedSamplesGroupedByObjective = annotationDao.findAnnotationsByTargets(sampleRefs).stream()
                .map(a -> ImmutablePair.of(getPublishedObjective(a.getName()), a.getTarget()))
                .filter(sampleWithObjective -> StringUtils.isNotBlank(sampleWithObjective.getLeft()))
                .collect(Collectors.groupingBy(
                        ImmutablePair::getLeft,
                        Collectors.collectingAndThen(
                                Collectors.toSet(),
                                r -> r.stream().map(ImmutablePair::getRight).collect(Collectors.toSet()))))
                ;

        return publishedSamplesGroupedByObjective.entrySet().stream()
                .map(e -> colorDepthImageDao.addLibraryBySampleRefs(libraryIdentifier, e.getKey(), e.getValue(), false))
                .reduce(0L, Long::sum);
    }

    private String getPublishedObjective(String annotationName) {
        Pattern publishedObjectiveRegExPattern = Pattern.compile("Publish(\\d+x)ToWeb", Pattern.CASE_INSENSITIVE);
        Matcher m = publishedObjectiveRegExPattern.matcher(annotationName);
        if (m.find()) {
            return m.group(1);
        } else {
            return null;
        }
    }

    private ColorDepthLibrary createLibraryForPublishedRelease(String libraryIdentifier) {
        logger.info("Create library {}", libraryIdentifier);
        ColorDepthLibrary library = new ColorDepthLibrary();
        library.setIdentifier(libraryIdentifier);
        library.setName(libraryIdentifier);
        library.setOwnerKey(defaultOwnerKey);
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
        return lineReleases.stream()
                .filter(lr -> StringUtils.isNotBlank(lr.getTargetWebsite()))
                .collect(Collectors.groupingBy(LineRelease::getTargetWebsite, Collectors.toList()));
    }

    private void processEmMetadata(ColorDepthLibrary library, String alignmentSpace, ColorDepthLibraryEmMetadata emMetadata) {

        EMDataSet dataSet = emDataSetDao.getDataSetByNameAndVersion(emMetadata.getName(), emMetadata.getVersion());
        if (dataSet==null) {
            logger.warn("Could not find data set {} specified by {}", emMetadata.getDataSetIdentifier(), emMetadata.getFile());
            return;
        }

        // Look up table from EM bodyId to internal id
        Map<Long, EMBody> emBodyByBodyId = new HashMap<>();
        for (EMBody emBody : emBodyDao.getBodiesForDataSet(dataSet)) {
            emBodyByBodyId.put(emBody.getBodyId(), emBody);
        }

        // Update the library with a data set reference
        colorDepthLibraryDao.update(library.getId(), ImmutableMap.of(
                "emDataSetRef", new SetFieldValueHandler<>(Reference.createFor(dataSet))
        ));

        ColorDepthImageQuery mipsQuery = new ColorDepthImageQuery()
                .withLibraryIdentifiers(Collections.singleton(library.getIdentifier()))
                .withAlignmentSpace(alignmentSpace);

        // Update all color depth MIPs with their respective body references
        colorDepthImageDao.streamColorDepthMIPs(mipsQuery)
                .forEach(cdm -> {

                    File file = new File(cdm.getFilepath());
                    Pattern pattern = emSkeletonRegexPattern();
                    Matcher matcher = pattern.matcher(file.getName());
                    if (matcher.matches()) {
                        Long bodyId = new Long(matcher.group(1));
                        EMBody emBody = emBodyByBodyId.get(bodyId);

                        if (emBody!=null) {
                            // Update reference from CDM to EMBody
                            colorDepthImageDao.update(cdm.getId(), ImmutableMap.of(
                                    "emBodyRef", new SetFieldValueHandler<>(Reference.createFor(EMBody.class, emBody.getId())),
                                    "bodyId", new SetFieldValueHandler<>(emBody.getBodyId()),
                                    "neuronType", new SetFieldValueHandler<>(emBody.getNeuronType()),
                                    "neuronInstance", new SetFieldValueHandler<>(emBody.getNeuronInstance())
                            ));

                            if (library.getParentLibraryRef() == null) {
                                // Update CDM on EMBody for easy visualization in the Workstation
                                emBody.getFiles().put(FileType.ColorDepthMip1, cdm.getFilepath());
                                emBodyDao.replace(emBody);
                            }
                        }
                        else {
                            logger.warn("  Could not find body with id {} in {}", bodyId, emMetadata.getDataSetIdentifier());
                        }
                    }
                    else {
                        logger.warn("  Could not parse EM filename: {}", file);
                    }

                });
    }

    /**
     * Copied from https://github.com/JaneliaSciComp/colormipsearch/blob/d8c1ccda49c3e7ed1af82926f0693e69f63aeae1/colormipsearch-tools/src/main/java/org/janelia/colormipsearch/cmd/MIPsHandlingUtils.java#L52
     * @return pattern for matching body ids from file names
     */
    private static Pattern emSkeletonRegexPattern() {
        return Pattern.compile("^(\\d+).*");
    }
}

package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.access.domain.dao.ColorDepthImageQuery;
import org.janelia.model.access.domain.dao.ColorDepthLibraryDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.EmBodyDao;
import org.janelia.model.access.domain.dao.EmDataSetDao;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.access.domain.dao.SetFieldValueHandler;
import org.janelia.model.access.domain.dao.SubjectDao;
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

    private static final String MISSING_FILES_KEY = "$$false$$";

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
        @Parameter(names = "-processingPartitionSize", description = "Processing partition size")
        Integer processingPartitionSize=25;

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

        // Get currently existing libraries
        Map<String, ColorDepthLibrary> existingLibraries = colorDepthLibraryDao.findAll(0, -1).stream()
                .collect(Collectors.toMap(ColorDepthLibrary::getIdentifier, Function.identity()));

        SyncArgs args = getArgs(jacsServiceData);

        return syncPublishedLibraries(args, existingLibraries)
                .thenCompose(ignored -> syncSystemLibraries(args, existingLibraries))
                .thenApply(r -> updateServiceResult(jacsServiceData, r));
    }

    private ServiceComputation<Void> syncPublishedLibraries(SyncArgs args, Map<String, ColorDepthLibrary> existingLibraries) {
        if (args.includePublishedDiscovery) {
            return computationFactory.<Void>newComputation().supply(() -> {
                runPublishedLinesBasedDiscovery(args, existingLibraries);
                return null;
            });
        } else {
            return computationFactory.newCompletedComputation(null);
        }
    }

    private ServiceComputation<Void> syncSystemLibraries(SyncArgs args, Map<String, ColorDepthLibrary> existingLibraries) {
        if (args.skipFileSystemDiscovery) {
            return computationFactory.newCompletedComputation(null);
        } else {
            logger.info("Running discovery with parameters:");
            logger.info("  alignmentSpace={}", args.alignmentSpace);
            logger.info("  library={}", args.library);
            List<ServiceComputation<?>> systemLibrariesComputations = partitionSystemLibraryDirs(rootPath.toFile(), args.alignmentSpace, args.library, args.processingPartitionSize).stream()
                    .map(libraryDirs -> computationFactory.<Void>newComputation().supply(() -> {
                        libraryDirs.forEach(libraryDir -> {
                            // Read optional metadata
                            ColorDepthLibraryEmMetadata emMetadata = null;
                            try {
                                emMetadata = ColorDepthLibraryEmMetadata.fromLibraryPath(libraryDir);
                            } catch (Exception e) {
                                logger.error("Error reading EM metadata for "+libraryDir, e);
                            }
                            processLibraryDir(
                                    libraryDir,
                                    libraryDir.getParentFile().getName(), // alignmentSpace
                                    Collections.emptyMap(),
                                    null, // source library
                                    existingLibraries,
                                    emMetadata);
                        });
                        return null;
                    }))
                    .collect(Collectors.toList());
            return computationFactory.newCompletedComputation(null)
                    .thenCombineAll(systemLibrariesComputations, (r, systemLibrariesResults) -> {
                        // It's necessary to recalculate all the counts here, because some color depth images may be part of constructed
                        // libraries which are not represented explicitly on disk.
                        try {
                            colorDepthLibraryDao.updateColorDepthCounts(colorDepthImageDao.countColorDepthMIPsByAlignmentSpaceForAllLibraries());
                        } catch (Exception e) {
                            logger.error("Failed to update color depth counts", e);
                        }
                        return null;
                    });
        }
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

//    private void runFileSystemBasedDiscovery(SyncArgs args, Map<String, ColorDepthLibrary> indexedLibraries) {
//        logger.info("Running discovery with parameters:");
//        logger.info("  alignmentSpace={}", args.alignmentSpace);
//        logger.info("  library={}", args.library);
//
//        // Walk the relevant alignment directories
//        listChildDirs(rootPath.toFile()).stream()
//                .filter(alignmentDir -> StringUtils.isBlank(args.alignmentSpace) || alignmentDir.getName().equals(args.alignmentSpace))
//                .flatMap(alignmentDir -> listChildDirs(alignmentDir).stream())
//                .filter(libraryDir -> StringUtils.isBlank(args.library) || libraryDir.getName().equals(args.library))
//                .forEach(libraryDir -> {
//
//                    // Read optional metadata
//                    ColorDepthLibraryEmMetadata emMetadata = null;
//                    try {
//                        emMetadata = ColorDepthLibraryEmMetadata.fromLibraryPath(libraryDir);
//                    } catch (Exception e) {
//                        logger.error("Error reading EM metadata for "+libraryDir, e);
//                    }
//
//                    processLibraryDir(libraryDir, libraryDir.getParentFile().getName(), Collections.emptyMap(), null, indexedLibraries, emMetadata);
//                });
//
//        // It's necessary to recalculate all the counts here, because some color depth images may be part of constructed
//        // libraries which are not represented explicitly on disk.
//        try {
//            colorDepthLibraryDao.updateColorDepthCounts(colorDepthImageDao.countColorDepthMIPsByAlignmentSpaceForAllLibraries());
//        } catch (Exception e) {
//            logger.error("Failed to update color depth counts", e);
//        }
//
//        logger.info("Completed color depth library synchronization. Imported {} images in total - deleted {}.", totalCreated, totalDeleted);
//    }

    private Collection<List<File>> partitionSystemLibraryDirs(File rootDir, String alignmentSpace, String selectedLibrary, int partitionSizeArg) {
        final AtomicInteger index = new AtomicInteger();
        int partitionSize = partitionSizeArg > 0 ? partitionSizeArg : 1;
        return listChildDirs(rootDir).stream()
                .filter(alignmentDir -> StringUtils.isBlank(alignmentSpace) || alignmentDir.getName().equals(alignmentSpace))
                .flatMap(alignmentDir -> listChildDirs(alignmentDir).stream())
                .filter(libraryDir -> StringUtils.isBlank(selectedLibrary) || libraryDir.getName().equals(selectedLibrary))
                .collect(Collectors.groupingBy(libraryDir -> index.getAndIncrement() / partitionSize))
                .values()
                ;
    }

    private void processLibraryDir(File libraryDir, String alignmentSpace, Map<String, Reference> sourceLibraryMIPs, ColorDepthLibrary parentLibrary, Map<String, ColorDepthLibrary> indexedLibraries, ColorDepthLibraryEmMetadata emMetadata) {
        logger.info("Discovering files in {}", libraryDir);

        ColorDepthLibrary library = findOrCreateLibraryByIndentifier(libraryDir, parentLibrary, indexedLibraries);

        Pair<Map<String, Reference>, List<File>> processLibResults = processLibraryFiles(libraryDir, alignmentSpace, parentLibrary, sourceLibraryMIPs, library);
        logger.info("  Verified {} existing images, created {} images", existing, created);

        if (emMetadata != null) {
            processEmMetadata(library, alignmentSpace, emMetadata);
            logger.info("  Associated library with EM data set {}", emMetadata.getDataSetIdentifier());
        }

        int total = existing + created - deleted;
        totalCreated += created;
        totalDeleted += deleted;

        library.getColorDepthCounts().put(alignmentSpace, total);
        try {
            colorDepthLibraryDao.saveBySubjectKey(library, library.getOwnerKey());
            logger.debug("  Saved color depth library {} with count {}", library.getIdentifier(), total);
        } catch (Exception e) {
            logger.error("Could not update library file counts for: {}", library.getIdentifier(), e);
        }

        // Indirect recursion - walk subdirs of the libraryDir
        Map<String, Reference> sourceMIPs;
        if (parentLibrary == null) {
            // this is a root library so pass this library's mips to be referenced by the variants
            sourceMIPs = processLibResults.getLeft();
        } else {
            // pass in the source library MIPs to be reference by the variants
            sourceMIPs = sourceLibraryMIPs;
        }
        processLibResults.getRight().stream()
                .forEach(libraryVariantDir -> processLibraryDir(
                        libraryVariantDir,
                        alignmentSpace,
                        sourceMIPs,
                        library,
                        indexedLibraries,
                        emMetadata));
    }

    private synchronized ColorDepthLibrary findOrCreateLibraryByIndentifier(File libraryDir, ColorDepthLibrary parentLibrary, Map<String, ColorDepthLibrary> indexedLibraries) {
        String libraryIdentifier = parentLibrary == null ? libraryDir.getName() : parentLibrary.getIdentifier() + '_' + libraryDir.getName();
        String libraryVariant = parentLibrary == null ? null : libraryDir.getName();

        ColorDepthLibrary library;
        if (indexedLibraries.get(libraryIdentifier) == null) {
            library = createNewLibrary(libraryIdentifier, libraryVariant, parentLibrary);
            indexedLibraries.put(libraryIdentifier, colorDepthLibraryDao.saveBySubjectKey(library, library.getOwnerKey()));
        } else {
            library = indexedLibraries.get(libraryIdentifier);
        }

        return library;
    }

    private List<File> listChildDirs(File dir) {
        if (dir == null) {
            return Collections.emptyList();
        }
        Path dirPath = dir.toPath();
        if (!Files.isDirectory(dirPath)) {
            return Collections.emptyList();
        }
        logger.info("Discovering sub-directories in {}", dir);
        return FileUtils.lookupFiles(dirPath, 1, "glob:**/*")
                .filter(Files::isDirectory)
                .map(Path::toFile)
                .filter(p -> !p.equals(dir))
                .collect(Collectors.toList());
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

    /**
     *
     * @param libraryDir
     * @param alignmentSpace
     * @param parentLibrary
     * @param sourceLibraryMIPs
     * @param library
     * @return all mips created from libraryDir as well as the possible variant sub-directories. If the mips are created for a non-variant libraries
     * they will be passed in to subsequent variant sub-libraries in order to avoid database querying for the source MIP which slows down the system
     * too much.
     */
    private Pair<Map<String, Reference>, List<File>> processLibraryFiles(File libraryDir, String alignmentSpace, ColorDepthLibrary parentLibrary, Map<String, Reference> sourceLibraryMIPs, ColorDepthLibrary library) {
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
                            if (cdf.getFile().exists()) {
                                if (cdf.getSampleRef() == null) {
                                    return cdf.getFile().getAbsolutePath();
                                } else {
                                    return cdf.getSampleRef().getTargetId().toString();
                                }
                            } else {
                                return MISSING_FILES_KEY;
                            }
                        },
                        Collectors.toSet())
                );

        if (existingColorDepthFiles.get(MISSING_FILES_KEY) != null) {
            // remove mips that no longer have an existing file
            existingColorDepthFiles.get(MISSING_FILES_KEY).forEach(cdc -> {
                if (deleteColorDepthImage(cdc)) {
                    logger.info("Deleted color depth image for {} because file does not exist", cdc.getFile());
                    deleted++;
                }
            });
        }

        if (!existingColorDepthFiles.isEmpty()) {
            logger.info("  Found mips for {} samples in {}/{}", existingColorDepthFiles.size(),
                        alignmentSpace, library.getIdentifier());
        }

        List<File> librarySubdirs = new ArrayList<>();

        // Walk all images within any structure
        FileUtils.lookupFiles(
                libraryDir.toPath(), 1, "glob:**/*")
                .map(Path::toFile)
                .filter(f -> !f.equals(libraryDir))
                .peek(f -> {
                    if (f.isDirectory()) {
                        librarySubdirs.add(f);
                    }
                })
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
                    if (createColorDepthImage(cdf, alignmentSpace, parentLibrary, sourceLibraryMIPs, library)) {
                        created++;
                    }
                });

        Map<String, Reference> libraryMIPs = new LinkedHashMap<>();
        // this post phase is for the case when files are already in the library and cleanup is needed.
        colorDepthImageDao.streamColorDepthMIPs(mipsQuery)
                .map(mip -> {
                    ColorDepthFileComponents cdf = parseColorDepthFileComponents(mip.getFilepath());
                    if (cdf.getSampleRef() == null) {
                        libraryMIPs.put(
                                Pattern.compile("(-\\d+)?_CDM$", Pattern.CASE_INSENSITIVE)
                                        .matcher(cdf.getFileName())
                                        .replaceFirst(StringUtils.EMPTY),
                                Reference.createFor(mip));
                    } else {
                        libraryMIPs.put(
                                ColorDepthFileComponents.createCDMNameFromNameComponents(
                                        cdf.getSampleName(),
                                        cdf.getObjective(),
                                        cdf.getAnatomicalArea(),
                                        cdf.getAlignmentSpace(),
                                        cdf.getSampleRef(),
                                        cdf.getChannelNumber(),
                                        null),
                                Reference.createFor(mip));
                    }
                    return cdf;
                })
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
                                            logger.info("Deleted coplor depth image for {}", cdc.getFile());
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

        return ImmutablePair.of(libraryMIPs, librarySubdirs);
    }

    /**
     * Create a ColorDepthImage image for the given file on disk.
     * @param colorDepthImageFileComponents color depth file components
     * @return true if the image was created successfully
     */
    private boolean createColorDepthImage(ColorDepthFileComponents colorDepthImageFileComponents,
                                          String alignmentSpace,
                                          ColorDepthLibrary parentLibrary,
                                          Map<String, Reference> sourceLibraryMIPs,
                                          ColorDepthLibrary library) {
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
            if (parentLibrary != null) {
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
                    // if the mip name does not follow the convention assume the variant is in the file name
                    // remove the variant from the filename
                    final Pattern cdmSuffixMatcher = Pattern.compile("-\\d+_CDM$", Pattern.CASE_INSENSITIVE);
                    final Pattern variantSuffixMatcher = Pattern.compile("[_-](\\d*)" + library.getVariant() + "$", Pattern.CASE_INSENSITIVE);
                    String n1 = cdmSuffixMatcher.matcher(colorDepthImageFileComponents.getFileName()).replaceFirst(StringUtils.EMPTY);
                    String n2 = variantSuffixMatcher.matcher(n1).replaceAll(StringUtils.EMPTY);
                    sourceCDMNameCandidates = ImmutableSet.of(n1, n2, removeLastNameComp(n1), removeLastNameComp(n2));
                }
                logger.debug("Lookup {}", sourceCDMNameCandidates);
                Reference sourceImageReference = sourceCDMNameCandidates.stream()
                        .map(n -> sourceLibraryMIPs.get(n))
                        .filter(ref -> ref != null)
                        .findFirst().orElse(null);
                if (sourceImageReference != null) {
                    image.setSourceImageRef(sourceImageReference);
                } else {
                    // this is the case when the file exist but the mip entity was deleted because
                    // it actually corresponds to a renamed mip
                    logger.warn("No referenced color depth image entity found for {} from library {}, alignment {}, so no MIP will be created",
                            sourceCDMNameCandidates, library.getIdentifier(), alignmentSpace);
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

    private String removeLastNameComp(String name) {
        int lastSepIndex = name.lastIndexOf('_');
        if (lastSepIndex > 0) {
            return name.substring(0, lastSepIndex);
        } else {
            return name;
        }
    }

    private boolean deleteColorDepthImage(ColorDepthFileComponents cdc) {
        return colorDepthImageDao.findColorDepthImageByPath(cdc.getFile().getAbsolutePath())
                .map(colorDepthImage -> {
                    colorDepthImageDao.delete(colorDepthImage);
                    return true;
                })
                .orElse(false);
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
        for (EMBody emBody : emBodyDao.getBodiesForDataSet(dataSet, 0, -1)) {
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
                                    "neuronInstance", new SetFieldValueHandler<>(emBody.getNeuronInstance()),
                                    "neuronStatus", new SetFieldValueHandler<>(emBody.getStatus())
                            ));

                            if (library.getParentLibraryRef() == null) {
                                // Update CDM on EMBody for easy visualization in the Workstation
                                emBody.getFiles().put(FileType.ColorDepthMip1, cdm.getFilepath());
                                emBodyDao.replace(emBody);
                            }
                        } else {
                            logger.warn("  Could not find body with id {} in {} for {}/{}",
                                        bodyId, emMetadata.getDataSetIdentifier(), alignmentSpace, library.getIdentifier());
                        }
                    } else {
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

package org.janelia.jacs2.asyncservice.imagesearch;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.access.domain.dao.EmBodyDao;
import org.janelia.model.access.domain.dao.EmDataSetDao;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.flyem.EMBody;
import org.janelia.model.domain.flyem.EMDataSet;
import org.janelia.model.service.JacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Synchronize the filesystem EMSkeletons directory with the database. Can be restricted to only
 * process a single alignmentSpace or library.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Dependent
@Named("emSkeletonSync")
public class EMSkeletonSynchronizer extends AbstractServiceProcessor<Void> {

    private static final int DEFAULT_PARTITION_SIZE = 30;

    static class SyncArgs extends ServiceArgs {
        @Parameter(names = "-alignmentSpace", description = "Alignment space")
        String alignmentSpace;
        @Parameter(names = "-library", description = "Library identifier. This has to be a root library, not a variant of another library")
        String library;
        @Parameter(names = "-processingPartitionSize", description = "Processing partition size")
        Integer processingPartitionSize=DEFAULT_PARTITION_SIZE;
        @Parameter(names = "-lookupDepth", description = "Skeleton lookup depth")
        Integer skeletonLookupDepth = 1;

        SyncArgs() {
            super("EM skeleton synchronization");
        }
    }

    private final Path rootPath;
    private final EmDataSetDao emDataSetDao;
    private final EmBodyDao emBodyDao;
    private final JacsNotificationDao jacsNotificationDao;
    private int foundObj = 0;
    private int foundSwc = 0;
    private int updatedObj = 0;
    private int updatedSwc = 0;

    @Inject
    EMSkeletonSynchronizer(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           @StrPropertyValue(name = "service.emSkeletons.filepath") String rootPath,
                           EmDataSetDao emDataSetDao,
                           EmBodyDao emBodyDao,
                           JacsNotificationDao jacsNotificationDao,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.rootPath = Paths.get(rootPath);
        this.emDataSetDao = emDataSetDao;
        this.emBodyDao = emBodyDao;
        this.jacsNotificationDao = jacsNotificationDao;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(EMSkeletonSynchronizer.class, new SyncArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        logger.info("Service {} perform EM skeleton sync", jacsServiceData);
        logEMSkelSyncMaintenanceEvent(jacsServiceData.getId());

        // Get currently existing libraries
        Map<String, EMDataSet> existingEMDataSets = emDataSetDao.findAll(0, -1).stream()
                .collect(Collectors.toMap(EMDataSet::getDataSetIdentifier, Function.identity()));

        SyncArgs args = getArgs(jacsServiceData);

        return syncSystemLibraries(args, existingEMDataSets)
                .thenApply(r -> {
                    logger.info("Found {} SWC files and updated {} bodies", foundSwc, updatedSwc);
                    logger.info("Found {} OBJ files and updated {} bodies", foundObj, updatedObj);
                    return r;
                })
                .thenApply(r -> updateServiceResult(jacsServiceData, r))
                ;
    }

    private ServiceComputation<Void> syncSystemLibraries(SyncArgs args, Map<String, EMDataSet> existingLibraries) {
        logger.info("Running discovery with parameters:");
        logger.info("  alignmentSpace={}", args.alignmentSpace);
        logger.info("  library={}", args.library);
        List<ServiceComputation<?>> systemLibrariesComputations = partitionSystemLibraryDirs(rootPath.toFile(), args.alignmentSpace, args.library, args.processingPartitionSize).stream()
                .map(libraryDirs -> computationFactory.<Void>newComputation().supply(() -> {
                    libraryDirs.forEach(libraryDir -> {
                        // Read optional metadata
                        EMDatasetMetadata emMetadata = null;
                        try {
                            emMetadata = EMDatasetMetadata.fromLibraryPath(libraryDir);
                        } catch (Exception e) {
                            logger.error("Error reading EM metadata for "+libraryDir, e);
                        }

                        EMDataSet emDataSet = existingLibraries.get(emMetadata.getDataSetIdentifier());
                        processLibraryDir(
                                libraryDir,
                                libraryDir.getParentFile().getName(), // alignmentSpace
                                args.skeletonLookupDepth,
                                emDataSet);
                    });
                    return null;
                }))
                .collect(Collectors.toList());
        return computationFactory.newCompletedComputation(null)
                .thenCombineAll(systemLibrariesComputations, (r, systemLibrariesResults) -> null);
    }

    private void logEMSkelSyncMaintenanceEvent(Number serviceId) {
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setEventName("EMSkeletonSync");
        jacsNotification.addNotificationData("serviceInstance", serviceId.toString());
        jacsNotification.setNotificationStage(JacsServiceLifecycleStage.PROCESSING);
        jacsNotificationDao.save(jacsNotification);
    }

    private SyncArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SyncArgs());
    }

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

    private void processLibraryDir(File libraryDir, String alignmentSpace, int lookupDepth, EMDataSet emDataSet) {
        logger.info("Discovering files in {}", libraryDir);
        String libraryIdentifier = libraryDir.getName();

        // Look up table from EM bodyId to internal id
        Map<Long, EMBody> emBodyByBodyId = new HashMap<>();
        for (EMBody emBody : emBodyDao.getBodiesForDataSet(emDataSet, 0, -1)) {
            emBodyByBodyId.put(emBody.getBodyId(), emBody);
        }

        Path swcDir = libraryDir.toPath().resolve("swc");
        if (Files.exists(swcDir)) {
            logger.info("Checking for SWC files in {}", swcDir);
            FileUtils.lookupFiles(
                            swcDir, lookupDepth, "glob:**/*.swc")
                    .map(Path::toFile)
                    .filter(File::isFile)
                    .forEach(file -> {
                        foundSwc++;
                        Pattern pattern = emSkeletonRegexPattern();
                        Matcher matcher = pattern.matcher(file.getName());
                        if (matcher.matches()) {
                            Long bodyId = Long.valueOf(matcher.group(1));
                            EMBody emBody = emBodyByBodyId.get(bodyId);

                            if (emBody != null) {
                                // Update CDM on EMBody for easy visualization in the Workstation
                                emBody.getFiles().put(FileType.SkeletonSWC, file.getAbsolutePath());
                                emBodyDao.replace(emBody);
                                updatedSwc++;
                            } else {
                                logger.warn("  Could not find body with id {} in {} for {}/{}",
                                        bodyId, emDataSet.getDataSetIdentifier(), alignmentSpace, libraryIdentifier);
                            }
                        }
                        else {
                            logger.warn("  Could not extract body id from filename: {}", file.getName());
                        }
                    });
        } else {
            logger.info("No swc folder found for {} in {}", libraryIdentifier, libraryDir);
        }


        Path objDir = libraryDir.toPath().resolve("obj");
        if (Files.exists(objDir)) {
            logger.info("Checking for OBJ files in {}", objDir);
            FileUtils.lookupFiles(
                            objDir, lookupDepth, "glob:**/*.obj")
                    .map(Path::toFile)
                    .filter(File::isFile)
                    .forEach(file -> {
                        foundObj++;
                        Pattern pattern = emSkeletonRegexPattern();
                        Matcher matcher = pattern.matcher(file.getName());
                        if (matcher.matches()) {
                            Long bodyId = Long.valueOf(matcher.group(1));
                            EMBody emBody = emBodyByBodyId.get(bodyId);

                            if (emBody != null) {
                                // Update CDM on EMBody for easy visualization in the Workstation
                                emBody.getFiles().put(FileType.SkeletonOBJ, file.getAbsolutePath());
                                emBodyDao.replace(emBody);
                                updatedObj++;
                            } else {
                                logger.warn("  Could not find body with id {} in {} for {}/{}",
                                        bodyId, emDataSet.getDataSetIdentifier(), alignmentSpace, libraryIdentifier);
                            }
                        }
                        else {
                            logger.warn("  Could not extract body id from filename: {}", file.getName());
                        }
                    });
        } else {
            logger.info("No obj folder found for {} in {}", libraryIdentifier, libraryDir);
        }
    }

    /**
     * Copied from https://github.com/JaneliaSciComp/colormipsearch/blob/d8c1ccda49c3e7ed1af82926f0693e69f63aeae1/colormipsearch-tools/src/main/java/org/janelia/colormipsearch/cmd/MIPsHandlingUtils.java#L52
     * @return pattern for matching body ids from file names
     */
    private static Pattern emSkeletonRegexPattern() {
        return Pattern.compile("^(\\d+).*");
    }
}

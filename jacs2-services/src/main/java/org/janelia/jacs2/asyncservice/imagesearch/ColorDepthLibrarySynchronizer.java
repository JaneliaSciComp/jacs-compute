package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
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
import org.janelia.model.domain.gui.cdmip.ColorDepthFilepathParser;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibrary;
import org.janelia.model.domain.sample.DataSet;
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

    public static class SyncArgs extends ServiceArgs {
        @Parameter(names = "-alignmentSpace", description = "Alignment space")
        String alignmentSpace;
        @Parameter(names = "-library", description = "Library identifier")
        String library;
        SyncArgs() {
            super("Color depth library synchronization");
        }
    }

    private final Path rootPath;
    private final LegacyDomainDao dao;
    private final JacsNotificationDao jacsNotificationDao;
    private final String DEFAULT_OWNER = "group:flylight";
    private int existing = 0;
    private int created = 0;
    private int totalCreated = 0;

    @Inject
    ColorDepthLibrarySynchronizer(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           @StrPropertyValue(name = "service.colorDepthSearch.filepath") String rootPath,
                           LegacyDomainDao dao,
                           JacsNotificationDao jacsNotificationDao,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.rootPath = Paths.get(rootPath);
        this.dao = dao;
        this.jacsNotificationDao = jacsNotificationDao;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthLibrarySynchronizer.class, new ColorDepthLibrarySynchronizer.SyncArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {

        logger.info("Service {} perform color depth library sync", jacsServiceData);
        logMaintenanceEvent("ColorDepthLibrarySync", jacsServiceData.getId());

        ColorDepthLibrarySynchronizer.SyncArgs args = getArgs(jacsServiceData);
        runDiscovery(args);

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private void logMaintenanceEvent(String maintenanceEvent, Number serviceId) {
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setEventName(maintenanceEvent);
        jacsNotification.addNotificationData("serviceInstance", serviceId.toString());
        jacsNotification.setNotificationStage(JacsServiceLifecycleStage.PROCESSING);
        jacsNotificationDao.save(jacsNotification);
    }

    private ColorDepthLibrarySynchronizer.SyncArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ColorDepthLibrarySynchronizer.SyncArgs());
    }

    private void walkChildDirs(File dir, Consumer<File> action) {
        if (!dir.isDirectory()) return;
        logger.info("Discovering files in {}", dir);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    action.accept(file);
                }
            }
        }
    }

    private void runDiscovery(ColorDepthLibrarySynchronizer.SyncArgs args) {

        logger.info("Running discovery with parameters:");
        logger.info("  alignmentSpace={}", args.alignmentSpace);
        logger.info("  library={}", args.library);

        // Get all existing libraries
        Map<String,ColorDepthLibrary> libraryMap = new HashMap<>();
        for (ColorDepthLibrary library : dao.getDomainObjects(null, ColorDepthLibrary.class)) {
            libraryMap.put(library.getIdentifier(), library);
        }

        // Walk the relevant alignment directories
        walkChildDirs(rootPath.toFile(), alignmentDir -> {
            if (StringUtils.isNotBlank(args.alignmentSpace) && !alignmentDir.getName().equals(args.alignmentSpace)) return;

            // Walk the relevant library directories
            walkChildDirs(alignmentDir, libraryDir -> {
                if (StringUtils.isNotBlank(args.library) && !libraryDir.getName().equals(args.library)) return;

                logger.info("Discovering files in {}", libraryDir);
                String alignmentSpace = alignmentDir.getName();
                String libraryIdentifier = libraryDir.getName();

                this.existing = 0;
                this.created = 0;

                // This prefetch is an optimization so that we can efficiency check which images already exist
                // in the database without incurring a huge cost for each image.
                Set<String> existingPaths = new HashSet<>(dao.getColorDepthPaths(null, libraryIdentifier, alignmentSpace));
                if (!existingPaths.isEmpty()) {
                    logger.info("  Found {} existing paths", existingPaths.size());
                }
                // Figure out the owner of the library and the images
                ColorDepthLibrary library = libraryMap.get(libraryIdentifier);
                if (library == null) {
                    library = new ColorDepthLibrary();
                    library.setIdentifier(libraryIdentifier);
                    library.setName(libraryIdentifier);

                    DataSet dataSet = dao.getDataSetByIdentifier(null, libraryIdentifier);
                    if (dataSet!=null) {
                        // Copy permissions from corresponding data set
                        library.setOwnerKey(dataSet.getOwnerKey());
                        library.setReaders(dataSet.getReaders());
                        library.setWriters(dataSet.getWriters());
                    }
                    else {
                        String ownerName = libraryIdentifier.split("_")[0];
                        Subject subject = dao.getSubjectByName(ownerName);
                        if (subject != null) {
                            logger.warn("Falling back on owner encoded in library identifier: {}", subject.getKey());
                            library.setOwnerKey(subject.getKey());
                        }
                        else {
                            logger.warn("Falling back on default owner: {}", DEFAULT_OWNER);
                            library.setOwnerKey(DEFAULT_OWNER);
                        }
                    }
                }

                final ColorDepthLibrary finalLibrary = library;

                // Walk all images within any structure
                FileUtils.lookupFiles(
                        libraryDir.toPath(), 4, "glob:**/*")
                        .map(Path::toFile)
                        .filter(file -> {
                            if (file.isDirectory()) return false;
                            String filepath = file.getAbsolutePath();
                            if (!accepted(filepath)) {
                                logger.warn("  File not accepted as color depth MIP: {}", filepath);
                                return false;
                            }
                            if (existingPaths.contains(filepath)) {
                                existing++;
                                return false;
                            }
                            return true;
                        })
                        .forEach(file -> {
                            if (createColorDepthImage(finalLibrary, file)) {
                                created++;
                            }
                        });

                logger.info("  Verified {} existing images, created {} images", existing, created);

                int total = existing + created;
                totalCreated += created;

                if (library.getId()!=null || total > 0) {
                    // If the library exists already, or should be created
                    library.getColorDepthCounts().put(alignmentSpace, total);
                    try {
                        library = dao.save(library.getOwnerKey(), library);
                        libraryMap.put(libraryIdentifier, library);
                        logger.debug("  Saved color depth library {} with count {}", libraryIdentifier, total);
                    }
                    catch (Exception e) {
                        logger.error("Could not update library file counts for: " + libraryIdentifier, e);
                    }
                }
            });

        });

        logger.info("Completed color depth library synchronization. Imported {} images in total.", totalCreated);
    }

    /**
     * Create a ColorDepthImage image for the given file on disk.
     * @param file file within the rootPath filestore
     * @return true if the image was created successfully
     */
    private boolean createColorDepthImage(ColorDepthLibrary library, File file) {
        String filepath = file.getPath();

        try {
            /*
                This assumes that the MIP files reside in a directory structure like this:
                <anything>/ROOT_NAME/<alignmentSpace>/<library>/d1/d2/d3/<file>

                Where "d1/d2/d3" can be any directory hierarchy, or none. This is done so that we can break up
                large directories which are inefficient to access on NFS.
             */
            String rootName = rootPath.getFileName().toString();

            boolean foundRoot = false;
            String libraryIdentifier = null;
            String alignmentSpace = null;

            String[] parts = filepath.split("/");
            for (String part : parts) {
                if (libraryIdentifier!=null) {
                    break;
                }
                else if (alignmentSpace != null) {
                    libraryIdentifier = part;
                }
                else if (foundRoot) {
                    alignmentSpace = part;
                }
                else if (rootName.equals(part)) {
                    foundRoot = true;
                }
            }

            if (!foundRoot) {
                throw new IllegalStateException("Path does not contain root "+rootName+": "+filepath);
            }

            if (alignmentSpace == null) {
                throw new IllegalStateException("Path does not contain alignment space: "+filepath);
            }

            if (libraryIdentifier == null) {
                throw new IllegalStateException("Path does not contain library: "+filepath);
            }

            if (!library.getIdentifier().equals(libraryIdentifier)) {
                throw new IllegalStateException("Library does not match path: ("
                        +library.getIdentifier()+" != "+libraryIdentifier+")");
            }

            ColorDepthFilepathParser parser = null;
            try {
                parser = ColorDepthFilepathParser.parse(filepath);
                if (!alignmentSpace.equals(parser.getAlignmentSpace())) {
                    throw new IllegalStateException("Alignment space does not match path: ("
                            +parser.getAlignmentSpace()+" != "+alignmentSpace+")");
                }
            }
            catch (ParseException e) {
                logger.warn("  Accepting non-standard filename: {}", filepath);
            }

            ColorDepthImage image = new ColorDepthImage();
            image.getLibraries().add(library.getIdentifier());
            image.setName(file.getName());
            image.setFilepath(filepath);
            image.setFileSize(file.length());
            image.setAlignmentSpace(alignmentSpace);
            image.setReaders(library.getReaders());
            image.setWriters(library.getWriters());
            if (parser != null) {
                image.setSampleRef(parser.getSampleRef());
                image.setObjective(parser.getObjective());
                image.setAnatomicalArea(parser.getAnatomicalArea());
                image.setChannelNumber(parser.getChannelNumber());
            }

            dao.save(library.getOwnerKey(), image);
            return true;
        }
        catch (Exception e) {
            logger.warn("  Could not create image for: " + file.getPath());
        }

        return false;
    }

    private boolean accepted(String filepath) {
        return filepath.endsWith(".png") || filepath.endsWith(".tif");
    }
}

package org.janelia.jacs2.asyncservice.lvtservices;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.rendering.DataLocation;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLocation;
import org.janelia.rendering.ymlrepr.RawVolData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

public class HortaDataManager {

    private static final Logger log = LoggerFactory.getLogger(HortaDataManager.class);

    private final TmSampleDao tmSampleDao;
    private final RenderedVolumeLoader renderedVolumeLoader;
    private final DataStorageLocationFactory dataStorageLocationFactory;

    @Inject
    public HortaDataManager(@AsyncIndex TmSampleDao tmSampleDao,
                            RenderedVolumeLoader renderedVolumeLoader,
                            DataStorageLocationFactory dataStorageLocationFactory) {
        this.tmSampleDao = tmSampleDao;
        this.renderedVolumeLoader = renderedVolumeLoader;
        this.dataStorageLocationFactory = dataStorageLocationFactory;
    }

    public TmSample getOwnedTmSampleByName(String ownerKey, String sampleName) {
        List<TmSample> samples = tmSampleDao.findEntitiesByExactName(sampleName)
                .stream()
                .filter(s -> s.getOwnerKey().equals(ownerKey))
                .collect(Collectors.toList());
        if (samples.isEmpty()) {
            return null;
        }
        if (samples.size()>1) {
            log.warn("More than one sample exists with name {}. Choosing the first.", sampleName);
        }
        return samples.get(0);
    }

    public TmSample createTmSample(String subjectKey, TmSample sample) throws UserException {

        String sampleName = sample.getName();
        String samplePath = sample.getLargeVolumeOctreeFilepath();

        RenderedVolumeLocation rvl = dataStorageLocationFactory.lookupJadeDataLocation(samplePath, subjectKey, null)
                .map(dataStorageLocationFactory::asRenderedVolumeLocation)
                .orElse(null);
        if (rvl == null) {
            throw new UserException("Error accessing sample path "+samplePath+" while trying to create sample "+sampleName+" for user "+subjectKey);
        }

        boolean transformFound = getConstants(rvl)
                .map(constants -> {
                    populateConstants(sample, constants);
                    log.info("Found {} levels in octree", sample.getNumImageryLevels());
                    return true;
                })
                .orElse(false);
        if (!transformFound) {
            throw new UserException("Error reading transform constants for "+subjectKey+" from "+samplePath);
        }

        final String ktxFullPath;
        if (StringUtils.isBlank(sample.getLargeVolumeKTXFilepath())) {
            log.info("KTX data path not provided for {}. Attempting to find it relative to the octree...", sample.getName());
            ktxFullPath = StringUtils.appendIfMissing(samplePath, "/") + "ktx";
        } else {
            ktxFullPath = sample.getLargeVolumeKTXFilepath();
        }

        // check if the ktx location is accessible
        boolean ktxFound = dataStorageLocationFactory.lookupJadeDataLocation(ktxFullPath, subjectKey, null)
                .map(dl -> true)
                .orElseGet(() -> {
                    log.warn("Could not find any storage for KTX directory for sample {} at {}", sample.getName(), ktxFullPath);
                    return false;
                })
                ;
        if (ktxFound) {
            if (StringUtils.isBlank(sample.getLargeVolumeKTXFilepath())) {
                log.info("Setting KTX data path to {}", ktxFullPath);
                sample.setLargeVolumeKTXFilepath(ktxFullPath);
            }
        } else {
            if (StringUtils.isNotBlank(sample.getLargeVolumeKTXFilepath())) {
                sample.setFilesystemSync(false); // set file system sync to false because ktx directory does not exist
            }
        }

        final String acquisitionPath;
        if (StringUtils.isBlank(sample.getAcquisitionFilepath())) {
            log.info("RAW data path not provided for {}. Attempting to read it from the tilebase.cache.yml...", sample.getName());
            RawVolData rawVolData = readRawVolumeData(rvl);
            if (rawVolData != null) {
                acquisitionPath = rawVolData.getPath();
            } else {
                acquisitionPath = null;
            }
        } else {
            acquisitionPath = sample.getAcquisitionFilepath();
        }

        if (StringUtils.isNotBlank(acquisitionPath)) {
            boolean acquisitionPathFound = dataStorageLocationFactory.lookupJadeDataLocation(acquisitionPath, subjectKey, null)
                    .map(dl -> true)
                    .orElseGet(() -> {
                        log.warn("Could not find any storage for acquisition path for sample {} at {}", sample.getName(), acquisitionPath);
                        return false;
                    })
                    ;
            if (acquisitionPathFound) {
                if (StringUtils.isBlank(sample.getAcquisitionFilepath())) {
                    log.info("Setting RAW data path to {}", acquisitionPath);
                    sample.setAcquisitionFilepath(acquisitionPath, false);
                }
            } else {
                if (StringUtils.isNotBlank(sample.getAcquisitionFilepath())) {
                    sample.setFilesystemSync(false); // set file system sync to false because the acquisition directory is not accessible
                }
            }
        }

        return tmSampleDao.createTmSample(subjectKey, sample);
    }

    public TmSample updateSample(String subjectKey, TmSample tmSample) {

        String samplePath = tmSample.getLargeVolumeOctreeFilepath();
        log.info("Verifying sample path {} for sample {}", samplePath, tmSample);

        boolean samplePathFound = dataStorageLocationFactory.lookupJadeDataLocation(samplePath, subjectKey, null)
                .map(dataStorageLocationFactory::asRenderedVolumeLocation)
                .flatMap(this::getConstants).isPresent();
        if (!samplePathFound) {
            tmSample.setFilesystemSync(false);
        }

        String ktxFullPath;
        if (StringUtils.isBlank(tmSample.getLargeVolumeKTXFilepath())) {
            ktxFullPath = StringUtils.appendIfMissing(samplePath, "/") + "ktx";
        } else {
            ktxFullPath = tmSample.getLargeVolumeKTXFilepath();
        }
        // check if the ktx location is accessible
        boolean ktxFound = dataStorageLocationFactory.lookupJadeDataLocation(ktxFullPath, subjectKey, null)
                .map(dl -> true)
                .orElseGet(() -> {
                    log.warn("Could not find any storage for KTX directory {} for sample {}", ktxFullPath, tmSample);
                    return false;
                })
                ;
        if (!ktxFound) {
            tmSample.setFilesystemSync(false);
        }

        String acquisitionPath = tmSample.getAcquisitionFilepath();
        if (StringUtils.isNotBlank(acquisitionPath)) {
            // for update only check the acquision path if set - don't try to read the tile yaml file
            boolean acquisitionPathFound = dataStorageLocationFactory.lookupJadeDataLocation(acquisitionPath, subjectKey, null)
                    .map(dl -> true)
                    .orElseGet(() -> {
                        log.warn("Could not find any storage for acquisition path for sample {} at {}", tmSample.getName(), acquisitionPath);
                        return false;
                    })
                    ;
            if (!acquisitionPathFound) {
                tmSample.setFilesystemSync(false);
            }
        }
        return tmSampleDao.updateTmSample(subjectKey, tmSample);
    }

    public Optional<Map<String, Object>> getSampleConstants(String subjectKey, String samplePath) {
        return dataStorageLocationFactory.lookupJadeDataLocation(samplePath, subjectKey, null)
                .flatMap(this::getConstants);
    }

    private RawVolData readRawVolumeData(RenderedVolumeLocation rvl) {
        try {
            return renderedVolumeLoader.loadRawVolumeData(rvl);
        } catch (Exception e) {
            log.error("Error reading raw volume data from {}/{}", rvl.getBaseStorageLocationURI(), RenderedVolumeLoader.DEFAULT_TILED_VOL_BASE_FILE_NAME);
        }
        return null;
    }

    private Optional<Map<String, Object>> getConstants(DataLocation rvl) {
        // read and process transform.txt file in Sample path
        // this is intended to be a one-time process and data returned will be stored in TmSample upon creation
        log.info("Reading {} from {}", RenderedVolumeLoader.DEFAULT_TRANSFORM_FILE_NAME, rvl.getBaseStorageLocationURI());
        return rvl.getContentFromRelativePath(RenderedVolumeLoader.DEFAULT_TRANSFORM_FILE_NAME)
                .consume(transformStream -> {
                    try {
                        Map<String, Object> constants = new HashMap<>();
                        Map<String, Integer> origin = new HashMap<>();
                        Map<String, Double> scaling = new HashMap<>();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(transformStream));
                        String line;
                        Map<String, Double> values = new HashMap<>();
                        while ((line = reader.readLine()) != null) {
                            String[] keyVals = line.split(":");
                            if (keyVals.length == 2) {
                                values.put(keyVals[0], Double.parseDouble(keyVals[1].trim()));
                            }
                        }
                        origin.put("x", values.get("ox").intValue());
                        origin.put("y", values.get("oy").intValue());
                        origin.put("z", values.get("oz").intValue());
                        scaling.put("x", values.get("sx"));
                        scaling.put("y", values.get("sy"));
                        scaling.put("z", values.get("sz"));

                        constants.put("origin", origin);
                        constants.put("scaling", scaling);
                        constants.put("numberLevels", values.get("nl").longValue());
                        return constants;
                    } catch (Exception e) {
                        log.error("Error reading transform constants", e);
                        return null;
                    } finally {
                        IOUtils.closeQuietly(transformStream);
                    }
                }, (constantsMap, l) -> (long) constantsMap.size())
                .asOptional()
                ;
    }

    private void populateConstants(TmSample sample, Map<String, Object> constants) {
        Map originMap = (Map)constants.get("origin");
        List<Integer> origin = new ArrayList<>();
        origin.add ((Integer)originMap.get("x"));
        origin.add ((Integer)originMap.get("y"));
        origin.add ((Integer)originMap.get("z"));
        Map scalingMap = (Map)constants.get("scaling");
        List<Double> scaling = new ArrayList<>();
        scaling.add ((Double)scalingMap.get("x"));
        scaling.add ((Double)scalingMap.get("y"));
        scaling.add ((Double)scalingMap.get("z"));

        sample.setOrigin(origin);
        sample.setScaling(scaling);

        Object numberLevels = constants.get("numberLevels");
        if (numberLevels instanceof Integer) {
            sample.setNumImageryLevels(((Integer)numberLevels).longValue());
        }
        else if (numberLevels instanceof Long) {
            sample.setNumImageryLevels((Long)numberLevels);
        }
        else  if (numberLevels instanceof String) {
            sample.setNumImageryLevels(Long.parseLong((String)numberLevels));
        }
        else {
            throw new IllegalStateException("Could not parse numberLevels: "+numberLevels);
        }
    }

    public static class UserException extends Exception {
        public UserException(String message) {
            super(message);
        }
    }
}

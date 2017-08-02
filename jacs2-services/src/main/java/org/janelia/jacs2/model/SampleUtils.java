package org.janelia.jacs2.model;

import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.DomainObject;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.support.SAGEAttribute;
import org.janelia.jacs2.model.sage.SlideImage;
import org.janelia.jacs2.model.sage.SlideImageGroup;
import org.reflections.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class SampleUtils {
    private static final String NO_CONSENSUS_VALUE = "No Consensus";

    public static LSMImage createLSMFromSlideImage(SlideImage slideImage) {
        LSMImage lsmImage = new LSMImage();
        lsmImage.setSageId(slideImage.getId());
        lsmImage.setDataSet(slideImage.getDataset());
        lsmImage.setLine(slideImage.getLineName());
        lsmImage.setSlideCode(slideImage.getSlideCode());
        lsmImage.setAnatomicalArea(slideImage.getArea());
        lsmImage.setObjectiveName(slideImage.getObjective());
        lsmImage.setTile(slideImage.getTile());
        lsmImage.setCaptureDate(slideImage.getCaptureDate());
        lsmImage.setCreatedBy(slideImage.getCreatedBy());
        lsmImage.setName(slideImage.getName());
        lsmImage.setTmogDate(Objects.requireNonNull(slideImage.getCreateDate()));

        String lsmFileName = StringUtils.defaultIfBlank(slideImage.getJfsPath(), slideImage.getPath());
        if (StringUtils.isNotBlank(lsmFileName)) lsmImage.setFileName(FileType.LosslessStack, lsmFileName);

        lsmImage.setRepresentative(slideImage.getRepresentative());
        lsmImage.setAge(slideImage.getAge());
        lsmImage.setAnnotatedBy(slideImage.getAnnotatedBy());
        lsmImage.setBcCorrection1(slideImage.getBcCorrection1());
        lsmImage.setBcCorrection2(slideImage.getBcCorrection2());
        lsmImage.setBitsPerSample(slideImage.getBitsPerSample());
        lsmImage.setChanSpec(slideImage.getChanSpec());
        lsmImage.setDetectionChannel1DetectorGain(slideImage.getDetectionChannel1DetectorGain());
        lsmImage.setDetectionChannel2DetectorGain(slideImage.getDetectionChannel2DetectorGain());
        lsmImage.setDetectionChannel3DetectorGain(slideImage.getDetectionChannel3DetectorGain());
        lsmImage.setDriver(slideImage.getDriver());
        lsmImage.setFileSize(slideImage.getFileSize());
        lsmImage.setEffector(slideImage.getEffector());
        lsmImage.setCrossBarcode(slideImage.getCrossBarcode());
        lsmImage.setGender(slideImage.getGender());
        lsmImage.setFullAge(slideImage.getFullAge());
        lsmImage.setMountingProtocol(slideImage.getMountingProtocol());
        lsmImage.setHeatShockHour(slideImage.getHeatShockHour());
        lsmImage.setHeatShockInterval(slideImage.getHeatShockInterval());
        lsmImage.setHeatShockMinutes(slideImage.getHeatShockMinutes());
        lsmImage.setIlluminationChannel1Name(slideImage.getIlluminationChannel1Name());
        lsmImage.setIlluminationChannel2Name(slideImage.getIlluminationChannel2Name());
        lsmImage.setIlluminationChannel3Name(slideImage.getIlluminationChannel3Name());
        lsmImage.setIlluminationChannel1PowerBC1(slideImage.getIlluminationChannel1PowerBC1());
        lsmImage.setIlluminationChannel2PowerBC1(slideImage.getIlluminationChannel2PowerBC1());
        lsmImage.setIlluminationChannel3PowerBC1(slideImage.getIlluminationChannel3PowerBC1());
        lsmImage.setImageFamily(slideImage.getImageFamily());
        lsmImage.setImagingProject(slideImage.getImagingProject());
        lsmImage.setInterpolationElapsed(slideImage.getInterpolationElapsed());
        lsmImage.setInterpolationStart(slideImage.getInterpolationStart());
        lsmImage.setInterpolationStop(slideImage.getInterpolationStop());
        lsmImage.setMicroscope(slideImage.getMicroscope());
        lsmImage.setMicroscopeFilename(slideImage.getMicroscopeFilename());
        lsmImage.setMacAddress(slideImage.getMacAddress());
        lsmImage.setSampleZeroTime(slideImage.getSampleZeroTime());
        lsmImage.setSampleZeroZ(slideImage.getSampleZeroZ());
        lsmImage.setScanStart(slideImage.getScanStart());
        lsmImage.setScanStop(slideImage.getScanStop());
        lsmImage.setScanType(slideImage.getScanType());
        lsmImage.setScreenState(slideImage.getScreenState());
        lsmImage.setSlideCode(slideImage.getSlideCode());
        lsmImage.setTissueOrientation(slideImage.getTissueOrientation());
        lsmImage.setTotalPixels(slideImage.getTotalPixels());
        lsmImage.setTracks(slideImage.getTracks());
        lsmImage.setVoxelSizeX(slideImage.getVoxelSizeX());
        lsmImage.setVoxelSizeY(slideImage.getVoxelSizeY());
        lsmImage.setVoxelSizeZ(slideImage.getVoxelSizeZ());
        lsmImage.setDimensionX(slideImage.getDimensionX());
        lsmImage.setDimensionY(slideImage.getDimensionY());
        lsmImage.setDimensionZ(slideImage.getDimensionZ());
        lsmImage.setZoomX(slideImage.getZoomX());
        lsmImage.setZoomY(slideImage.getZoomY());
        lsmImage.setZoomZ(slideImage.getZoomZ());
        lsmImage.setVtLine(slideImage.getVtLine());
        lsmImage.setQiScore(slideImage.getQiScore());
        lsmImage.setQmScore(slideImage.getQmScore());
        lsmImage.setOrganism(slideImage.getOrganism());
        lsmImage.setGenotype(slideImage.getGenotype());
        lsmImage.setFlycoreId(slideImage.getFlycoreId());
        lsmImage.setFlycoreAlias(slideImage.getFlycoreAlias());
        lsmImage.setFlycoreLabId(slideImage.getFlycoreLabId());
        lsmImage.setFlycoreLandingSite(slideImage.getFlycoreLandingSite());
        lsmImage.setFlycorePermission(slideImage.getFlycorePermission());
        lsmImage.setFlycoreProject(slideImage.getFlycoreProject());
        lsmImage.setFlycorePSubcategory(slideImage.getFlycorePSubcategory());
        lsmImage.setLineHide(slideImage.getLineHide());

        lsmImage.setOpticalResolution(slideImage.getOpticalResolution());
        lsmImage.setImageSize(slideImage.getImageSize());
        return lsmImage;
    }

    public static Map<String, EntityFieldValueHandler<?>> updateLsmAttributes(LSMImage src, LSMImage dst) {
        Map<String, EntityFieldValueHandler<?>> updatedFields = new LinkedHashMap<>();
        if (src.getPublishingName() != null) {
            dst.setPublishingName(src.getPublishingName());
            updatedFields.put("publishingName", new SetFieldValueHandler<>(src.getPublishingName()));
        }
        if (src.getPublishedExternally() != null) {
            dst.setPublishedExternally(src.getPublishedExternally());
            updatedFields.put("publishedExternally", new SetFieldValueHandler<>(src.getPublishedExternally()));
        }
        if (src.getRepresentative() != null) {
            dst.setRepresentative(src.getRepresentative());
            updatedFields.put("representative", new SetFieldValueHandler<>(src.getRepresentative()));
        }
        if (src.getAge() != null) {
            dst.setAge(src.getAge());
            updatedFields.put("age", new SetFieldValueHandler<>(src.getAge()));
        }
        if (src.getAnnotatedBy() != null) {
            dst.setAnnotatedBy(src.getAnnotatedBy());
            updatedFields.put("annotatedBy", new SetFieldValueHandler<>(src.getAnnotatedBy()));
        }
        if (src.getBcCorrection1() != null) {
            dst.setBcCorrection1(src.getBcCorrection1());
            updatedFields.put("bcCorrection1", new SetFieldValueHandler<>(src.getBcCorrection1()));
        }
        if (src.getBcCorrection2() != null) {
            dst.setBcCorrection2(src.getBcCorrection2());
            updatedFields.put("bcCorrection2", new SetFieldValueHandler<>(src.getBcCorrection2()));
        }
        if (src.getBitsPerSample() != null) {
            dst.setBitsPerSample(src.getBitsPerSample());
            updatedFields.put("bitsPerSample", new SetFieldValueHandler<>(src.getBitsPerSample()));
        }
        if (src.getChanSpec() != null) {
            dst.setChanSpec(src.getChanSpec());
            updatedFields.put("chanSpec", new SetFieldValueHandler<>(src.getChanSpec()));
        }
        if (src.getDetectionChannel1DetectorGain() != null) {
            dst.setDetectionChannel1DetectorGain(src.getDetectionChannel1DetectorGain());
            updatedFields.put("detectionChannel1DetectorGain", new SetFieldValueHandler<>(src.getDetectionChannel1DetectorGain()));
        }
        if (src.getDetectionChannel2DetectorGain() != null) {
            dst.setDetectionChannel2DetectorGain(src.getDetectionChannel2DetectorGain());
            updatedFields.put("detectionChannel2DetectorGain", new SetFieldValueHandler<>(src.getDetectionChannel2DetectorGain()));
        }
        if (src.getDetectionChannel3DetectorGain() != null) {
            dst.setDetectionChannel3DetectorGain(src.getDetectionChannel3DetectorGain());
            updatedFields.put("detectionChannel3DetectorGain", new SetFieldValueHandler<>(src.getDetectionChannel3DetectorGain()));
        }
        if (src.getDriver() != null) {
            dst.setDriver(src.getDriver());
            updatedFields.put("driver", new SetFieldValueHandler<>(src.getDriver()));
        }
        if (src.getFileSize() != null) {
            dst.setFileSize(src.getFileSize());
            updatedFields.put("fileSize", new SetFieldValueHandler<>(src.getFileSize()));
        }
        if (src.getEffector() != null) {
            dst.setEffector(src.getEffector());
            updatedFields.put("effector", new SetFieldValueHandler<>(src.getEffector()));
        }
        if (src.getCrossBarcode() != null) {
            dst.setCrossBarcode(src.getCrossBarcode());
            updatedFields.put("crossBarcode", new SetFieldValueHandler<>(src.getCrossBarcode()));
        }
        if (src.getGender() != null) {
            dst.setGender(src.getGender());
            updatedFields.put("gender", new SetFieldValueHandler<>(src.getGender()));
        }
        if (src.getFullAge() != null) {
            dst.setFullAge(src.getFullAge());
            updatedFields.put("fullAge", new SetFieldValueHandler<>(src.getFullAge()));
        }
        if (src.getMountingProtocol() != null) {
            dst.setMountingProtocol(src.getMountingProtocol());
            updatedFields.put("mountingProtocol", new SetFieldValueHandler<>(src.getMountingProtocol()));
        }
        if (src.getHeatShockHour() != null) {
            dst.setHeatShockHour(src.getHeatShockHour());
            updatedFields.put("heatShockHour", new SetFieldValueHandler<>(src.getHeatShockHour()));
        }
        if (src.getHeatShockInterval() != null) {
            dst.setHeatShockInterval(src.getHeatShockInterval());
            updatedFields.put("heatShockInterval", new SetFieldValueHandler<>(src.getHeatShockInterval()));
        }
        if (src.getHeatShockMinutes() != null) {
            dst.setHeatShockMinutes(src.getHeatShockMinutes());
            updatedFields.put("heatShockMinutes", new SetFieldValueHandler<>(src.getHeatShockMinutes()));
        }
        if (src.getIlluminationChannel1Name() != null) {
            dst.setIlluminationChannel1Name(src.getIlluminationChannel1Name());
            updatedFields.put("illuminationChannel1Name", new SetFieldValueHandler<>(src.getIlluminationChannel1Name()));
        }
        if (src.getIlluminationChannel2Name() != null) {
            dst.setIlluminationChannel2Name(src.getIlluminationChannel2Name());
            updatedFields.put("illuminationChannel2Name", new SetFieldValueHandler<>(src.getIlluminationChannel2Name()));
        }
        if (src.getIlluminationChannel3Name() != null) {
            dst.setIlluminationChannel3Name(src.getIlluminationChannel3Name());
            updatedFields.put("illuminationChannel3Name", new SetFieldValueHandler<>(src.getIlluminationChannel3Name()));
        }
        if (src.getIlluminationChannel1PowerBC1() != null) {
            dst.setIlluminationChannel1PowerBC1(src.getIlluminationChannel1PowerBC1());
            updatedFields.put("illuminationChannel1PowerBC1", new SetFieldValueHandler<>(src.getIlluminationChannel1PowerBC1()));
        }
        if (src.getIlluminationChannel2PowerBC1() != null) {
            dst.setIlluminationChannel2PowerBC1(src.getIlluminationChannel2PowerBC1());
            updatedFields.put("illuminationChannel2PowerBC1", new SetFieldValueHandler<>(src.getIlluminationChannel2PowerBC1()));
        }
        if (src.getIlluminationChannel3PowerBC1() != null) {
            dst.setIlluminationChannel3PowerBC1(src.getIlluminationChannel3PowerBC1());
            updatedFields.put("illuminationChannel3PowerBC1", new SetFieldValueHandler<>(src.getIlluminationChannel3PowerBC1()));
        }
        if (src.getImageFamily() != null) {
            dst.setImageFamily(src.getImageFamily());
            updatedFields.put("imageFamily", new SetFieldValueHandler<>(src.getImageFamily()));
        }
        if (src.getImagingProject() != null) {
            dst.setImagingProject(src.getImagingProject());
            updatedFields.put("imagingProject", new SetFieldValueHandler<>(src.getImagingProject()));
        }
        if (src.getInterpolationElapsed() != null) {
            dst.setInterpolationElapsed(src.getInterpolationElapsed());
            updatedFields.put("interpolationElapsed", new SetFieldValueHandler<>(src.getInterpolationElapsed()));
        }
        if (src.getInterpolationStart() != null) {
            dst.setInterpolationStart(src.getInterpolationStart());
            updatedFields.put("interpolationStart", new SetFieldValueHandler<>(src.getInterpolationStart()));
        }
        if (src.getInterpolationStop() != null) {
            dst.setInterpolationStop(src.getInterpolationStop());
            updatedFields.put("interpolationStop", new SetFieldValueHandler<>(src.getInterpolationStop()));
        }
        if (src.getMicroscope() != null) {
            dst.setMicroscope(src.getMicroscope());
            updatedFields.put("microscope", new SetFieldValueHandler<>(src.getMicroscope()));
        }
        if (src.getMicroscopeFilename() != null) {
            dst.setMicroscopeFilename(src.getMicroscopeFilename());
            updatedFields.put("microscopeFilename", new SetFieldValueHandler<>(src.getMicroscopeFilename()));
        }
        if (src.getMacAddress() != null) {
            dst.setMacAddress(src.getMacAddress());
            updatedFields.put("macAddress", new SetFieldValueHandler<>(src.getMacAddress()));
        }
        if (src.getSampleZeroTime() != null) {
            dst.setSampleZeroTime(src.getSampleZeroTime());
            updatedFields.put("sampleZeroTime", new SetFieldValueHandler<>(src.getSampleZeroTime()));
        }
        if (src.getSampleZeroZ() != null) {
            dst.setSampleZeroZ(src.getSampleZeroZ());
            updatedFields.put("sampleZeroZ", new SetFieldValueHandler<>(src.getSampleZeroZ()));
        }
        if (src.getScanStart() != null) {
            dst.setScanStart(src.getScanStart());
            updatedFields.put("scanStart", new SetFieldValueHandler<>(src.getScanStart()));
        }
        if (src.getScanStop() != null) {
            dst.setScanStop(src.getScanStop());
            updatedFields.put("scanStop", new SetFieldValueHandler<>(src.getScanStop()));
        }
        if (src.getScanType() != null) {
            dst.setScanType(src.getScanType());
            updatedFields.put("scanType", new SetFieldValueHandler<>(src.getScanType()));
        }
        if (src.getScreenState() != null) {
            dst.setScreenState(src.getScreenState());
            updatedFields.put("screenState", new SetFieldValueHandler<>(src.getScreenState()));
        }
        if (src.getSlideCode() != null) {
            dst.setSlideCode(src.getSlideCode());
            updatedFields.put("slideCode", new SetFieldValueHandler<>(src.getSlideCode()));
        }
        if (src.getTissueOrientation() != null) {
            dst.setTissueOrientation(src.getTissueOrientation());
            updatedFields.put("tissueOrientation", new SetFieldValueHandler<>(src.getTissueOrientation()));
        }
        if (src.getTotalPixels() != null) {
            dst.setTotalPixels(src.getTotalPixels());
            updatedFields.put("totalPixels", new SetFieldValueHandler<>(src.getTotalPixels()));
        }
        if (src.getTracks() != null) {
            dst.setTracks(src.getTracks());
            updatedFields.put("tracks", new SetFieldValueHandler<>(src.getTracks()));
        }
        if (src.getVoxelSizeX() != null) {
            dst.setVoxelSizeX(src.getVoxelSizeX());
            updatedFields.put("voxelSizeX", new SetFieldValueHandler<>(src.getVoxelSizeX()));
        }
        if (src.getVoxelSizeY() != null) {
            dst.setVoxelSizeY(src.getVoxelSizeY());
            updatedFields.put("voxelSizeY", new SetFieldValueHandler<>(src.getVoxelSizeY()));
        }
        if (src.getVoxelSizeZ() != null) {
            dst.setVoxelSizeZ(src.getVoxelSizeZ());
            updatedFields.put("voxelSizeZ", new SetFieldValueHandler<>(src.getVoxelSizeZ()));
        }
        if (src.getDimensionX() != null) {
            dst.setDimensionX(src.getDimensionX());
            updatedFields.put("dimensionX", new SetFieldValueHandler<>(src.getDimensionX()));
        }
        if (src.getDimensionY() != null) {
            dst.setDimensionY(src.getDimensionY());
            updatedFields.put("dimensionY", new SetFieldValueHandler<>(src.getDimensionY()));
        }
        if (src.getDimensionZ() != null) {
            dst.setDimensionZ(src.getDimensionZ());
            updatedFields.put("dimensionZ", new SetFieldValueHandler<>(src.getDimensionZ()));
        }
        if (src.getZoomX() != null) {
            dst.setZoomX(src.getZoomX());
            updatedFields.put("zoomX", new SetFieldValueHandler<>(src.getZoomX()));
        }
        if (src.getZoomY() != null) {
            dst.setZoomY(src.getZoomY());
            updatedFields.put("zoomY", new SetFieldValueHandler<>(src.getZoomY()));
        }
        if (src.getZoomZ() != null) {
            dst.setZoomZ(src.getZoomZ());
            updatedFields.put("zoomZ", new SetFieldValueHandler<>(src.getZoomZ()));
        }
        if (src.getVtLine() != null) {
            dst.setVtLine(src.getVtLine());
            updatedFields.put("vtLine", new SetFieldValueHandler<>(src.getVtLine()));
        }
        if (src.getQiScore() != null) {
            dst.setQiScore(src.getQiScore());
            updatedFields.put("qiScore", new SetFieldValueHandler<>(src.getQiScore()));
        }
        if (src.getQmScore() != null) {
            dst.setQmScore(src.getQmScore());
            updatedFields.put("qmScore", new SetFieldValueHandler<>(src.getQmScore()));
        }
        if (src.getOrganism() != null) {
            dst.setOrganism(src.getOrganism());
            updatedFields.put("organism",new SetFieldValueHandler<>(src.getOrganism()));
        }
        if (src.getGenotype() != null) {
            dst.setGenotype(src.getGenotype());
            updatedFields.put("genotype", new SetFieldValueHandler<>(src.getGenotype()));
        }
        if (src.getFlycoreId() != null) {
            dst.setFlycoreId(src.getFlycoreId());
            updatedFields.put("flycoreId", new SetFieldValueHandler<>(src.getFlycoreId()));
        }
        if (src.getFlycoreAlias() != null) {
            dst.setFlycoreAlias(src.getFlycoreAlias());
            updatedFields.put("flycoreAlias", new SetFieldValueHandler<>(src.getFlycoreAlias()));
        }
        if (src.getFlycoreLabId() != null) {
            dst.setFlycoreLabId(src.getFlycoreLabId());
            updatedFields.put("flycoreLabId", new SetFieldValueHandler<>(src.getFlycoreLabId()));
        }
        if (src.getFlycoreLandingSite() != null) {
            dst.setFlycoreLandingSite(src.getFlycoreLandingSite());
            updatedFields.put("flycoreLandingSite", new SetFieldValueHandler<>(src.getFlycoreLandingSite()));
        }
        if (src.getFlycorePermission() != null) {
            dst.setFlycorePermission(src.getFlycorePermission());
            updatedFields.put("flycorePermission", new SetFieldValueHandler<>(src.getFlycorePermission()));
        }
        if (src.getFlycoreProject() != null) {
            dst.setFlycoreProject(src.getFlycoreProject());
            updatedFields.put("flycoreProject", new SetFieldValueHandler<>(src.getFlycoreProject()));
        }
        if (src.getFlycorePSubcategory() != null) {
            dst.setFlycorePSubcategory(src.getFlycorePSubcategory());
            updatedFields.put("flycorePSubcategory", new SetFieldValueHandler<>(src.getFlycorePSubcategory()));
        }
        if (src.getLineHide() != null) {
            dst.setLineHide(src.getLineHide());
            updatedFields.put("lineHide", new SetFieldValueHandler<>(src.getLineHide()));
        }
        return updatedFields;
    }

    public static Map<String, EntityFieldValueHandler<?>> updateSampleAttributes(Sample sample, Collection<SlideImageGroup> objectiveGroups) {
        Set<String> nonConsesusFieldNames = new HashSet<>();
        Map<String, EntityFieldValueHandler<?>> consensusLsmFieldValues = new LinkedHashMap<>();

        Map<String, SageField> lsmSageAttrs = getSageFields(LSMImage.class, sf -> sf.getKey());
        objectiveGroups.stream()
                .flatMap(slideImageGroup -> slideImageGroup.getImages().stream())
                .forEach((LSMImage lsm) -> {
                    lsmSageAttrs.forEach((fkey, sageField) -> {
                        String fieldName = sageField.field.getName();
                        if (!nonConsesusFieldNames.contains(fieldName)) {
                            // only do this if there's been no conflict for the field so far
                            Object sageFieldValue = null;
                            try {sageField.field.setAccessible(true);
                                sageFieldValue = sageField.field.get(lsm);
                                if (consensusLsmFieldValues.containsKey(fieldName)) {
                                    if ("tmogDate".equals(fieldName)) {
                                        // tmog is treated differently - simply take the max
                                        Date tmogDate = (Date) sageFieldValue;
                                        Date currentTmogDate = (Date) consensusLsmFieldValues.get(fieldName);
                                        if (currentTmogDate == null || tmogDate.after(currentTmogDate)) {
                                            consensusLsmFieldValues.put(fieldName, new SetFieldValueHandler<>(tmogDate));
                                        }
                                    } else {
                                        Object existingConsensusValue = consensusLsmFieldValues.get(fieldName);
                                        if (!Objects.equals(existingConsensusValue, sageFieldValue)) {
                                            nonConsesusFieldNames.add(fieldName);
                                            if (String.class.equals(sageField.field.getType())) {
                                                consensusLsmFieldValues.put(fieldName, new SetFieldValueHandler<>(NO_CONSENSUS_VALUE));
                                            } else {
                                                consensusLsmFieldValues.put(fieldName, new SetFieldValueHandler<>(null));
                                            }
                                        }
                                    }
                                } else {
                                    consensusLsmFieldValues.put(fieldName, new SetFieldValueHandler<>(sageFieldValue));
                                }
                            } catch (IllegalAccessException e) {
                                throw new IllegalArgumentException(e);
                            }
                        }
                    });
                });

        Map<String, EntityFieldValueHandler<?>> sampleFields = new LinkedHashMap<>();
        Map<String, SageField> sampleSageAttrs = getSageFields(Sample.class, sf -> sf.field.getName());
        consensusLsmFieldValues.forEach((fieldName, fieldValue) -> {
            if (sampleSageAttrs.containsKey(fieldName)) {
                try {
                    Field sampleField = sampleSageAttrs.get(fieldName).field;
                    sampleField.setAccessible(true);
                    sampleField.set(sample, fieldValue);
                    sampleFields.put(fieldName, fieldValue);
                } catch (IllegalAccessException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        });
        return sampleFields;
    }

    private static class SageField {
        String cvName;
        String termName;
        Field field;
        String getKey() {
            return cvName+"_"+termName;
        }
    }

    private static <D extends DomainObject> Map<String, SageField> getSageFields(Class<D> dType, Function<SageField, String> keyMapper) {
        Map<String, SageField> sageFields = new HashMap<>();
        for (Field field : ReflectionUtils.getAllFields(dType)) {
            SAGEAttribute sageAttribute = field.getAnnotation(SAGEAttribute.class);
            if (sageAttribute!=null) {
                SageField attr = new SageField();
                attr.cvName = sageAttribute.cvName();
                attr.termName = sageAttribute.termName();
                attr.field = field;
                sageFields.put(keyMapper.apply(attr), attr);
            }
        }
        return sageFields;
    }

}

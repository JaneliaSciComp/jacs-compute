package org.janelia.jacs2.model;

import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.jacs2.model.sage.SlideImage;
import org.janelia.jacs2.model.sage.SlideImageGroup;

import java.util.Collection;

public class SampleUtils {
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
        lsmImage.setTmogDate(slideImage.getCreateDate());

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

    public static void updateLsmAttributes(LSMImage src, LSMImage dst) {
        if (src.getPublishingName() != null) dst.setPublishingName(src.getPublishingName());
        if (src.getPublishedExternally() != null) dst.setPublishedExternally(src.getPublishedExternally());
        if (src.getRepresentative() != null) dst.setRepresentative(src.getRepresentative());
        if (src.getAge() != null) dst.setAge(src.getAge());
        if (src.getAnnotatedBy() != null) dst.setAnnotatedBy(src.getAnnotatedBy());
        if (src.getBcCorrection1() != null) dst.setBcCorrection1(src.getBcCorrection1());
        if (src.getBcCorrection2() != null) dst.setBcCorrection2(src.getBcCorrection2());
        if (src.getBitsPerSample() != null) dst.setBitsPerSample(src.getBitsPerSample());
        if (src.getChanSpec() != null) dst.setChanSpec(src.getChanSpec());
        if (src.getDetectionChannel1DetectorGain() != null) dst.setDetectionChannel1DetectorGain(src.getDetectionChannel1DetectorGain());
        if (src.getDetectionChannel2DetectorGain() != null) dst.setDetectionChannel2DetectorGain(src.getDetectionChannel2DetectorGain());
        if (src.getDetectionChannel3DetectorGain() != null) dst.setDetectionChannel3DetectorGain(src.getDetectionChannel3DetectorGain());
        if (src.getDriver() != null) dst.setDriver(src.getDriver());
        if (src.getFileSize() != null) dst.setFileSize(src.getFileSize());
        if (src.getEffector() != null) dst.setEffector(src.getEffector());
        if (src.getCrossBarcode() != null) dst.setCrossBarcode(src.getCrossBarcode());
        if (src.getGender() != null) dst.setGender(src.getGender());
        if (src.getFullAge() != null) dst.setFullAge(src.getFullAge());
        if (src.getMountingProtocol() != null) dst.setMountingProtocol(src.getMountingProtocol());
        if (src.getHeatShockHour() != null) dst.setHeatShockHour(src.getHeatShockHour());
        if (src.getHeatShockInterval() != null) dst.setHeatShockInterval(src.getHeatShockInterval());
        if (src.getHeatShockMinutes() != null) dst.setHeatShockMinutes(src.getHeatShockMinutes());
        if (src.getIlluminationChannel1Name() != null) dst.setIlluminationChannel1Name(src.getIlluminationChannel1Name());
        if (src.getIlluminationChannel2Name() != null) dst.setIlluminationChannel2Name(src.getIlluminationChannel2Name());
        if (src.getIlluminationChannel3Name() != null) dst.setIlluminationChannel3Name(src.getIlluminationChannel3Name());
        if (src.getIlluminationChannel1PowerBC1() != null) dst.setIlluminationChannel1PowerBC1(src.getIlluminationChannel1PowerBC1());
        if (src.getIlluminationChannel2PowerBC1() != null) dst.setIlluminationChannel2PowerBC1(src.getIlluminationChannel2PowerBC1());
        if (src.getIlluminationChannel3PowerBC1() != null) dst.setIlluminationChannel3PowerBC1(src.getIlluminationChannel3PowerBC1());
        if (src.getImageFamily() != null) dst.setImageFamily(src.getImageFamily());
        if (src.getImagingProject() != null) dst.setImagingProject(src.getImagingProject());
        if (src.getInterpolationElapsed() != null) dst.setInterpolationElapsed(src.getInterpolationElapsed());
        if (src.getInterpolationStart() != null) dst.setInterpolationStart(src.getInterpolationStart());
        if (src.getInterpolationStop() != null) dst.setInterpolationStop(src.getInterpolationStop());
        if (src.getMicroscope() != null) dst.setMicroscope(src.getMicroscope());
        if (src.getMicroscopeFilename() != null) dst.setMicroscopeFilename(src.getMicroscopeFilename());
        if (src.getMacAddress() != null) dst.setMacAddress(src.getMacAddress());
        if (src.getSampleZeroTime() != null) dst.setSampleZeroTime(src.getSampleZeroTime());
        if (src.getSampleZeroZ() != null) dst.setSampleZeroZ(src.getSampleZeroZ());
        if (src.getScanStart() != null) dst.setScanStart(src.getScanStart());
        if (src.getScanStop() != null) dst.setScanStop(src.getScanStop());
        if (src.getScanType() != null) dst.setScanType(src.getScanType());
        if (src.getScreenState() != null) dst.setScreenState(src.getScreenState());
        if (src.getSlideCode() != null) dst.setSlideCode(src.getSlideCode());
        if (src.getTissueOrientation() != null) dst.setTissueOrientation(src.getTissueOrientation());
        if (src.getTotalPixels() != null) dst.setTotalPixels(src.getTotalPixels());
        if (src.getTracks() != null) dst.setTracks(src.getTracks());
        if (src.getVoxelSizeX() != null) dst.setVoxelSizeX(src.getVoxelSizeX());
        if (src.getVoxelSizeY() != null) dst.setVoxelSizeY(src.getVoxelSizeY());
        if (src.getVoxelSizeZ() != null) dst.setVoxelSizeZ(src.getVoxelSizeZ());
        if (src.getDimensionX() != null) dst.setDimensionX(src.getDimensionX());
        if (src.getDimensionY() != null) dst.setDimensionY(src.getDimensionY());
        if (src.getDimensionZ() != null) dst.setDimensionZ(src.getDimensionZ());
        if (src.getZoomX() != null) dst.setZoomX(src.getZoomX());
        if (src.getZoomY() != null) dst.setZoomY(src.getZoomY());
        if (src.getZoomZ() != null) dst.setZoomZ(src.getZoomZ());
        if (src.getVtLine() != null) dst.setVtLine(src.getVtLine());
        if (src.getQiScore() != null) dst.setQiScore(src.getQiScore());
        if (src.getQmScore() != null) dst.setQmScore(src.getQmScore());
        if (src.getOrganism() != null) dst.setOrganism(src.getOrganism());
        if (src.getGenotype() != null) dst.setGenotype(src.getGenotype());
        if (src.getFlycoreId() != null) dst.setFlycoreId(src.getFlycoreId());
        if (src.getFlycoreAlias() != null) dst.setFlycoreAlias(src.getFlycoreAlias());
        if (src.getFlycoreLabId() != null) dst.setFlycoreLabId(src.getFlycoreLabId());
        if (src.getFlycoreLandingSite() != null) dst.setFlycoreLandingSite(src.getFlycoreLandingSite());
        if (src.getFlycorePermission() != null) dst.setFlycorePermission(src.getFlycorePermission());
        if (src.getFlycoreProject() != null) dst.setFlycoreProject(src.getFlycoreProject());
        if (src.getFlycorePSubcategory() != null) dst.setFlycorePSubcategory(src.getFlycorePSubcategory());
        if (src.getLineHide() != null) dst.setLineHide(src.getLineHide());
    }

    public static boolean updateSampleAttributes(Sample sample, Collection<SlideImageGroup> objectiveGroups) {
        // TODO
        return false; // !!!! FIXME
    }
}

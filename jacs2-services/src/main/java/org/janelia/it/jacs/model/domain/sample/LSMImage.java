package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.Reference;
import org.janelia.it.jacs.model.domain.support.SAGEAttribute;

import java.util.Date;

public class LSMImage extends Image {
    private Reference sampleRef;
    private Boolean sageSynced = false;
    private String channelColors;
    private String channelDyeNames;
    private String brightnessCompensation;
    private Date completionDate;

    // SAGE Attributes
    @SAGEAttribute(cvName="image_query", termName="create_date")
    private Date tmogDate;
    @SAGEAttribute(cvName="image_query", termName="id")
    private Integer sageId;
    @SAGEAttribute(cvName="image_query", termName="line")
    private String line;
    @SAGEAttribute(cvName="light_imagery", termName="publishing_name")
    private String publishingName;
    @SAGEAttribute(cvName="light_imagery", termName="published_externally")
    private String publishedExternally;
    @SAGEAttribute(cvName="light_imagery", termName="representative")
    private Boolean representative;
    @SAGEAttribute(cvName="light_imagery", termName="age")
    private String age;
    @SAGEAttribute(cvName="light_imagery", termName="annotated_by")
    private String annotatedBy;
    @SAGEAttribute(cvName="light_imagery", termName="area")
    private String anatomicalArea;
    @SAGEAttribute(cvName="light_imagery", termName="bc_correction1")
    private String bcCorrection1;
    @SAGEAttribute(cvName="light_imagery", termName="bc_correction2")
    private String bcCorrection2;
    @SAGEAttribute(cvName="light_imagery", termName="bits_per_sample")
    private Integer bitsPerSample;
    @SAGEAttribute(cvName="light_imagery", termName="capture_date")
    private Date captureDate;
    @SAGEAttribute(cvName="light_imagery", termName="channel_spec")
    private String chanSpec;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_detection_channel_1_detector_gain")
    private String detectionChannel1DetectorGain;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_detection_channel_2_detector_gain")
    private String detectionChannel2DetectorGain;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_detection_channel_3_detector_gain")
    private String detectionChannel3DetectorGain;
    @SAGEAttribute(cvName="light_imagery", termName="driver")
    private String driver;
    @SAGEAttribute(cvName="light_imagery", termName="file_size")
    private Long fileSize;
    @SAGEAttribute(cvName="fly", termName="effector")
    private String effector;
    @SAGEAttribute(cvName="fly", termName="cross_barcode")
    private Integer crossBarcode;
    @SAGEAttribute(cvName="light_imagery", termName="gender")
    private String gender;
    @SAGEAttribute(cvName="light_imagery", termName="full_age")
    private String fullAge;
    @SAGEAttribute(cvName="light_imagery", termName="mounting_protocol")
    private String mountingProtocol;
    @SAGEAttribute(cvName="light_imagery", termName="heat_shock_hour")
    private String heatShockHour;
    @SAGEAttribute(cvName="light_imagery", termName="heat_shock_interval")
    private String heatShockInterval;
    @SAGEAttribute(cvName="light_imagery", termName="heat_shock_minutes")
    private String heatShockMinutes;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_illumination_channel_1_name")
    private String illuminationChannel1Name;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_illumination_channel_2_name")
    private String illuminationChannel2Name;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_illumination_channel_3_name")
    private String illuminationChannel3Name;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_illumination_channel_1_power_bc_1")
    private String illuminationChannel1PowerBC1;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_illumination_channel_2_power_bc_1")
    private String illuminationChannel2PowerBC1;
    @SAGEAttribute(cvName="light_imagery", termName="lsm_illumination_channel_3_power_bc_1")
    private String illuminationChannel3PowerBC1;
    @SAGEAttribute(cvName="light_imagery", termName="family")
    private String imageFamily;
    @SAGEAttribute(cvName="light_imagery", termName="created_by")
    private String createdBy;
    @SAGEAttribute(cvName="light_imagery", termName="data_set")
    private String dataSet;
    @SAGEAttribute(cvName="light_imagery", termName="imaging_project")
    private String imagingProject;
    @SAGEAttribute(cvName="light_imagery", termName="interpolation_elapsed")
    private String interpolationElapsed;
    @SAGEAttribute(cvName="light_imagery", termName="interpolation_start")
    private Integer interpolationStart;
    @SAGEAttribute(cvName="light_imagery", termName="interpolation_stop")
    private Integer interpolationStop;
    @SAGEAttribute(cvName="light_imagery", termName="microscope")
    private String microscope;
    @SAGEAttribute(cvName="light_imagery", termName="microscope_filename")
    private String microscopeFilename;
    @SAGEAttribute(cvName="light_imagery", termName="mac_address")
    private String macAddress;
    @SAGEAttribute(cvName="light_imagery", termName="objective")
    private String objectiveName;
    @SAGEAttribute(cvName="light_imagery", termName="sample_0time")
    private String sampleZeroTime;
    @SAGEAttribute(cvName="light_imagery", termName="sample_0z")
    private String sampleZeroZ;
    @SAGEAttribute(cvName="light_imagery", termName="scan_start")
    private Integer scanStart;
    @SAGEAttribute(cvName="light_imagery", termName="scan_stop")
    private Integer scanStop;
    @SAGEAttribute(cvName="light_imagery", termName="scan_type")
    private String scanType;
    @SAGEAttribute(cvName="light_imagery", termName="screen_state")
    private String screenState;
    @SAGEAttribute(cvName="light_imagery", termName="slide_code")
    private String slideCode;
    @SAGEAttribute(cvName="light_imagery", termName="tile")
    private String tile;
    @SAGEAttribute(cvName="light_imagery", termName="tissue_orientation")
    private String tissueOrientation;
    @SAGEAttribute(cvName="light_imagery", termName="total_pixels")
    private String totalPixels;
    @SAGEAttribute(cvName="light_imagery", termName="tracks")
    private Integer tracks;
    @SAGEAttribute(cvName="light_imagery", termName="voxel_size_x")
    private String voxelSizeX;
    @SAGEAttribute(cvName="light_imagery", termName="voxel_size_y")
    private String voxelSizeY;
    @SAGEAttribute(cvName="light_imagery", termName="voxel_size_z")
    private String voxelSizeZ;
    @SAGEAttribute(cvName="light_imagery", termName="dimension_x")
    private String dimensionX;
    @SAGEAttribute(cvName="light_imagery", termName="dimension_y")
    private String dimensionY;
    @SAGEAttribute(cvName="light_imagery", termName="dimension_z")
    private String dimensionZ;
    @SAGEAttribute(cvName="light_imagery", termName="zoom_x")
    private String zoomX;
    @SAGEAttribute(cvName="light_imagery", termName="zoom_y")
    private String zoomY;
    @SAGEAttribute(cvName="light_imagery", termName="zoom_z")
    private String zoomZ;
    @SAGEAttribute(cvName="light_imagery", termName="vt_line")
    private String vtLine;
    @SAGEAttribute(cvName="light_imagery", termName="qi")
    private String qiScore;
    @SAGEAttribute(cvName="light_imagery", termName="qm")
    private String qmScore;
    @SAGEAttribute(cvName="line_query", termName="organism")
    private String organism;
    @SAGEAttribute(cvName="line", termName="genotype")
    private String genotype;
    @SAGEAttribute(cvName="line", termName="flycore_id")
    private Integer flycoreId;
    @SAGEAttribute(cvName="line", termName="flycore_alias")
    private String flycoreAlias;
    @SAGEAttribute(cvName="line", termName="flycore_lab")
    private String flycoreLabId;
    @SAGEAttribute(cvName="line", termName="flycore_landing_site")
    private String flycoreLandingSite;
    @SAGEAttribute(cvName="line", termName="flycore_permission")
    private String flycorePermission;
    @SAGEAttribute(cvName="line", termName="flycore_project")
    private String flycoreProject;
    @SAGEAttribute(cvName="line", termName="flycore_project_subcat")
    private String flycorePSubcategory;
    @SAGEAttribute(cvName="line", termName="hide")
    private String lineHide;

    public Reference getSampleRef() {
        return sampleRef;
    }

    public void setSampleRef(Reference sampleRef) {
        this.sampleRef = sampleRef;
    }

    public Boolean getSageSynced() {
        return sageSynced;
    }

    public void setSageSynced(Boolean sageSynced) {
        this.sageSynced = sageSynced;
    }

    public String getChannelColors() {
        return channelColors;
    }

    public void setChannelColors(String channelColors) {
        this.channelColors = channelColors;
    }

    public String getChannelDyeNames() {
        return channelDyeNames;
    }

    public void setChannelDyeNames(String channelDyeNames) {
        this.channelDyeNames = channelDyeNames;
    }

    public String getBrightnessCompensation() {
        return brightnessCompensation;
    }

    public void setBrightnessCompensation(String brightnessCompensation) {
        this.brightnessCompensation = brightnessCompensation;
    }

    public Date getCompletionDate() {
        return completionDate;
    }

    public void setCompletionDate(Date completionDate) {
        this.completionDate = completionDate;
    }

    public Date getTmogDate() {
        return tmogDate;
    }

    public void setTmogDate(Date tmogDate) {
        this.tmogDate = tmogDate;
    }

    public Integer getSageId() {
        return sageId;
    }

    public void setSageId(Integer sageId) {
        this.sageId = sageId;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getPublishingName() {
        return publishingName;
    }

    public void setPublishingName(String publishingName) {
        this.publishingName = publishingName;
    }

    public String getPublishedExternally() {
        return publishedExternally;
    }

    public void setPublishedExternally(String publishedExternally) {
        this.publishedExternally = publishedExternally;
    }

    public Boolean getRepresentative() {
        return representative;
    }

    public void setRepresentative(Boolean representative) {
        this.representative = representative;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getAnnotatedBy() {
        return annotatedBy;
    }

    public void setAnnotatedBy(String annotatedBy) {
        this.annotatedBy = annotatedBy;
    }

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public String getBcCorrection1() {
        return bcCorrection1;
    }

    public void setBcCorrection1(String bcCorrection1) {
        this.bcCorrection1 = bcCorrection1;
    }

    public String getBcCorrection2() {
        return bcCorrection2;
    }

    public void setBcCorrection2(String bcCorrection2) {
        this.bcCorrection2 = bcCorrection2;
    }

    public Integer getBitsPerSample() {
        return bitsPerSample;
    }

    public void setBitsPerSample(Integer bitsPerSample) {
        this.bitsPerSample = bitsPerSample;
    }

    public Date getCaptureDate() {
        return captureDate;
    }

    public void setCaptureDate(Date captureDate) {
        this.captureDate = captureDate;
    }

    public String getChanSpec() {
        return chanSpec;
    }

    public void setChanSpec(String chanSpec) {
        this.chanSpec = chanSpec;
    }

    @JsonIgnore
    public boolean isChanSpecDefined() {
        return StringUtils.isNotBlank(chanSpec);
    }

    public String getDetectionChannel1DetectorGain() {
        return detectionChannel1DetectorGain;
    }

    public void setDetectionChannel1DetectorGain(String detectionChannel1DetectorGain) {
        this.detectionChannel1DetectorGain = detectionChannel1DetectorGain;
    }

    public String getDetectionChannel2DetectorGain() {
        return detectionChannel2DetectorGain;
    }

    public void setDetectionChannel2DetectorGain(String detectionChannel2DetectorGain) {
        this.detectionChannel2DetectorGain = detectionChannel2DetectorGain;
    }

    public String getDetectionChannel3DetectorGain() {
        return detectionChannel3DetectorGain;
    }

    public void setDetectionChannel3DetectorGain(String detectionChannel3DetectorGain) {
        this.detectionChannel3DetectorGain = detectionChannel3DetectorGain;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }

    public String getEffector() {
        return effector;
    }

    public void setEffector(String effector) {
        this.effector = effector;
    }

    public Integer getCrossBarcode() {
        return crossBarcode;
    }

    public void setCrossBarcode(Integer crossBarcode) {
        this.crossBarcode = crossBarcode;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getFullAge() {
        return fullAge;
    }

    public void setFullAge(String fullAge) {
        this.fullAge = fullAge;
    }

    public String getMountingProtocol() {
        return mountingProtocol;
    }

    public void setMountingProtocol(String mountingProtocol) {
        this.mountingProtocol = mountingProtocol;
    }

    public String getHeatShockHour() {
        return heatShockHour;
    }

    public void setHeatShockHour(String heatShockHour) {
        this.heatShockHour = heatShockHour;
    }

    public String getHeatShockInterval() {
        return heatShockInterval;
    }

    public void setHeatShockInterval(String heatShockInterval) {
        this.heatShockInterval = heatShockInterval;
    }

    public String getHeatShockMinutes() {
        return heatShockMinutes;
    }

    public void setHeatShockMinutes(String heatShockMinutes) {
        this.heatShockMinutes = heatShockMinutes;
    }

    public String getIlluminationChannel1Name() {
        return illuminationChannel1Name;
    }

    public void setIlluminationChannel1Name(String illuminationChannel1Name) {
        this.illuminationChannel1Name = illuminationChannel1Name;
    }

    public String getIlluminationChannel2Name() {
        return illuminationChannel2Name;
    }

    public void setIlluminationChannel2Name(String illuminationChannel2Name) {
        this.illuminationChannel2Name = illuminationChannel2Name;
    }

    public String getIlluminationChannel3Name() {
        return illuminationChannel3Name;
    }

    public void setIlluminationChannel3Name(String illuminationChannel3Name) {
        this.illuminationChannel3Name = illuminationChannel3Name;
    }

    public String getIlluminationChannel1PowerBC1() {
        return illuminationChannel1PowerBC1;
    }

    public void setIlluminationChannel1PowerBC1(String illuminationChannel1PowerBC1) {
        this.illuminationChannel1PowerBC1 = illuminationChannel1PowerBC1;
    }

    public String getIlluminationChannel2PowerBC1() {
        return illuminationChannel2PowerBC1;
    }

    public void setIlluminationChannel2PowerBC1(String illuminationChannel2PowerBC1) {
        this.illuminationChannel2PowerBC1 = illuminationChannel2PowerBC1;
    }

    public String getIlluminationChannel3PowerBC1() {
        return illuminationChannel3PowerBC1;
    }

    public void setIlluminationChannel3PowerBC1(String illuminationChannel3PowerBC1) {
        this.illuminationChannel3PowerBC1 = illuminationChannel3PowerBC1;
    }

    public String getImageFamily() {
        return imageFamily;
    }

    public void setImageFamily(String imageFamily) {
        this.imageFamily = imageFamily;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getDataSet() {
        return dataSet;
    }

    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
    }

    public String getImagingProject() {
        return imagingProject;
    }

    public void setImagingProject(String imagingProject) {
        this.imagingProject = imagingProject;
    }

    public String getInterpolationElapsed() {
        return interpolationElapsed;
    }

    public void setInterpolationElapsed(String interpolationElapsed) {
        this.interpolationElapsed = interpolationElapsed;
    }

    public Integer getInterpolationStart() {
        return interpolationStart;
    }

    public void setInterpolationStart(Integer interpolationStart) {
        this.interpolationStart = interpolationStart;
    }

    public Integer getInterpolationStop() {
        return interpolationStop;
    }

    public void setInterpolationStop(Integer interpolationStop) {
        this.interpolationStop = interpolationStop;
    }

    public String getMicroscope() {
        return microscope;
    }

    public void setMicroscope(String microscope) {
        this.microscope = microscope;
    }

    public String getMicroscopeFilename() {
        return microscopeFilename;
    }

    public void setMicroscopeFilename(String microscopeFilename) {
        this.microscopeFilename = microscopeFilename;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }

    public String getObjectiveName() {
        return objectiveName;
    }

    public void setObjectiveName(String objectiveName) {
        this.objectiveName = objectiveName;
    }

    public String getSampleZeroTime() {
        return sampleZeroTime;
    }

    public void setSampleZeroTime(String sampleZeroTime) {
        this.sampleZeroTime = sampleZeroTime;
    }

    public String getSampleZeroZ() {
        return sampleZeroZ;
    }

    public void setSampleZeroZ(String sampleZeroZ) {
        this.sampleZeroZ = sampleZeroZ;
    }

    public Integer getScanStart() {
        return scanStart;
    }

    public void setScanStart(Integer scanStart) {
        this.scanStart = scanStart;
    }

    public Integer getScanStop() {
        return scanStop;
    }

    public void setScanStop(Integer scanStop) {
        this.scanStop = scanStop;
    }

    public String getScanType() {
        return scanType;
    }

    public void setScanType(String scanType) {
        this.scanType = scanType;
    }

    public String getScreenState() {
        return screenState;
    }

    public void setScreenState(String screenState) {
        this.screenState = screenState;
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    public String getTile() {
        return tile;
    }

    public void setTile(String tile) {
        this.tile = tile;
    }

    public String getTissueOrientation() {
        return tissueOrientation;
    }

    public void setTissueOrientation(String tissueOrientation) {
        this.tissueOrientation = tissueOrientation;
    }

    public String getTotalPixels() {
        return totalPixels;
    }

    public void setTotalPixels(String totalPixels) {
        this.totalPixels = totalPixels;
    }

    public Integer getTracks() {
        return tracks;
    }

    public void setTracks(Integer tracks) {
        this.tracks = tracks;
    }

    public String getVoxelSizeX() {
        return voxelSizeX;
    }

    public void setVoxelSizeX(String voxelSizeX) {
        this.voxelSizeX = voxelSizeX;
    }

    public String getVoxelSizeY() {
        return voxelSizeY;
    }

    public void setVoxelSizeY(String voxelSizeY) {
        this.voxelSizeY = voxelSizeY;
    }

    public String getVoxelSizeZ() {
        return voxelSizeZ;
    }

    public void setVoxelSizeZ(String voxelSizeZ) {
        this.voxelSizeZ = voxelSizeZ;
    }

    public String getDimensionX() {
        return dimensionX;
    }

    public void setDimensionX(String dimensionX) {
        this.dimensionX = dimensionX;
    }

    public String getDimensionY() {
        return dimensionY;
    }

    public void setDimensionY(String dimensionY) {
        this.dimensionY = dimensionY;
    }

    public String getDimensionZ() {
        return dimensionZ;
    }

    public void setDimensionZ(String dimensionZ) {
        this.dimensionZ = dimensionZ;
    }

    public String getZoomX() {
        return zoomX;
    }

    public void setZoomX(String zoomX) {
        this.zoomX = zoomX;
    }

    public String getZoomY() {
        return zoomY;
    }

    public void setZoomY(String zoomY) {
        this.zoomY = zoomY;
    }

    public String getZoomZ() {
        return zoomZ;
    }

    public void setZoomZ(String zoomZ) {
        this.zoomZ = zoomZ;
    }

    public String getVtLine() {
        return vtLine;
    }

    public void setVtLine(String vtLine) {
        this.vtLine = vtLine;
    }

    public String getQiScore() {
        return qiScore;
    }

    public void setQiScore(String qiScore) {
        this.qiScore = qiScore;
    }

    public String getQmScore() {
        return qmScore;
    }

    public void setQmScore(String qmScore) {
        this.qmScore = qmScore;
    }

    public String getOrganism() {
        return organism;
    }

    public void setOrganism(String organism) {
        this.organism = organism;
    }

    public String getGenotype() {
        return genotype;
    }

    public void setGenotype(String genotype) {
        this.genotype = genotype;
    }

    public Integer getFlycoreId() {
        return flycoreId;
    }

    public void setFlycoreId(Integer flycoreId) {
        this.flycoreId = flycoreId;
    }

    public String getFlycoreAlias() {
        return flycoreAlias;
    }

    public void setFlycoreAlias(String flycoreAlias) {
        this.flycoreAlias = flycoreAlias;
    }

    public String getFlycoreLabId() {
        return flycoreLabId;
    }

    public void setFlycoreLabId(String flycoreLabId) {
        this.flycoreLabId = flycoreLabId;
    }

    public String getFlycoreLandingSite() {
        return flycoreLandingSite;
    }

    public void setFlycoreLandingSite(String flycoreLandingSite) {
        this.flycoreLandingSite = flycoreLandingSite;
    }

    public String getFlycorePermission() {
        return flycorePermission;
    }

    public void setFlycorePermission(String flycorePermission) {
        this.flycorePermission = flycorePermission;
    }

    public String getFlycoreProject() {
        return flycoreProject;
    }

    public void setFlycoreProject(String flycoreProject) {
        this.flycoreProject = flycoreProject;
    }

    public String getFlycorePSubcategory() {
        return flycorePSubcategory;
    }

    public void setFlycorePSubcategory(String flycorePSubcategory) {
        this.flycorePSubcategory = flycorePSubcategory;
    }

    public String getLineHide() {
        return lineHide;
    }

    public void setLineHide(String lineHide) {
        this.lineHide = lineHide;
    }

}

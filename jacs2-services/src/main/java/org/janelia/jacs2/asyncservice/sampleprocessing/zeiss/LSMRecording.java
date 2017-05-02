package org.janelia.jacs2.asyncservice.sampleprocessing.zeiss;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LSMRecording {
    @JsonProperty("RECORDING_ENTRY_CAMERA_BINNING")
    private String cameraBinning;
    @JsonProperty("RECORDING_ENTRY_CAMERA_FRAME_HEIGHT")
    private Integer cameraFrameHeight;
    @JsonProperty("RECORDING_ENTRY_CAMERA_FRAME_WIDTH")
    private Integer cameraFrameWidth;
    @JsonProperty("RECORDING_ENTRY_CAMERA_OFFSETX")
    private Integer cameraOffsetX;
    @JsonProperty("RECORDING_ENTRY_CAMERA_OFFSETY")
    private Integer cameraOffsetY;
    @JsonProperty("RECORDING_ENTRY_CAMERA_SUPERSAMPLING")
    private Double cameraSuperSampling;
    @JsonProperty("RECORDING_ENTRY_DESCRIPTION")
    private String entryDescription;
    @JsonProperty("RECORDING_ENTRY_IMAGES_HEIGHT")
    private Integer imagesHeight;
    @JsonProperty("RECORDING_ENTRY_IMAGES_WIDTH")
    private Integer imagesWidth;
    @JsonProperty("RECORDING_ENTRY_IMAGES_NUMBER_CHANNELS")
    private Integer imagesNumberChannels;
    @JsonProperty("RECORDING_ENTRY_IMAGES_NUMBER_PLANES")
    private Integer imagesNumberPlanes;
    @JsonProperty("RECORDING_ENTRY_IMAGES_NUMBER_STACKS")
    private Integer imagesNumberStacks;
    @JsonProperty("RECORDING_ENTRY_INTERPOLATIONY")
    private Double interpolation;
    @JsonProperty("RECORDING_ENTRY_LINES_PER_PLANE")
    private Double linesPerPlane;
    @JsonProperty("RECORDING_ENTRY_LINE_SPACING")
    private Double lineSpacing;
    @JsonProperty("RECORDING_ENTRY_LINSCAN_XY_SIZE")
    private Double lineScanXYSize;
    @JsonProperty("RECORDING_ENTRY_NAME")
    private String entryName;
    @JsonProperty("RECORDING_ENTRY_NOTES")
    private String entryNotes;
    @JsonProperty("RECORDING_ENTRY_NUMBER_OF_STACKS")
    private Integer numberOfStacks;
    @JsonProperty("RECORDING_ENTRY_NUTATION")
    private Double entryMutation;
    @JsonProperty("RECORDING_ENTRY_OBJECTIVE")
    private String entryObjective;
    @JsonProperty("RECORDING_ENTRY_ORIGINAL_SCAN_DATA")
    private Double originalScanData;
    @JsonProperty("RECORDING_ENTRY_PLANES_PER_VOLUME")
    private Double planesPerVolume;
    @JsonProperty("RECORDING_ENTRY_PLANE_SPACING")
    private Double planeSpacing;
    @JsonProperty("RECORDING_ENTRY_POSITIONBCCORRECTION1")
    private Double positionBCorrection1;
    @JsonProperty("RECORDING_ENTRY_POSITIONBCCORRECTION2")
    private Double positionBCorrection2;
    @JsonProperty("RECORDING_ENTRY_PRECESSION")
    private Double precession;
    @JsonProperty("RECORDING_ENTRY_PRESCAN")
    private Double prescan;
    @JsonProperty("RECORDING_ENTRY_ROTATION")
    private Double rotation;
    @JsonProperty("RECORDING_ENTRY_RT_BINNING")
    private Double rtBinning;
    @JsonProperty("RECORDING_ENTRY_RT_FRAME_HEIGHT")
    private Double rtFrameHeight;
    @JsonProperty("RECORDING_ENTRY_RT_FRAME_WIDTH")
    private Double rtFrameWidth;
    @JsonProperty("RECORDING_ENTRY_RT_LINEPERIOD")
    private Double rtLinePeriod;
    @JsonProperty("RECORDING_ENTRY_RT_OFFSETX")
    private Double rtOffsetX;
    @JsonProperty("RECORDING_ENTRY_RT_OFFSETY")
    private Double rtOffsetY;
    @JsonProperty("RECORDING_ENTRY_RT_REGION_HEIGHT")
    private Double rtRegionHeight;
    @JsonProperty("RECORDING_ENTRY_RT_REGION_WIDTH")
    private Double rtRegionWidth;
    @JsonProperty("RECORDING_ENTRY_RT_SUPERSAMPLING")
    private Double rtSupersampling;
    @JsonProperty("RECORDING_ENTRY_RT_ZOOM")
    private Double rtZoom;
    @JsonProperty("RECORDING_ENTRY_SAMPLES_PER_LINE")
    private Double samplesPerLine;
    @JsonProperty("RECORDING_ENTRY_SAMPLE_0TIME")
    private Double sample0Time;
    @JsonProperty("RECORDING_ENTRY_SAMPLE_0X")
    private Double sample0X;
    @JsonProperty("RECORDING_ENTRY_SAMPLE_0Y")
    private Double sample0Y;
    @JsonProperty("RECORDING_ENTRY_SAMPLE_0Z")
    private Double sample0Z;
    @JsonProperty("RECORDING_ENTRY_SAMPLE_SPACING")
    private Double sampleSpacing;
    @JsonProperty("RECORDING_ENTRY_SCAN_DIRECTION")
    private Integer scanDirection;
    @JsonProperty("RECORDING_ENTRY_SCAN_DIRECTIONZ")
    private Integer scanDirectionZ;
    @JsonProperty("RECORDING_ENTRY_SCAN_LINE")
    private Integer scanLine;
    @JsonProperty("RECORDING_ENTRY_SCAN_MODE")
    private String scanMode;
    @JsonProperty("RECORDING_ENTRY_SPECIAL_SCAN_MODE")
    private String specialScanMode;
    @JsonProperty("RECORDING_ENTRY_START_SCAN_EVENT")
    private String startScanEvent;
    @JsonProperty("RECORDING_ENTRY_START_SCAN_TIME")
    private Long startScanTime;
    @JsonProperty("RECORDING_ENTRY_START_SCAN_TRIGGER_IN")
    private String startScanTriggerIn;
    @JsonProperty("RECORDING_ENTRY_START_SCAN_TRIGGER_OUT")
    private String startScanTriggerOut;
    @JsonProperty("RECORDING_ENTRY_STOP_SCAN_EVENT")
    private String stopScanEvent;
    @JsonProperty("RECORDING_ENTRY_STOP_SCAN_TIME")
    private Long stopScanTime;
    @JsonProperty("RECORDING_ENTRY_STOP_SCAN_TRIGGER_IN")
    private String stopScanTriggerIn;
    @JsonProperty("RECORDING_ENTRY_STOP_SCAN_TRIGGER_OUT")
    private String stopScanTriggerOut;
    @JsonProperty("RECORDING_ENTRY_TIME_SERIES")
    private String timeSeries;
    @JsonProperty("RECORDING_ENTRY_USEBCCORRECTION")
    private String uberCorrection;
    @JsonProperty("RECORDING_ENTRY_USER")
    private String user;
    @JsonProperty("RECORDING_ENTRY_USE_REDUCED_MEMORY_ROIS")
    private Integer useReducedMemoryROIs;
    @JsonProperty("RECORDING_ENTRY_USE_ROIS")
    private Integer useROIs;
    @JsonProperty("RECORDING_ENTRY_ZOOM_X")
    private Double zoomX;
    @JsonProperty("RECORDING_ENTRY_ZOOM_Y")
    private Double zoomY;
    @JsonProperty("RECORDING_ENTRY_ZOOM_Z")
    private Double zoomZ;

    public String getCameraBinning() {
        return cameraBinning;
    }

    public Integer getCameraFrameHeight() {
        return cameraFrameHeight;
    }

    public Integer getCameraFrameWidth() {
        return cameraFrameWidth;
    }

    public Integer getCameraOffsetX() {
        return cameraOffsetX;
    }

    public Integer getCameraOffsetY() {
        return cameraOffsetY;
    }

    public Double getCameraSuperSampling() {
        return cameraSuperSampling;
    }

    public String getEntryDescription() {
        return entryDescription;
    }

    public Integer getImagesHeight() {
        return imagesHeight;
    }

    public Integer getImagesWidth() {
        return imagesWidth;
    }

    public Integer getImagesNumberChannels() {
        return imagesNumberChannels;
    }

    public Integer getImagesNumberPlanes() {
        return imagesNumberPlanes;
    }

    public Integer getImagesNumberStacks() {
        return imagesNumberStacks;
    }

    public Double getInterpolation() {
        return interpolation;
    }

    public Double getLinesPerPlane() {
        return linesPerPlane;
    }

    public Double getLineSpacing() {
        return lineSpacing;
    }

    public Double getLineScanXYSize() {
        return lineScanXYSize;
    }

    public String getEntryName() {
        return entryName;
    }

    public String getEntryNotes() {
        return entryNotes;
    }

    public Integer getNumberOfStacks() {
        return numberOfStacks;
    }

    public Double getEntryMutation() {
        return entryMutation;
    }

    public String getEntryObjective() {
        return entryObjective;
    }

    public Double getOriginalScanData() {
        return originalScanData;
    }

    public Double getPlanesPerVolume() {
        return planesPerVolume;
    }

    public Double getPlaneSpacing() {
        return planeSpacing;
    }

    public Double getPositionBCorrection1() {
        return positionBCorrection1;
    }

    public Double getPositionBCorrection2() {
        return positionBCorrection2;
    }

    public Double getPrecession() {
        return precession;
    }

    public Double getPrescan() {
        return prescan;
    }

    public Double getRotation() {
        return rotation;
    }

    public Double getRtBinning() {
        return rtBinning;
    }

    public Double getRtFrameHeight() {
        return rtFrameHeight;
    }

    public Double getRtFrameWidth() {
        return rtFrameWidth;
    }

    public Double getRtLinePeriod() {
        return rtLinePeriod;
    }

    public Double getRtOffsetX() {
        return rtOffsetX;
    }

    public Double getRtOffsetY() {
        return rtOffsetY;
    }

    public Double getRtRegionHeight() {
        return rtRegionHeight;
    }

    public Double getRtRegionWidth() {
        return rtRegionWidth;
    }

    public Double getRtSupersampling() {
        return rtSupersampling;
    }

    public Double getRtZoom() {
        return rtZoom;
    }

    public Double getSamplesPerLine() {
        return samplesPerLine;
    }

    public Double getSample0Time() {
        return sample0Time;
    }

    public Double getSample0X() {
        return sample0X;
    }

    public Double getSample0Y() {
        return sample0Y;
    }

    public Double getSample0Z() {
        return sample0Z;
    }

    public Double getSampleSpacing() {
        return sampleSpacing;
    }

    public Integer getScanDirection() {
        return scanDirection;
    }

    public Integer getScanDirectionZ() {
        return scanDirectionZ;
    }

    public Integer getScanLine() {
        return scanLine;
    }

    public String getScanMode() {
        return scanMode;
    }

    public String getSpecialScanMode() {
        return specialScanMode;
    }

    public String getStartScanEvent() {
        return startScanEvent;
    }

    public Long getStartScanTime() {
        return startScanTime;
    }

    public String getStartScanTriggerIn() {
        return startScanTriggerIn;
    }

    public String getStartScanTriggerOut() {
        return startScanTriggerOut;
    }

    public String getStopScanEvent() {
        return stopScanEvent;
    }

    public Long getStopScanTime() {
        return stopScanTime;
    }

    public String getStopScanTriggerIn() {
        return stopScanTriggerIn;
    }

    public String getStopScanTriggerOut() {
        return stopScanTriggerOut;
    }

    public String getTimeSeries() {
        return timeSeries;
    }

    public String getUberCorrection() {
        return uberCorrection;
    }

    public String getUser() {
        return user;
    }

    public Integer getUseReducedMemoryROIs() {
        return useReducedMemoryROIs;
    }

    public Integer getUseROIs() {
        return useROIs;
    }

    public Double getZoomX() {
        return zoomX;
    }

    public Double getZoomY() {
        return zoomY;
    }

    public Double getZoomZ() {
        return zoomZ;
    }
}

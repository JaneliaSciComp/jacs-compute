package org.janelia.jacs2.model.sage;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.model.BaseEntity;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class SlideImage implements BaseEntity {
    private Integer id;
    private String name;
    private String url;
    private String path;
    private String jfsPath;
    private Date captureDate;
    private String createdBy;
    private Date createDate;
    private String lab;
    private String dataset;
    private String lineName;
    private String slideCode;
    private String area;
    private String objective;
    private String tile;
    private Map<String, String> properties = new LinkedHashMap<>();

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getJfsPath() {
        return jfsPath;
    }

    public void setJfsPath(String jfsPath) {
        this.jfsPath = jfsPath;
    }

    public Date getCaptureDate() {
        return captureDate;
    }

    public void setCaptureDate(Date captureDate) {
        this.captureDate = captureDate;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public String getLab() {
        return lab;
    }

    public void setLab(String lab) {
        this.lab = lab;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    public String getArea() {
        return StringUtils.defaultIfBlank(area, null);
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getTile() {
        return tile;
    }

    public void setTile(String tile) {
        this.tile = tile;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addProperty(String name, String value) {
        if (StringUtils.isNotBlank(value)) this.properties.put(name, value);
    }

    public Boolean getPropertyAsBoolean(String propertyName) {
        if (properties.containsKey(propertyName)) {
            return Boolean.valueOf(properties.get(propertyName));
        } else {
            return null;
        }
    }

    public Integer getPropertyAsInteger(String propertyName) {
        if (properties.containsKey(propertyName)) {
            return Integer.valueOf(properties.get(propertyName));
        } else {
            return null;
        }
    }

    public Long getPropertyAsLong(String propertyName) {
        if (properties.containsKey(propertyName)) {
            return Long.valueOf(properties.get(propertyName));
        } else {
            return null;
        }
    }

    public String getPropertyAsString(String propertyName) {
        return properties.get(propertyName);
    }

    public Boolean getRepresentative() {
        return getPropertyAsBoolean("light_imagery_representative");
    }

    public String getAge() {
        return getPropertyAsString("light_imagery_age");
    }

    public String getAnnotatedBy() {
        return getPropertyAsString("light_imagery_annotated_by");
    }

    public String getBcCorrection1() {
        return getPropertyAsString("light_imagery_bc_correction1");
    }

    public String getBcCorrection2() {
        return getPropertyAsString("light_imagery_bc_correction2");
    }

    public Integer getBitsPerSample() {
        return getPropertyAsInteger("light_imagery_bits_per_sample");
    }

    public String getChanSpec() {
        return getPropertyAsString("light_imagery_channel_spec");
    }

    public String getDetectionChannel1DetectorGain() {
        return getPropertyAsString("light_imagery_lsm_detection_channel_1_detector_gain");
    }

    public String getDetectionChannel2DetectorGain() {
        return getPropertyAsString("light_imagery_lsm_detection_channel_2_detector_gain");
    }

    public String getDetectionChannel3DetectorGain() {
        return getPropertyAsString("light_imagery_lsm_detection_channel_3_detector_gain");
    }

    public String getDriver() {
        return getPropertyAsString("light_imagery_driver");
    }

    public Long getFileSize() {
        return getPropertyAsLong("light_imagery_file_size");
    }

    public String getEffector() {
        return getPropertyAsString("fly_effector");
    }

    public Integer getCrossBarcode() {
        return getPropertyAsInteger("fly_cross_barcode");
    }

    public String getGender() {
        String genderValue = getPropertyAsString("light_imagery_gender");
        if (StringUtils.isBlank(genderValue)) return null;
        char gender = genderValue.trim().charAt(0);
        switch (gender) {
            case 'F':
            case 'f':
                return "f";
            case 'M':
            case 'm':
                return "m";
            case 'X':
            case 'x':
                return "x";
            default:
                return null;
        }
    }

    public String getFullAge() {
        return getPropertyAsString("light_imagery_full_age");
    }

    public String getMountingProtocol() {
        return getPropertyAsString("light_imagery_mounting_protocol");
    }

    public String getHeatShockHour() {
        return getPropertyAsString("light_imagery_heat_shock_hour");
    }

    public String getHeatShockInterval() {
        return getPropertyAsString("light_imagery_heat_shock_interval");
    }

    public String getHeatShockMinutes() {
        return getPropertyAsString("light_imagery_heat_shock_minutes");
    }

    public String getIlluminationChannel1Name() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_1_name");
    }
    
    public String getIlluminationChannel2Name() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_2_name");
    }

    public String getIlluminationChannel3Name() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_3_name");
    }

    public String getIlluminationChannel1PowerBC1() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_1_power_bc_1");
    }

    public String getIlluminationChannel2PowerBC1() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_2_power_bc_1");
    }

    public String getIlluminationChannel3PowerBC1() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_3_power_bc_1");
    }

    public String getImageFamily() {
        return getPropertyAsString("light_imagery_family");
    }

    public String getImagingProject() {
        return getPropertyAsString("light_imagery_imaging_project");
    }

    public String getInterpolationElapsed() {
        return getPropertyAsString("light_imagery_interpolation_elapsed");
    }

    public Integer getInterpolationStart() {
        return getPropertyAsInteger("light_imagery_interpolation_start");
    }

    public Integer getInterpolationStop() {
        return getPropertyAsInteger("light_imagery_interpolation_stop");
    }

    public String getMicroscope() {
        return getPropertyAsString("light_imagery_microscope");
    }

    public String getMicroscopeFilename() {
        return getPropertyAsString("light_imagery_microscope_filename");
    }

    public String getMacAddress() {
        return getPropertyAsString("light_imagery_mac_address");
    }

    public String getSampleZeroTime() {
        return getPropertyAsString("light_imagery_sample_0time");
    }

    public String getSampleZeroZ() {
        return getPropertyAsString("light_imagery_sample_0z");
    }

    public Integer getScanStart() {
        return getPropertyAsInteger("light_imagery_scan_start");
    }

    public Integer getScanStop() {
        return getPropertyAsInteger("light_imagery_scan_stop");
    }

    public String getScanType() {
        return getPropertyAsString("light_imagery_scan_type");
    }

    public String getScreenState() {
        return getPropertyAsString("light_imagery_screen_state");
    }

    public String getTissueOrientation() {
        return getPropertyAsString("light_imagery_tissue_orientation");
    }

    public String getTotalPixels() {
        return getPropertyAsString("light_imagery_total_pixels");
    }

    public Integer getTracks() {
        return getPropertyAsInteger("light_imagery_tracks");
    }

    public String getVoxelSizeX() {
        return getPropertyAsString("light_imagery_voxel_size_x");
    }

    public String getVoxelSizeY() {
        return getPropertyAsString("light_imagery_voxel_size_y");
    }

    public String getVoxelSizeZ() {
        return getPropertyAsString("light_imagery_voxel_size_z");
    }

    public String getDimensionX() {
        return getPropertyAsString("light_imagery_dimension_x");
    }

    public String getDimensionY() {
        return getPropertyAsString("light_imagery_dimension_y");
    }

    public String getDimensionZ() {
        return getPropertyAsString("light_imagery_dimension_z");
    }

    public String getZoomX() {
        return getPropertyAsString("light_imagery_zoom_x");
    }

    public String getZoomY() {
        return getPropertyAsString("light_imagery_zoom_y");
    }

    public String getZoomZ() {
        return getPropertyAsString("light_imagery_zoom_z");
    }

    public String getVtLine() {
        return getPropertyAsString("light_imagery_vt_line");
    }

    public String getQiScore() {
        return getPropertyAsString("light_imagery_qi");
    }

    public String getQmScore() {
        return getPropertyAsString("light_imagery_qm");
    }

    public String getOrganism() {
        return getPropertyAsString("line_organism");
    }

    public String getGenotype() {
        return getPropertyAsString("line_genotype");
    }

    public Integer getFlycoreId() {
        return getPropertyAsInteger("line_flycore_id");
    }

    public String getFlycoreAlias() {
        return getPropertyAsString("line_flycore_alias");
    }

    public String getFlycoreLabId() {
        return getPropertyAsString("line_flycore_lab");
    }

    public String getFlycoreLandingSite() {
        return getPropertyAsString("line_flycore_landing_site");
    }

    public String getFlycorePermission() {
        return getPropertyAsString("line_flycore_permission");
    }

    public String getFlycoreProject() {
        return getPropertyAsString("line_flycore_project");
    }

    public String getFlycorePSubcategory() {
        return getPropertyAsString("line_flycore_project_subcat");
    }

    public String getLineHide() {
        return getPropertyAsString("line_hide");
    }

    public String getOpticalResolution() {
        if (getVoxelSizeX() != null && getVoxelSizeY() != null && getVoxelSizeZ() != null) {
            return getVoxelSizeX() + "x" + getVoxelSizeY() + "x" + getVoxelSizeZ();
        } else {
            return null;
        }
    }

    public String getImageSize() {
        if (getDimensionX() != null && getDimensionY() != null && getDimensionZ() != null) {
            return getDimensionX() + "x" + getDimensionY() + "x" + getDimensionZ();
        } else {
            return null;
        }
    }

}

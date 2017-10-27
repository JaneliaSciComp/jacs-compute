package org.janelia.model.jacs2.sage;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.jacs2.domain.support.SAGEAttribute;
import org.janelia.model.jacs2.BaseEntity;

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
    private String objectiveName;
    private String tile;
    private Map<String, String> properties = new LinkedHashMap<>();

    @SAGEAttribute(cvName = "image_query", termName = "id")
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

    @SAGEAttribute(cvName = "light_imagery", termName = "capture_date")
    public Date getCaptureDate() {
        return captureDate;
    }

    public void setCaptureDate(Date captureDate) {
        this.captureDate = captureDate;
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "created_by")
    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @SAGEAttribute(cvName = "image_query", termName = "create_date")
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

    @SAGEAttribute(cvName = "light_imagery", termName = "data_set")
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

    @SAGEAttribute(cvName = "light_imagery", termName = "slide_code")
    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "area")
    public String getArea() {
        return StringUtils.defaultIfBlank(area, "");
    }

    public void setArea(String area) {
        this.area = area;
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "objective")
    public String getObjectiveName() {
        return StringUtils.defaultIfBlank(objectiveName, "");
    }

    public void setObjectiveName(String objectiveName) {
        this.objectiveName = objectiveName;
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "tile")
    public String getTile() {
        return StringUtils.defaultIfBlank(tile, "");
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

    @SAGEAttribute(cvName = "light_imagery", termName = "representative")
    public Boolean getRepresentative() {
        return getPropertyAsBoolean("light_imagery_representative");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "age")
    public String getAge() {
        return getPropertyAsString("light_imagery_age");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "publishing_name")
    public String getPublishingName() {
        return getPropertyAsString("light_imagery_publishing_name");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "published_externally")
    public String getPublishedExternally() {
        return getPropertyAsString("light_imagery_published_externally");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "annotated_by")
    public String getAnnotatedBy() {
        return getPropertyAsString("light_imagery_annotated_by");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "bc_correction1")
    public String getBcCorrection1() {
        return getPropertyAsString("light_imagery_bc_correction1");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "bc_correction2")
    public String getBcCorrection2() {
        return getPropertyAsString("light_imagery_bc_correction2");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "bits_per_sample")
    public Integer getBitsPerSample() {
        return getPropertyAsInteger("light_imagery_bits_per_sample");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "channel_spec")
    public String getChanSpec() {
        return getPropertyAsString("light_imagery_channel_spec");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_detection_channel_1_detector_gain")
    public String getDetectionChannel1DetectorGain() {
        return getPropertyAsString("light_imagery_lsm_detection_channel_1_detector_gain");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_detection_channel_2_detector_gain")
    public String getDetectionChannel2DetectorGain() {
        return getPropertyAsString("light_imagery_lsm_detection_channel_2_detector_gain");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_detection_channel_3_detector_gain")
    public String getDetectionChannel3DetectorGain() {
        return getPropertyAsString("light_imagery_lsm_detection_channel_3_detector_gain");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "driver")
    public String getDriver() {
        return getPropertyAsString("light_imagery_driver");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "file_size")
    public Long getFileSize() {
        return getPropertyAsLong("light_imagery_file_size");
    }

    @SAGEAttribute(cvName = "fly", termName = "effector")
    public String getEffector() {
        return getPropertyAsString("fly_effector");
    }

    @SAGEAttribute(cvName = "fly", termName = "cross_barcode")
    public Integer getCrossBarcode() {
        return getPropertyAsInteger("fly_cross_barcode");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "gender")
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

    @SAGEAttribute(cvName = "light_imagery", termName = "full_age")
    public String getFullAge() {
        return getPropertyAsString("light_imagery_full_age");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "mounting_protocol")
    public String getMountingProtocol() {
        return getPropertyAsString("light_imagery_mounting_protocol");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "heat_shock_hour")
    public String getHeatShockHour() {
        return getPropertyAsString("light_imagery_heat_shock_hour");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "heat_shock_interval")
    public String getHeatShockInterval() {
        return getPropertyAsString("light_imagery_heat_shock_interval");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "heat_shock_minutes")
    public String getHeatShockMinutes() {
        return getPropertyAsString("light_imagery_heat_shock_minutes");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_illumination_channel_1_name")
    public String getIlluminationChannel1Name() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_1_name");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_illumination_channel_2_name")
    public String getIlluminationChannel2Name() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_2_name");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_illumination_channel_3_name")
    public String getIlluminationChannel3Name() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_3_name");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_illumination_channel_1_power_bc_1")
    public String getIlluminationChannel1PowerBC1() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_1_power_bc_1");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_illumination_channel_2_power_bc_1")
    public String getIlluminationChannel2PowerBC1() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_2_power_bc_1");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "lsm_illumination_channel_3_power_bc_1")
    public String getIlluminationChannel3PowerBC1() {
        return getPropertyAsString("light_imagery_lsm_illumination_channel_3_power_bc_1");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "family")
    public String getImageFamily() {
        return getPropertyAsString("light_imagery_family");
    }

    public String getImagingProject() {
        return getPropertyAsString("light_imagery_imaging_project");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "interpolation_elapsed")
    public String getInterpolationElapsed() {
        return getPropertyAsString("light_imagery_interpolation_elapsed");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "interpolation_start")
    public Integer getInterpolationStart() {
        return getPropertyAsInteger("light_imagery_interpolation_start");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "interpolation_stop")
    public Integer getInterpolationStop() {
        return getPropertyAsInteger("light_imagery_interpolation_stop");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "microscope")
    public String getMicroscope() {
        return getPropertyAsString("light_imagery_microscope");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "microscope_filename")
    public String getMicroscopeFilename() {
        return getPropertyAsString("light_imagery_microscope_filename");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "mac_address")
    public String getMacAddress() {
        return getPropertyAsString("light_imagery_mac_address");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "sample_0time")
    public String getSampleZeroTime() {
        return getPropertyAsString("light_imagery_sample_0time");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "sample_0z")
    public String getSampleZeroZ() {
        return getPropertyAsString("light_imagery_sample_0z");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "scan_start")
    public Integer getScanStart() {
        return getPropertyAsInteger("light_imagery_scan_start");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "scan_stop")
    public Integer getScanStop() {
        return getPropertyAsInteger("light_imagery_scan_stop");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "scan_type")
    public String getScanType() {
        return getPropertyAsString("light_imagery_scan_type");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "screen_state")
    public String getScreenState() {
        return getPropertyAsString("light_imagery_screen_state");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "tissue_orientation")
    public String getTissueOrientation() {
        return getPropertyAsString("light_imagery_tissue_orientation");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "total_pixels")
    public String getTotalPixels() {
        return getPropertyAsString("light_imagery_total_pixels");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "tracks")
    public Integer getTracks() {
        return getPropertyAsInteger("light_imagery_tracks");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "voxel_size_x")
    public String getVoxelSizeX() {
        return getPropertyAsString("light_imagery_voxel_size_x");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "voxel_size_y")
    public String getVoxelSizeY() {
        return getPropertyAsString("light_imagery_voxel_size_y");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "voxel_size_z")
    public String getVoxelSizeZ() {
        return getPropertyAsString("light_imagery_voxel_size_z");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "dimension_x")
    public String getDimensionX() {
        return getPropertyAsString("light_imagery_dimension_x");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "dimension_y")
    public String getDimensionY() {
        return getPropertyAsString("light_imagery_dimension_y");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "dimension_z")
    public String getDimensionZ() {
        return getPropertyAsString("light_imagery_dimension_z");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "zoom_x")
    public String getZoomX() {
        return getPropertyAsString("light_imagery_zoom_x");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "zoom_y")
    public String getZoomY() {
        return getPropertyAsString("light_imagery_zoom_y");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "zoom_z")
    public String getZoomZ() {
        return getPropertyAsString("light_imagery_zoom_z");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "vt_line")
    public String getVtLine() {
        return getPropertyAsString("light_imagery_vt_line");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "qi")
    public String getQiScore() {
        return getPropertyAsString("light_imagery_qi");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "qm")
    public String getQmScore() {
        return getPropertyAsString("light_imagery_qm");
    }

    @SAGEAttribute(cvName = "light_imagery", termName = "channels")
    public Integer getNumChannels() {
        return getPropertyAsInteger("light_imagery_channels");
    }

    @SAGEAttribute(cvName = "line_query", termName = "organism")
    public String getOrganism() {
        return getPropertyAsString("line_organism");
    }

    @SAGEAttribute(cvName = "line", termName = "genotype")
    public String getGenotype() {
        return getPropertyAsString("line_genotype");
    }

    @SAGEAttribute(cvName = "line", termName = "flycore_id")
    public Integer getFlycoreId() {
        return getPropertyAsInteger("line_flycore_id");
    }

    @SAGEAttribute(cvName = "line", termName = "flycore_alias")
    public String getFlycoreAlias() {
        return getPropertyAsString("line_flycore_alias");
    }

    @SAGEAttribute(cvName = "line", termName = "flycore_lab")
    public String getFlycoreLabId() {
        return getPropertyAsString("line_flycore_lab");
    }

    @SAGEAttribute(cvName = "line", termName = "flycore_landing_site")
    public String getFlycoreLandingSite() {
        return getPropertyAsString("line_flycore_landing_site");
    }

    @SAGEAttribute(cvName = "line", termName = "flycore_permission")
    public String getFlycorePermission() {
        return getPropertyAsString("line_flycore_permission");
    }

    @SAGEAttribute(cvName = "line", termName = "flycore_project")
    public String getFlycoreProject() {
        return getPropertyAsString("line_flycore_project");
    }

    @SAGEAttribute(cvName = "line", termName = "flycore_project_subcat")
    public String getFlycorePSubcategory() {
        return getPropertyAsString("line_flycore_project_subcat");
    }

    @SAGEAttribute(cvName = "line", termName = "hide")
    public String getLineHide() {
        return getPropertyAsString("line_hide");
    }

    @SAGEAttribute(cvName = "jacs_calculated", termName = "optical_resolution")
    public String getOpticalResolution() {
        if (getVoxelSizeX() != null && getVoxelSizeY() != null && getVoxelSizeZ() != null) {
            return getVoxelSizeX() + "x" + getVoxelSizeY() + "x" + getVoxelSizeZ();
        } else {
            return null;
        }
    }

    @SAGEAttribute(cvName = "jacs_calculated", termName = "image_size")
    public String getImageSize() {
        if (getDimensionX() != null && getDimensionY() != null && getDimensionZ() != null) {
            return getDimensionX() + "x" + getDimensionY() + "x" + getDimensionZ();
        } else {
            return null;
        }
    }

    public String getFilepath() {
        String filepath = getJfsPath();
        if (StringUtils.isBlank(filepath)) {
            filepath = getPath();
            return StringUtils.isNotBlank(filepath) ? filepath : getName();
        } else {
            return filepath;
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("lab", lab)
                .append("dataset", dataset)
                .append("lineName", lineName)
                .append("slideCode", slideCode)
                .append("name", name)
                .append("area", area)
                .append("objectiveName", objectiveName)
                .append("tile", tile)
                .build();
    }
}

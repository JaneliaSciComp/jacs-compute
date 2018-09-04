package org.janelia.jacs2.asyncservice.sample.aux;

import java.io.File;
import java.util.Map;

import org.janelia.model.domain.enums.Objective;

/**
 * Image data from SAGE that is used for creating Sample and LSM Stack entities. 
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SlideImage {

    public static final String DATA_SET = "light_imagery_data_set";
    public static final String FLY_CROSS_BARCODE = "fly_cross_barcode";
    public static final String LIGHT_IMAGERY_CHANNEL_SPEC = "light_imagery_channel_spec";
    public static final String LIGHT_IMAGERY_GENDER = "light_imagery_gender";
    public static final String LIGHT_IMAGERY_AGE = "light_imagery_age";
    public static final String LIGHT_IMAGERY_CHANNELS = "light_imagery_channels";
    public static final String LIGHT_IMAGERY_MOUNTING_PROTOCOL = "light_imagery_mounting_protocol";
    public static final String LIGHT_IMAGERY_TISSUE_ORIENTATION = "light_imagery_tissue_orientation";
    public static final String LIGHT_IMAGERY_VT_LINE = "light_imagery_vt_line";
    public static final String FLY_EFFECTOR = "fly_effector";
    public static final String LIGHT_IMAGERY_VOXEL_SIZE_X = "light_imagery_voxel_size_x";
    public static final String LIGHT_IMAGERY_VOXEL_SIZE_Y = "light_imagery_voxel_size_y";
    public static final String LIGHT_IMAGERY_VOXEL_SIZE_Z = "light_imagery_voxel_size_z";
    public static final String LIGHT_IMAGERY_DIMENSION_X = "light_imagery_dimension_x";
    public static final String LIGHT_IMAGERY_DIMENSION_Y = "light_imagery_dimension_y";
    public static final String LIGHT_IMAGERY_DIMENSION_Z = "light_imagery_dimension_z";
    public static final String LIGHT_IMAGERY_OBJECTIVE = "light_imagery_objective";
    public static final String IMAGE_QUERY_JFS_PATH = "image_query_jfs_path";
    public static final String IMAGE_QUERY_PATH = "image_query_path";
    public static final String IMAGE_QUERY_ID = "image_query_id";
    public static final String LIGHT_IMAGERY_SLIDE_CODE = "light_imagery_slide_code";
    public static final String IMAGE_QUERY_NAME = "image_query_name";
    public static final String LIGHT_IMAGERY_TILE = "light_imagery_tile";
    public static final String IMAGE_QUERY_LINE = "image_query_line";
    public static final String LIGHT_IMAGERY_AREA = "light_imagery_area";
    public static final String TMOG_DATE = "tmog_date";
    public static final String IMAGE_LAB_NAME = "image_lab_name";

    private static final String F_PREFIX = "f";
    private static final String M_PREFIX = "m";
    private static final String X_PREFIX = "x";
    private Map<String,Object> properties;

    public SlideImage(Map<String,Object> properties) {
        this.properties = properties;

        String gender = (String)properties.get(LIGHT_IMAGERY_GENDER);
        if (gender!=null) {
        	properties.put(LIGHT_IMAGERY_GENDER, sanitizeGender(gender));
        }
    }
    
    public Map<String,Object> getProperties() {
        return properties;
    }
    
    public String getFilepath() {

        // Use JFS path if available
        String jfsPath = (String)properties.get(IMAGE_QUERY_JFS_PATH);
        if (jfsPath!=null) {
            return jfsPath;
        }
    
        // Or use the normal path
        String path = (String)properties.get(IMAGE_QUERY_PATH);
        if (path!=null) {
            return path;
        }
     
        return null;
    }
    
    public File getFile() {
    	String filepath = getFilepath();
    	if (filepath==null) return null;
    	return new File(filepath);
    }
    
    public String getName() {
    	File file = getFile();
    	if (file==null) return null;
    	return file.getName();
    }
        
    public String getObjective() {
        String objectiveStr = (String)properties.get(LIGHT_IMAGERY_OBJECTIVE);
        if (objectiveStr!=null) {
            if (objectiveStr.contains(Objective.OBJECTIVE_10X.getName())) {
                return Objective.OBJECTIVE_10X.getName();
            }
            else if (objectiveStr.contains(Objective.OBJECTIVE_20X.getName())) {
                return Objective.OBJECTIVE_20X.getName();
            }
            else if (objectiveStr.contains(Objective.OBJECTIVE_25X.getName())) {
                return Objective.OBJECTIVE_25X.getName();
            }
            else if (objectiveStr.contains(Objective.OBJECTIVE_40X.getName())) {
                return Objective.OBJECTIVE_40X.getName();
            }
            else if (objectiveStr.contains(Objective.OBJECTIVE_63X.getName())) {
                return Objective.OBJECTIVE_63X.getName();
            }
        }
        return "";
    }
    
    public Integer getSageId() {
        Number id = (Number)properties.get(IMAGE_QUERY_ID);
        if (id==null) return null;
        return id.intValue();
    }

    public void setSageId(Integer sageId) {
        properties.put(IMAGE_QUERY_ID, new Long(sageId));
    }

    
    public String getSlideCode() {
        return (String)properties.get(LIGHT_IMAGERY_SLIDE_CODE);
    }

    public void setSlideCode(String slideCode) {
        properties.put(LIGHT_IMAGERY_SLIDE_CODE, slideCode);
    }

    public String getImageName() {
        return (String)properties.get(IMAGE_QUERY_NAME);
    }

    public void setImageName(String imageName) {
        properties.put(IMAGE_QUERY_NAME, imageName);
    }

    public String getImagePath() {
        return (String)properties.get(IMAGE_QUERY_PATH);
    }

    public void setImagePath(String imagePath) {
        properties.put(IMAGE_QUERY_PATH, imagePath);
    }

    public String getJfsPath() {
        return (String)properties.get(IMAGE_QUERY_JFS_PATH);
    }

    public void setJfsPath(String jfsPath) {
        properties.put(IMAGE_QUERY_JFS_PATH, jfsPath);
    }

    public String getTile() {
        return (String)properties.get(LIGHT_IMAGERY_TILE);
    }

    public void setTile(String tileType) {
        properties.put(LIGHT_IMAGERY_TILE, tileType);
    }

    public String getLine() {
        return (String)properties.get(IMAGE_QUERY_LINE);
    }

    public void setLine(String line) {
        properties.put(IMAGE_QUERY_LINE, line);
    }

    public String getAnatomicalArea() {
        return (String)properties.get(LIGHT_IMAGERY_AREA);
    }

    public void setAnatomicalArea(String area) {
        properties.put(LIGHT_IMAGERY_AREA, area);
    }

    public String getTmogDate() {
        return (String)properties.get(TMOG_DATE);
    }

    public void setTmogDate(String tmogDate) {
        properties.put(TMOG_DATE, tmogDate);
    }

    public String getLab() {
        return (String)properties.get(IMAGE_LAB_NAME);
    }

    public void setLab(String lab) {
        properties.put(IMAGE_LAB_NAME, lab);
    }
    
    public String getDataSet() {
        return (String)properties.get(DATA_SET);
    }

    public void setDataSet(String dataSetName) {
        properties.put(DATA_SET, dataSetName);
    }

    public String getCrossBarcode() {
        return (String)properties.get(FLY_CROSS_BARCODE);
    }

    public void setCrossBarcode(String crossBarCode) {
        properties.put(FLY_CROSS_BARCODE, crossBarCode);
    }

    public String getChannelSpec() {
        return (String)properties.get(LIGHT_IMAGERY_CHANNEL_SPEC);
    }

    public void setChannelSpec(String channelSpec) {
        properties.put(LIGHT_IMAGERY_CHANNEL_SPEC, channelSpec);
    }

    public String getAge() {
        return (String)properties.get(LIGHT_IMAGERY_AGE);
    }

    public void setAge(String age) {
        properties.put(LIGHT_IMAGERY_AGE, age);
    }

    public String getGender() {
        return (String)properties.get(LIGHT_IMAGERY_GENDER);
    }

    public void setGender(String gender) {
        properties.put(LIGHT_IMAGERY_GENDER, sanitizeGender(gender));
    }

    public String getChannels() {
        return (String)properties.get(LIGHT_IMAGERY_CHANNELS);
    }

    public void setChannels(String channels) {
        properties.put(LIGHT_IMAGERY_CHANNELS, channels);
    }

    public String getMountingProtocol() {
        return (String)properties.get(LIGHT_IMAGERY_MOUNTING_PROTOCOL);
    }

    public void setMountingProtocol(String mountingProtocol) {
        properties.put(LIGHT_IMAGERY_MOUNTING_PROTOCOL, mountingProtocol);
    }

    public String getTissueOrientation() {
        return (String)properties.get(LIGHT_IMAGERY_TISSUE_ORIENTATION);
    }

    public void setTissueOrientation(String tissueOrientation) {
        properties.put(LIGHT_IMAGERY_TISSUE_ORIENTATION, tissueOrientation);
    }

    public String getVtLine() {
        return (String)properties.get(LIGHT_IMAGERY_VT_LINE);
    }

    public void setVtLine(String vtLine) {
        properties.put(LIGHT_IMAGERY_VT_LINE, vtLine);
    }

    public String getEffector() {
        return (String)properties.get(FLY_EFFECTOR);
    }

    public void setEffector(String effector) {
        properties.put(FLY_EFFECTOR, effector);
    }

    public void setObjective(String objective) {
        properties.put(LIGHT_IMAGERY_OBJECTIVE, objective);
    }

    public String[] getOpticalRes() {
        return new String[] {
                (String)properties.get(LIGHT_IMAGERY_VOXEL_SIZE_X),
                (String)properties.get(LIGHT_IMAGERY_VOXEL_SIZE_Y),
                (String)properties.get(LIGHT_IMAGERY_VOXEL_SIZE_Z),
        };
    }

    public void setOpticalRes(String opticalResX, String opticalResY, String opticalResZ) {
        properties.put(LIGHT_IMAGERY_VOXEL_SIZE_X, opticalResX);
        properties.put(LIGHT_IMAGERY_VOXEL_SIZE_Y, opticalResY);
        properties.put(LIGHT_IMAGERY_VOXEL_SIZE_Z, opticalResZ);
    }

    public void setOpticalRes(String[] opticalRes) {
        properties.put(LIGHT_IMAGERY_VOXEL_SIZE_X, opticalRes[0]);
        properties.put(LIGHT_IMAGERY_VOXEL_SIZE_Y, opticalRes[1]);
        properties.put(LIGHT_IMAGERY_VOXEL_SIZE_Z, opticalRes[2]);
    }

    public String[] getPixelRes() {
        return new String[] {
                (String)properties.get(LIGHT_IMAGERY_VOXEL_SIZE_X),
                (String)properties.get(LIGHT_IMAGERY_VOXEL_SIZE_Y),
                (String)properties.get(LIGHT_IMAGERY_VOXEL_SIZE_Z),
        };
    }

    public void setPixelRes(String pixelResX, String pixelResY, String pixelResZ) {
        properties.put(LIGHT_IMAGERY_DIMENSION_X, pixelResX);
        properties.put(LIGHT_IMAGERY_DIMENSION_Y, pixelResY);
        properties.put(LIGHT_IMAGERY_DIMENSION_Z, pixelResZ);
    }

    public void setPixelRes(String[] pixelRes) {
        properties.put(LIGHT_IMAGERY_DIMENSION_X, pixelRes[0]);
        properties.put(LIGHT_IMAGERY_DIMENSION_Y, pixelRes[1]);
        properties.put(LIGHT_IMAGERY_DIMENSION_Z, pixelRes[2]);
    }

    /**
     * Convert non-standard gender values like "Female" into standardized codes like "f". The
     * four standardized codes are "m", "f", "x", and "No Consensus" in the case of samples.
     */
    private String sanitizeGender(String gender) {
        if (gender==null) {
            return null;
        }
        String genderLc = gender.toLowerCase();
        if (genderLc.startsWith(F_PREFIX)) {
            return F_PREFIX;
        }
        else if (genderLc.startsWith(M_PREFIX)) {
            return M_PREFIX;
        }
        else if (genderLc.startsWith(X_PREFIX)) {
            return X_PREFIX;
        }
        else {
            return null;
        }
    }

}

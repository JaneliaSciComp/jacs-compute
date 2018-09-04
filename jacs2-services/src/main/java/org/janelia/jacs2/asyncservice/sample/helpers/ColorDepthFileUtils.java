package org.janelia.jacs2.asyncservice.sample.helpers;

import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileFilter;
import java.util.HashMap;
import java.util.Map;

/**
 * Get or create the color depth filepath for the current sample. 
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Singleton
public class ColorDepthFileUtils {

    private static final Logger log = LoggerFactory.getLogger(QuotaValidator.class);

    @PropertyValue(name = "service.DefaultWorkingDir")
    private String FILESTORE_DIR;

    private static final String USERNAME = "system";
    private static final String FILE_NODE_NAME = "ColorDepthMIPs";
    
    private static File rootDir;
    
    /**
     * Returns the filestore's color depth MIP directory. 
     * 
     * The directory is created in the filestore if it doesn't exist.
     */
    public synchronized File getDir() {
        
        if (rootDir==null) {
            String colorDepthMipPath = FILESTORE_DIR+"/"+USERNAME+"/"+FILE_NODE_NAME;
            rootDir = new File(colorDepthMipPath);
    
            if (!rootDir.exists()) {
                if (!rootDir.mkdirs()) {
                    throw new IllegalStateException("Could not create color depth MIP dir: "+rootDir);
                }
            }
        }
        
        return rootDir;
    }
    
    /**
     * Returns the dir containing color depth MIPs for the given alignment space and data set.
     *  
     * The directory is created in the filestore if it doesn't exist.
     * 
     * @param alignmentSpace
     * @param dataSetIdentifier
     */
    public File getDir(String alignmentSpace, String dataSetIdentifier) {

        File alignmentTypeDir = new File(getDir(), alignmentSpace);
        alignmentTypeDir.mkdirs();
        
        File dataSetDir = new File(alignmentTypeDir, dataSetIdentifier);
        dataSetDir.mkdirs();
        
        return dataSetDir;
    }
    
    /**
     * Counts all color depth directories for the given data set, and returns a map keyed
     * on alignment space, with values representing the number of projections available in that
     * space.
     * @param dataSetIdentifier
     * @return
     */
    public Map<String,Integer> getFileCounts(String dataSetIdentifier) {
        return getFileCounts(dataSetIdentifier, null);
    }
    
    /**
     * Counts all color depth directories for the given data set, and returns a map keyed
     * on alignment space, with values representing the number of projections available in that
     * space. If the given alignmentType is null, all alignment spaces are counted for the given data set.
     * Otherwise, if alignmentType is specified, only a single count is returned within the map.
     * @param dataSetIdentifier
     * @return
     */
    public Map<String,Integer> getFileCounts(String dataSetIdentifier, String alignmentType) {
        
        Map<String,Integer> counts = new HashMap<>();
        
        File rootDir = getDir();
        
        File[] alignmentDirs;
        if (alignmentType!=null) {
            alignmentDirs = new File[] { new File(rootDir, alignmentType) };
        }
        else {
            alignmentDirs = rootDir.listFiles();
        }
        
        for(File alignmentSpaceDir : alignmentDirs) {
            
            if (!alignmentSpaceDir.isDirectory()) continue;
            
            File dataSetDir = new File(alignmentSpaceDir, dataSetIdentifier);
            log.info("Counting files in "+dataSetDir);
            
            if (dataSetDir.exists()) {
                
                // Count all the PNG images in the directory
                Integer fileCount = dataSetDir.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File file) {
                        return !file.isDirectory() && file.getName().endsWith(".png");
                    }
                }).length;
                
                log.info("  Found "+fileCount+" png files");
                
                counts.put(alignmentSpaceDir.getName(), fileCount);
            }
        }
        
        return counts;
    }
    
}
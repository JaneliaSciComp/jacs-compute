package org.janelia.jacs2.asyncservice.sample.helpers;

import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper methods for creating virtual representations of the file system in the Entity model.
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class FileDiscoveryHelper {

    private static final Logger log = LoggerFactory.getLogger(FileDiscoveryHelper.class);

    public final Long FILE_3D_SIZE_THRESHOLD = new Long(5000000L);

    private Set<Pattern> exclusions = new HashSet<Pattern>();
    private boolean excludeSymLinks = true;
    
    public FileDiscoveryHelper() {
        addFileExclusion("*.log");
        addFileExclusion("*.oos");
        addFileExclusion("sge_*");
        addFileExclusion("temp");
        addFileExclusion("debug");
        addFileExclusion("tmp.*");
        addFileExclusion("core.*");
        addFileExclusion("screenshot_*");
        addFileExclusion("xvfb.*");
    }
    
    public void addFileExclusion(String filePattern) {
    	Pattern p = Pattern.compile(filePattern.replaceAll("\\*", "(.*?)"));
    	exclusions.add(p);
    }
    
	public void setExcludeSymLinks(boolean excludeSymLinks) {
		this.excludeSymLinks = excludeSymLinks;
	}

	private boolean isExcluded(String filename) {		
		for(Pattern p : exclusions) {
			Matcher m = p.matcher(filename);
			if (m.matches()) {
				//logger.debug("Excluding "+filename+" based on pattern "+p.pattern());
				return true;
			}
		}
		return false;
    }
    
    public List<File> collectFiles(File dir) throws Exception {
    	return collectFiles(dir, false);
    }
    
    public List<File> collectFiles(File dir, boolean recurse) throws Exception {
    	
    	List<File> allFiles = new ArrayList<File>();
        List<File> files = FileUtils.getOrderedFilesInDir(dir);
        log.info("Found "+files.size()+" files in "+dir.getAbsolutePath());
        
        for (File resultFile : files) {
        	String filename = resultFile.getName();
        	
			if (isExcluded(filename)) {
				continue; 
			}
			
			if (FileUtils.isSymlink(resultFile) && excludeSymLinks) {
				continue; 
			}
        	
        	if (resultFile.isDirectory()) {
        		if (recurse) {
            		allFiles.addAll(collectFiles(resultFile, true));
            	}
        	}
        	else {
        		allFiles.add(resultFile);	
        	}
        }
        
        return allFiles;
    }

    public List<String> getFilepaths(String rootPath) throws Exception {

        List<String> filepaths = new ArrayList<>();
        File dir = new File(rootPath);
        log.debug("Processing results in "+dir.getAbsolutePath());
        
        if (!dir.canRead()) {
            log.warn("Cannot read from folder "+dir.getAbsolutePath());
            return filepaths;
        }
        
        for(File file : collectFiles(dir, true)) {
            filepaths.add(file.getAbsolutePath());
        }
        return filepaths;
    }
	
}

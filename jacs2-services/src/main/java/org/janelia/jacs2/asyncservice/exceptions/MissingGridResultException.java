package org.janelia.jacs2.asyncservice.exceptions;

/**
 * Indicates a missing result of a grid submission. One can expect the filepath
 * to have "sge_output" and "sge_error" directories which may provide more details
 * to why the results are missing.   
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class MissingGridResultException extends MissingDataException {

    private String filepath;
    
    public MissingGridResultException(String filepath, String msg) {
        super(msg);
        this.filepath = filepath;
    }
    
    public MissingGridResultException(String filepath, String msg, Throwable e) {
        super(msg, e);
        this.filepath = filepath;
    }
    
    public String getFilepath() {
        return filepath;
    }
}

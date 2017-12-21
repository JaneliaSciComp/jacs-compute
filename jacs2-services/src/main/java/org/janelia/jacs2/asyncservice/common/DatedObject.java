
package org.janelia.jacs2.asyncservice.common;

/**
 * An object wrapper which knows when it was created.
 * @param <T> type of the wrapped object
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class DatedObject<T> {

    private long creationTime;
    private T obj;

    public DatedObject(T obj) {
        this.creationTime = System.currentTimeMillis();
        this.obj = obj;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public T getObj() {
        return obj;
    }
}
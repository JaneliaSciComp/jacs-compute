package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.model.jacsservice.JacsServiceData;

/**
 * Jacs service result.
 * @param <R> result type.
 */
public final class JacsServiceResult<R> {
    private final JacsServiceData jacsServiceData;
    private R result;

    public JacsServiceResult(JacsServiceData jacsServiceData) {
        this.jacsServiceData = jacsServiceData;
    }

    public JacsServiceResult(JacsServiceData jacsServiceData, R result) {
        this.jacsServiceData = jacsServiceData;
        this.result = result;
    }

    public final JacsServiceData getJacsServiceData() {
        return jacsServiceData;
    }

    public final R getResult() {
        return result;
    }

    public final void setResult(R result) {
        this.result = result;
    }
}

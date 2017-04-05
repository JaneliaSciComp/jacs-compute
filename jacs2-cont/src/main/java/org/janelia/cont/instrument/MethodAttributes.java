package org.janelia.cont.instrument;

import com.google.common.collect.ImmutableList;

import java.util.List;

final class MethodAttributes {

    private final List<ContinuationPoint> continuationPoints;
    private final List<SynchronizationPoint> synchPoints;
    private final VariableStorageContainer contStorage;
    private final VariablesStorage localsStorage;
    private final VariablesStorage stackStorage;
    private final VariableStorageContainer methodStateContainer;
    private final VariableStorageContainer storageContainerVars;
    private final CacheVariables cacheVars;
    private final LockVariables lockVars;

    MethodAttributes(List<ContinuationPoint> continuationPoints,
                     List<SynchronizationPoint> synchPoints,
                     VariableStorageContainer contStorage,
                     VariablesStorage localsStorage,
                     VariablesStorage stackStorage,
                     VariableStorageContainer methodStateContainer,
                     VariableStorageContainer storageContainerVars,
                     CacheVariables cacheVars,
                     LockVariables lockVars) {
        this.continuationPoints = ImmutableList.copyOf(continuationPoints);
        this.synchPoints = ImmutableList.copyOf(synchPoints);
        this.contStorage = contStorage;
        this.localsStorage = localsStorage;
        this.stackStorage = stackStorage;
        this.methodStateContainer = methodStateContainer;
        this.storageContainerVars = storageContainerVars;
        this.cacheVars = cacheVars;
        this.lockVars = lockVars;
    }

    List<ContinuationPoint> getContinuationPoints() {
        return continuationPoints;
    }

    ContinuationPoint getContinuationPoint(int idx) {
        return continuationPoints.get(idx);
    }

    List<SynchronizationPoint> getSynchPoints() {
        return synchPoints;
    }

    VariableStorageContainer getContStorage() {
        return contStorage;
    }

    VariablesStorage getLocalsStorage() {
        return localsStorage;
    }

    VariablesStorage getStackStorage() {
        return stackStorage;
    }

    VariableStorageContainer getMethodStateContainer() {
        return methodStateContainer;
    }

    VariableStorageContainer getStorageContainerVars() {
        return storageContainerVars;
    }

    CacheVariables getCacheVars() {
        return cacheVars;
    }

    LockVariables getLockVars() {
        return lockVars;
    }
}

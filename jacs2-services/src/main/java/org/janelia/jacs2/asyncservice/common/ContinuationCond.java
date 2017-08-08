package org.janelia.jacs2.asyncservice.common;

/**
 * Interface to check the continuation condition based on a provided state.
 * @param <S> state type
 */
@FunctionalInterface
public interface ContinuationCond<S> {
    class Cond<T> {
        private final T state;
        private final boolean condValue;

        public Cond(T state, boolean condValue) {
            this.state = state;
            this.condValue = condValue;
        }

        public T getState() {
            return state;
        }

        public boolean isCondValue() {
            return condValue;
        }

        public boolean isNotCondValue() {
            return !isCondValue();
        }

    }
    Cond<S> checkCond(S state);

    default ContinuationCond<S> negate() {
        return (S state) -> {
            Cond<S> cond = checkCond(state);
            return new Cond<>(cond.state, !cond.condValue);
        };
    }

}

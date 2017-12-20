package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * FutureBasedServiceComputation is an implementation of a ServiceComputation.
 *
 * @param <T> result type
 */
public class FutureBasedServiceComputation<T> implements ServiceComputation<T> {

    private static <U> U waitForResult(ServiceComputation<U> computation) {
        if (computation.isDone()) {
            return computation.get();
        } else {
            throw new SuspendedException();
        }
    }

    private final ServiceComputationQueue computationQueue;
    private final Logger logger;
    private final ServiceComputationTask<T> task;

    FutureBasedServiceComputation(ServiceComputationQueue computationQueue, Logger logger, ServiceComputationTask<T> task) {
        this.computationQueue = computationQueue;
        this.logger = logger;
        this.task = task;
    }

    FutureBasedServiceComputation(ServiceComputationQueue computationQueue, Logger logger) {
        this(computationQueue, logger, new ServiceComputationTask<>(null));
    }

    FutureBasedServiceComputation(ServiceComputationQueue computationQueue, Logger logger, T result) {
        this(computationQueue, logger, new ServiceComputationTask<>(null, result));
    }

    FutureBasedServiceComputation(ServiceComputationQueue computationQueue, Logger logger, Throwable exc) {
        this(computationQueue, logger, new ServiceComputationTask<>(null, exc));
    }

    @Override
    public T get() {
        ServiceComputationTask.ComputeResult<T> result = task.get();
        if (result.exc != null) {
            if (result.exc instanceof ComputationException) {
                throw (ComputationException) result.exc;
            } else if (result.exc instanceof SuspendedException) {
                throw (SuspendedException) result.exc;
            } else if (result.exc instanceof CompletionException) {
                throw (CompletionException) result.exc;
            } else {
                throw new CompletionException(result.exc);
            }
        }
        return result.result;
    }

    @Override
    public boolean cancel() {
        return task.cancel();
    }

    @Override
    public boolean isCanceled() {
        return task.isCanceled();
    }

    @Override
    public boolean isDone() {
        return task.isDone();
    }

    @Override
    public boolean isCompletedExceptionally() {
        return task.isCompletedExceptionally();
    }

    public void complete(T result) {
        task.complete(result);
    }

    public void completeExceptionally(Throwable exc) {
        task.completeExceptionally(exc);
    }

    @Override
    public ServiceComputation<T> supply(Supplier<T> fn) {
        submit(fn::get);
        return this;
    }

    @Override
    public ServiceComputation<T> exceptionally(Function<Throwable, ? extends T> fn) {
        FutureBasedServiceComputation<T> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        next.submit(() -> {
            try {
                T r = waitForResult(this);
                next.complete(r);
            } catch (SuspendedException e) {
                throw e;
            } catch (Exception e) {
                next.complete(fn.apply(e));
            }
            return next.get();
        });
        return next;
    }

    @Override
    public <U> ServiceComputation<U> thenApply(Function<? super T, ? extends U> fn) {
        FutureBasedServiceComputation<U> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        next.submit(() -> {
            applyStage(next, () -> waitForResult(this), fn);
            return next.get();
         });
        return next;
    }

    private <U, V> void applyStage(FutureBasedServiceComputation<V> c, Supplier<U> stageResultSupplier, Function<? super U, ? extends V> fn) {
        try {
            U r = stageResultSupplier.get();
            c.complete(fn.apply(r));
        } catch (SuspendedException e) {
            throw e;
        } catch (Exception e) {
            c.completeExceptionally(e);
        }
    }

    @Override
    public <U> ServiceComputation<U> thenCompose(Function<? super T, ? extends ServiceComputation<U>> fn) {
        FutureBasedServiceComputation<ServiceComputation<U>> nextStage = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        FutureBasedServiceComputation<U> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(nextStage));
        nextStage.submit(() -> {
            applyStage(nextStage, () -> waitForResult(this), fn);
            return nextStage.get();
        });
        next.submit(() -> {
            applyStage(next, () -> waitForResult(nextStage), FutureBasedServiceComputation::waitForResult);
            return next.get();
        });
        return next;
    }

    @Override
    public ServiceComputation<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        FutureBasedServiceComputation<T> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        next.submit(() -> {
            try {
                T r = waitForResult(this);
                action.accept(r, null);
                next.complete(r);
            } catch (SuspendedException e) {
                throw e;
            } catch (Exception e) {
                action.accept(null, e);
                next.completeExceptionally(e);
            }
            return next.get();
        });
        return next;
    }

    @Override
    public <U, V> ServiceComputation<V> thenCombine(ServiceComputation<U> otherComputation, BiFunction<? super T, ? super U, ? extends V> fn) {
        FutureBasedServiceComputation<V> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        next.submit(() -> {
            try {
                T r = waitForResult(this);
                U u = waitForResult(otherComputation);
                next.complete(fn.apply(r, u));
            } catch (SuspendedException e) {
                throw e;
            } catch (Exception e) {
                next.completeExceptionally(e);
            }
            return next.get();
        });
        return next;
    }

    @Override
    public <U> ServiceComputation<U> thenCombineAll(List<ServiceComputation<?>> otherComputations, BiFunction<? super T, List<?>, ? extends U> fn) {
        FutureBasedServiceComputation<U> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        next.submit(() -> {
            try {
                T r = waitForResult(this);
                List<Object> otherResults = otherComputations.stream()
                        .map(FutureBasedServiceComputation::waitForResult)
                        .collect(Collectors.toList());
                next.complete(fn.apply(r, otherResults));
            } catch (SuspendedException e) {
                throw e;
            } catch (Exception e) {
                next.completeExceptionally(e);
            }
            return next.get();
        });
        return next;
    }

    @Override
    public <U> ServiceComputation<U> thenComposeAll(List<ServiceComputation<?>> otherComputations, BiFunction<? super T, List<?>, ? extends ServiceComputation<U>> fn) {
        FutureBasedServiceComputation<ServiceComputation<U>> nextStage = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        FutureBasedServiceComputation<U> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(nextStage));
        nextStage.submit(() -> {
            try {
                T r = waitForResult(this);
                List<Object> otherResults = otherComputations.stream()
                        .map(FutureBasedServiceComputation::waitForResult)
                        .collect(Collectors.toList());
                nextStage.complete(fn.apply(r, otherResults));
            } catch (SuspendedException e) {
                throw e;
            } catch (Exception e) {
                nextStage.completeExceptionally(e);
            }
            return nextStage.get();
        });
        next.submit(() -> {
            applyStage(next, () -> waitForResult(nextStage), FutureBasedServiceComputation::waitForResult);
            return next.get();
        });
        return next;
    }

    public ServiceComputation<ContinuationCond.Cond<T>> thenSuspendUntil(ContinuationCond<T> fn) {
        return thenSuspendUntil(fn, null, null);
    }

    public ServiceComputation<ContinuationCond.Cond<T>> thenSuspendUntil(ContinuationCond<T> fn, Long intervalCheckInMillis, Long timeoutInMillis) {

        long startTime = System.currentTimeMillis();
        PeriodicallyCheckableState<JacsServiceData> periodicCheck = intervalCheckInMillis==null?null:new PeriodicallyCheckableState<>(null, intervalCheckInMillis);

        FutureBasedServiceComputation<ContinuationCond.Cond<T>> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        next.submit(() -> {
            try {
                T r = waitForResult(this);
                if (this.isCompletedExceptionally()) {
                    next.complete(new ContinuationCond.Cond<>(this.get(), false));
                }

                // Check for timeout
                if (timeoutInMillis!=null && (System.currentTimeMillis() - startTime > timeoutInMillis)) {
                    throw new CondTimeoutException(timeoutInMillis);
                }

                // Check to see if we're allowed to test the value yet
                if (periodicCheck!=null && !periodicCheck.updateCheckTime()) {
                    throw new SuspendedException();
                }

                // Check the conditional value
                ContinuationCond.Cond<T> condResult = fn.checkCond(r);
                if (condResult.isNotCondValue()) {
                    throw new SuspendedException();
                }
                else {
                    next.complete(condResult);
                }

            }
            catch (SuspendedException e) {
                throw e;
            }
            catch (Exception e) {
                next.completeExceptionally(e);
            }
            return next.get();
        });
        return next;
    }

    /**
     * TODO: Refactor client code to use this method instead of thenSuspendUntil.
     * This is the refactored version of thenSuspendUntil, which automatically unwraps the cond, which is the only thing
     * you can do with a cond anyway. We should convert the rest of the code to use this, and then remove the old one and
     * rename this one to thenSuspendUntil.
     */
    public ServiceComputation<T> thenSuspendUntil2(ContinuationCond<T> fn) {
        return thenSuspendUntil2(fn, null, null);
    }

    public ServiceComputation<T> thenSuspendUntil2(ContinuationCond<T> fn, Long intervalCheckInMillis, Long timeoutInMillis) {

        long startTime = System.currentTimeMillis();
        PeriodicallyCheckableState<JacsServiceData> periodicCheck = intervalCheckInMillis==null?null:new PeriodicallyCheckableState<>(null, intervalCheckInMillis);

        FutureBasedServiceComputation<T> next = new FutureBasedServiceComputation<>(computationQueue, logger, new ServiceComputationTask<>(this));
        next.submit(() -> {
            try {
                T r = waitForResult(this);
                if (this.isCompletedExceptionally()) {
                    next.complete(this.get());
                }

                // Check for timeout
                if (timeoutInMillis!=null && (System.currentTimeMillis() - startTime > timeoutInMillis)) {
                    throw new CondTimeoutException(timeoutInMillis);
                }

                // Check to see if we're allowed to test the value yet
                if (periodicCheck!=null && !periodicCheck.updateCheckTime()) {
                    throw new SuspendedException();
                }

                // Check the conditional value
                ContinuationCond.Cond<T> condResult = fn.checkCond(r);
                if (condResult.isNotCondValue()) {
                    throw new SuspendedException();
                }
                else {
                    next.complete(condResult.getState());
                }

            }
            catch (SuspendedException e) {
                throw e;
            }
            catch (Exception e) {
                next.completeExceptionally(e);
            }
            return next.get();
        });
        return next;
    }

    private void submit(ServiceComputationTask.ContinuationSupplier<T> fn) {
        task.setResultSupplier(fn);
        computationQueue.submit(task);
    }

}

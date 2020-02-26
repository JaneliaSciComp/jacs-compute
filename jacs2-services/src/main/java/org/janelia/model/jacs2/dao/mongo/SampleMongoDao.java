package org.janelia.model.jacs2.dao.mongo;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacs2.cdi.qualifier.Jacs2Future;
import org.janelia.model.access.dao.mongo.MongoDaoHelper;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.domain.sample.PipelineResult;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.model.jacs2.domain.sample.SamplePipelineRun;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.jacs2.dao.SampleDao;
import org.janelia.model.util.TimebasedIdentifierGenerator;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.DomainModelUtils;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;

public class SampleMongoDao extends AbstractDomainObjectDao<Sample> implements SampleDao {
    @Inject
    @Jacs2Future
    public SampleMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    public PageResult<Sample> findMatchingSamples(Subject subject, Sample pattern, DataInterval<Date> tmogInterval, PageRequest pageRequest) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (pattern.getId() != null) {
            filtersBuilder.add(eq("_id", pattern.getId()));
        }
        if (StringUtils.isNotBlank(pattern.getName())) {
            filtersBuilder.add(eq("name", pattern.getName()));
        }
        if (StringUtils.isNotBlank(pattern.getOwnerKey())) {
            filtersBuilder.add(eq("ownerKey", pattern.getOwnerKey()));
        }
        if (StringUtils.isNotBlank(pattern.getAge())) {
            filtersBuilder.add(eq("age", pattern.getAge()));
        }
        if (StringUtils.isNotBlank(pattern.getEffector())) {
            filtersBuilder.add(eq("effector", pattern.getEffector()));
        }
        if (StringUtils.isNotBlank(pattern.getDataSet())) {
            filtersBuilder.add(eq("dataSet", pattern.getDataSet()));
        }
        if (StringUtils.isNotBlank(pattern.getLine())) {
            filtersBuilder.add(eq("line", pattern.getLine()));
        }
        if (StringUtils.isNotBlank(pattern.getSlideCode())) {
            filtersBuilder.add(eq("slideCode", pattern.getSlideCode()));
        }
        if (StringUtils.isNotBlank(pattern.getGender())) {
            filtersBuilder.add(eq("gender", pattern.getGender()));
        }
        if (StringUtils.isNotBlank(pattern.getStatus())) {
            filtersBuilder.add(eq("status", pattern.getStatus()));
        }
        if (tmogInterval.hasFrom()) {
            filtersBuilder.add(gte("tmogDate", tmogInterval.getFrom()));
        }
        if (tmogInterval.hasTo()) {
            filtersBuilder.add(lt("tmogDate", tmogInterval.getTo()));
        }
        if (DomainModelUtils.isNotAdmin(subject)) {
            filtersBuilder.add(createSubjectReadPermissionFilter(subject));
        }

        ImmutableList<Bson> filters = filtersBuilder.build();

        Bson bsonFilter = null;
        if (!filters.isEmpty()) bsonFilter = and(filters);
        List<Sample> results = find(
                bsonFilter,
                MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                Sample.class);
        return new PageResult<>(pageRequest, results);
    }

    @Override
    public void addObjectivePipelineRun(Sample sample, String objective, SamplePipelineRun samplePipelineRun) {
        if (sample.getObjectiveSamples() == null) {
            throw new IllegalArgumentException("Sample " + sample + " has no objective samples");
        }
        List<Bson> updatedFields = new ArrayList<>();
        sample.lookupIndexedObjective(objective)
                .map(indexedObjectiveSample -> {
                            Optional<IndexedReference<SamplePipelineRun, Integer>> indexedPipelineRun = indexedObjectiveSample.getReference().findPipelineRunById(samplePipelineRun.getId());
                            if (!indexedPipelineRun.isPresent()) {
                                // if no pipeline run was found create one
                                String fieldName = String.format("objectiveSamples.%d.pipelineRuns", indexedObjectiveSample.getPos());
                                updatedFields.add(Updates.push(fieldName, samplePipelineRun));
                                return true; // add pipeline run
                            } else {
                                if (StringUtils.isNotBlank(samplePipelineRun.getName())) {
                                    String fieldName = String.format("objectiveSamples.%d.pipelineRuns.%d.name", indexedObjectiveSample.getPos(), indexedPipelineRun.get().getPos());
                                    updatedFields.add(Updates.set(fieldName, samplePipelineRun.getName()));
                                }
                                if (StringUtils.isNotBlank(samplePipelineRun.getPipelineProcess())) {
                                    String fieldName = String.format("objectiveSamples.%d.pipelineRuns.%d.pipelineProcess", indexedObjectiveSample.getPos(), indexedPipelineRun.get().getPos());
                                    updatedFields.add(Updates.set(fieldName, samplePipelineRun.getPipelineProcess()));
                                }
                                return false; // update the pipeline run
                            }
                        }
                )
                .orElseThrow(() -> new IllegalArgumentException("No '" + objective + "' found for " + sample.getName()));
        if (!updatedFields.isEmpty()) {
            UpdateOptions updateOptions = new UpdateOptions();
            updateOptions.upsert(false);
            update(getUpdateMatchCriteria(sample), Updates.combine(updatedFields), updateOptions);
        }
    }

    @Override
    public void addSampleObjectivePipelineRunResult(Sample sample, String objective, Number runId, Number resultId, PipelineResult pipelineResult) {
        if (sample.getObjectiveSamples() == null) {
            throw new IllegalArgumentException("Sample " + sample + " has no objective samples");
        }

        List<Bson> updatedFields = new ArrayList<>();
        sample.lookupIndexedObjective(objective)
                .flatMap(positionalObjectiveSample -> {
                    Optional<IndexedReference<SamplePipelineRun, Integer>> positionalPipelineRun = positionalObjectiveSample.getReference().findPipelineRunById(runId);
                    if (positionalPipelineRun.isPresent()) {
                        if (resultId == null) {
                            return Optional.of(String.format("objectiveSamples.%d.pipelineRuns.%d.results", positionalObjectiveSample.getPos(), positionalPipelineRun.get().getPos()));
                        } else {
                            return positionalPipelineRun.get().getReference().streamResults()
                                    .filter(indexedResult -> indexedResult.getReference().sameId(resultId))
                                    .findFirst()
                                    .map(indexedResult -> {
                                        List<Object> formatArgs = new ArrayList<>(ImmutableList.of(positionalObjectiveSample.getPos(), positionalPipelineRun.get().getPos()));
                                        StringBuilder fieldFormat = new StringBuilder("objectiveSamples.%d.pipelineRuns.%d");
                                        for (IndexedReference<Integer, Integer> treePos : indexedResult.getPos()) {
                                            fieldFormat.append(".results.%d");
                                            formatArgs.add(treePos.getPos());
                                        }
                                        fieldFormat.append(".results");
                                        return String.format(fieldFormat.toString(), formatArgs.toArray());
                                    })
                                    ;
                        }
                    } else {
                        return Optional.<String>empty();
                    }
                })
                .ifPresent(fn -> updatedFields.add(Updates.push(fn, pipelineResult)));
        if (!updatedFields.isEmpty()) {
            UpdateOptions updateOptions = new UpdateOptions();
            updateOptions.upsert(false);
            update(getUpdateMatchCriteria(sample), Updates.combine(updatedFields), updateOptions);
        }
    }

    @Override
    public void updateSampleObjectivePipelineRunResult(Sample sample, String objective, Number runId, PipelineResult pipelineResult) {
        if (sample.getObjectiveSamples() == null) {
            throw new IllegalArgumentException("Sample " + sample + " has no objective samples");
        }

        List<Bson> updatedFields = new ArrayList<>();
        sample.lookupIndexedObjective(objective)
                .flatMap(positionalObjectiveSample -> {
                    Optional<IndexedReference<SamplePipelineRun, Integer>> positionalPipelineRun = positionalObjectiveSample.getReference().findPipelineRunById(runId);
                    if (positionalPipelineRun.isPresent()) {
                        return positionalPipelineRun.get().getReference().streamResults()
                                .filter(indexedResult -> indexedResult.getReference().sameId(pipelineResult.getId()))
                                .findFirst()
                                .map(indexedResult -> {
                                    List<Object> formatArgs = new ArrayList<>(ImmutableList.of(positionalObjectiveSample.getPos(), positionalPipelineRun.get().getPos()));
                                    StringBuilder fieldFormat = new StringBuilder("objectiveSamples.%d.pipelineRuns.%d");
                                    for (IndexedReference<Integer, Integer> treePos : indexedResult.getPos()) {
                                        fieldFormat.append(".results.%d");
                                        formatArgs.add(treePos.getPos());
                                    }
                                    return String.format(fieldFormat.toString(), formatArgs.toArray());
                                })
                                ;
                    } else {
                        return Optional.<String>empty();
                    }
                })
                .ifPresent(fn -> updatedFields.add(Updates.set(fn, pipelineResult)));
        if (!updatedFields.isEmpty()) {
            UpdateOptions updateOptions = new UpdateOptions();
            updateOptions.upsert(false);
            update(getUpdateMatchCriteria(sample), Updates.combine(updatedFields), updateOptions);
        }
    }
}

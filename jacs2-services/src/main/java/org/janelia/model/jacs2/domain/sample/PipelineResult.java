package org.janelia.model.jacs2.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.jacs2.domain.FileReference;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.jacs2.domain.enums.FileType;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;
import org.janelia.model.jacs2.domain.interfaces.HasRelativeFiles;
import org.janelia.model.jacs2.EntityFieldValueHandler;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The result of some processing. May be nested if further processing is done on this result.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public class PipelineResult implements HasIdentifier, HasRelativeFiles {

    private Number id;
    private String name;
    private Date creationDate = new Date();
    private List<PipelineResult> results = new ArrayList<>();
    @JsonIgnore
    private transient PipelineResult parentResult;

    @JsonIgnore
    private HasRelativeFilesImpl relativeFilesImpl = new HasRelativeFilesImpl();

    @Override
    public Number getId() {
        return id;
    }

    public void setId(Number id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getFilepath() {
        return relativeFilesImpl.getFilepath();
    }

    public void setFilepath(String filepath) {
        relativeFilesImpl.setFilepath(filepath);
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public List<PipelineResult> getResults() {
        return results;
    }

    public void setResults(List<PipelineResult> results) {
        this.results = results;
    }

    public void addResult(PipelineResult result) {
        results.add(result);
    }

    @Override
    public Map<FileType, String> getFiles() {
        return relativeFilesImpl.getFiles();
    }

    @Override
    public String getFileName(FileType fileType) {
        return relativeFilesImpl.getFileName(fileType);
    }

    @Override
    public Map<String, EntityFieldValueHandler<?>> setFileName(FileType fileType, String fileName) {
        return relativeFilesImpl.setFileName(fileType, fileName);
    }

    @Override
    public Map<String, EntityFieldValueHandler<?>> removeFileName(FileType fileType) {
        return relativeFilesImpl.removeFileName(fileType);
    }

    @JsonProperty
    public List<FileReference> getDeprecatedFiles() {
        return relativeFilesImpl.getDeprecatedFiles();
    }

    public void setDeprecatedFiles(List<FileReference> deprecatedFiles) {
        this.relativeFilesImpl.setDeprecatedFiles(deprecatedFiles);
    }

    @JsonIgnore
    public PipelineResult getParentResult() {
        return parentResult;
    }

    /**
     * generate a stream of results from this node and its children.
     * @param index current node index
     * @param trace to the current node
     * @return a stream of indexed references by level and position
     */
    @SuppressWarnings("unchecked")
    public Stream<IndexedReference<PipelineResult, IndexedReference<Integer, Integer>[]>> streamResults(int level, int index, IndexedReference<Integer, Integer>[] trace) {
        IndexedReference<Integer, Integer>[] currentTrace;
        if (ArrayUtils.isEmpty(trace)) {
            currentTrace = (IndexedReference<Integer, Integer>[]) new IndexedReference[0];
        } else {
            currentTrace = trace;
        }
        IndexedReference<Integer, Integer>[] newTrace = ArrayUtils.add(currentTrace, new IndexedReference<>(level, index));
        return Stream.concat(
                Stream.of(new IndexedReference<>(this, newTrace)),
                IntStream.range(0, results.size())
                        .mapToObj(pos -> {
                            PipelineResult currentResult = results.get(pos);
                            currentResult.parentResult = this;
                            return new IndexedReference<>(currentResult, pos);
                        })
                        .flatMap(indexedResult -> {
                            return indexedResult.getReference().streamResults(level + 1, indexedResult.getPos(), newTrace);
                        })
        );
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .build();
    }
}

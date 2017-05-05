package org.janelia.jacs2.model.sage;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.model.BaseEntity;

public class ControlledVocabulary implements BaseEntity {
    private Integer vocabularyId;
    private Integer termId;
    private String vocabularyName;
    private String vocabularyTerm;

    public Integer getVocabularyId() {
        return vocabularyId;
    }

    public void setVocabularyId(Integer vocabularyId) {
        this.vocabularyId = vocabularyId;
    }

    public Integer getTermId() {
        return termId;
    }

    public void setTermId(Integer termId) {
        this.termId = termId;
    }

    public String getVocabularyName() {
        return vocabularyName;
    }

    public void setVocabularyName(String vocabularyName) {
        this.vocabularyName = vocabularyName;
    }

    public String getVocabularyTerm() {
        return vocabularyTerm;
    }

    public void setVocabularyTerm(String vocabularyTerm) {
        this.vocabularyTerm = vocabularyTerm;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

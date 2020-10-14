package org.janelia.jacs2.asyncservice.imagesearch;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AbstractCDMMetadata {
    private String id;
    private String libraryName;
    private String alignmentSpace;
    private String cdmPath;
    private String imageName;
    private String sampleRef;
    private String relatedImageRefId;
    @JsonProperty
    private Map<String, String> variants;

    public String getId() {
        return id;
    }

    public AbstractCDMMetadata setId(String id) {
        this.id = id;
        return this;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public AbstractCDMMetadata setLibraryName(String libraryName) {
        this.libraryName = libraryName;
        return this;
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public AbstractCDMMetadata setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
        return this;
    }

    public String getCdmPath() {
        return cdmPath;
    }

    public AbstractCDMMetadata setCdmPath(String cdmPath) {
        this.cdmPath = cdmPath;
        return this;
    }

    public String getImageName() {
        return imageName;
    }

    public AbstractCDMMetadata setImageName(String imageName) {
        this.imageName = imageName;
        return this;
    }

    public String getSampleRef() {
        return sampleRef;
    }

    public AbstractCDMMetadata setSampleRef(String sampleRef) {
        this.sampleRef = sampleRef;
        return this;
    }

    public String getRelatedImageRefId() {
        return relatedImageRefId;
    }

    public AbstractCDMMetadata setRelatedImageRefId(String relatedImageRefId) {
        this.relatedImageRefId = relatedImageRefId;
        return this;
    }

    public boolean hasVariant(String variant) {
        return variants != null && StringUtils.isNotBlank(variants.get(variant));
    }

    public String getVariant(String variant) {
        return variants.get(variant);
    }

    public void addVariant(String variant, String variantLocation) {
        if (StringUtils.isBlank(variantLocation)) {
            return;
        }
        if (StringUtils.isBlank(variant)) {
            throw new IllegalArgumentException("Variant type for " + variantLocation + " cannot be blank");
        }
        if (variants == null) {
            variants = new LinkedHashMap<>();
        }
        variants.put(variant, variantLocation);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("libraryName", libraryName)
                .append("alignmentSpace", alignmentSpace)
                .append("cdmPath", cdmPath)
                .append("imageName", imageName)
                .toString();
    }

}

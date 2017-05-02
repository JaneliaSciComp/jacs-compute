package org.janelia.jacs2.model.sage;

import org.janelia.jacs2.model.BaseEntity;

import java.util.Date;

public class SageLine implements BaseEntity {
    private Integer id;
    private String lab;
    private String name;
    private Integer geneId;
    private Integer organismId;
    private String genotype;
    private Date createDate;

}

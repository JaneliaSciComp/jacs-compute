package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.service.ServiceArgDescriptor;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JacsServiceArgs {

    private final List<String> listArgs;
    private final Map<String, Object> dictArgs;

    public JacsServiceArgs(List<String> listArgs, Map<String, Object> dictArgs) {
        this.listArgs = listArgs;
        this.dictArgs = dictArgs;
    }

    public List<String> getListArgs() {
        return listArgs;
    }

    public String[] getListArgsAsArray() {
        return listArgs.toArray(new String[0]);
    }

    public Map<String, Object> getDictArgs() {
        return dictArgs;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

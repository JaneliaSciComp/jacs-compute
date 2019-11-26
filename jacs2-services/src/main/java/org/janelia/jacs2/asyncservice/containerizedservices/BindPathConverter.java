package org.janelia.jacs2.asyncservice.containerizedservices;

import java.util.List;
import java.util.Optional;

import com.beust.jcommander.IStringConverter;
import com.google.common.base.Splitter;

import org.apache.commons.lang3.StringUtils;

class BindPathConverter implements IStringConverter<BindPath> {
    @Override
    public BindPath convert(String value) {
        BindPath bindPath = new BindPath();
        List<String> bindArgList = Splitter.on(':').trimResults().splitToList(value);
        bindPath.srcPath = getArgAt(bindArgList, 0)
                .orElse("");
        bindPath.targetPath = getArgAt(bindArgList, 1)
                .orElse("");
        bindPath.mountOpts = getArgAt(bindArgList, 2)
                .map(a -> {
                    if (a.equalsIgnoreCase("ro")) {
                        return "ro";
                    } else if (a.equalsIgnoreCase("rw")) {
                        return "rw";
                    } else {
                        return "";
                    }
                })
                .orElse("");
        return bindPath;
    }

    private Optional<String> getArgAt(List<String> bindSpecList, int index) {
        if (bindSpecList.size() > index && StringUtils.isNotBlank(bindSpecList.get(index))) {
            return Optional.of(bindSpecList.get(index));
        } else {
            return Optional.empty();
        }

    }
}

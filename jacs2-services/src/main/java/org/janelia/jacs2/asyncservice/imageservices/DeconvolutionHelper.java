package org.janelia.jacs2.asyncservice.imageservices;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class DeconvolutionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(DeconvolutionHelper.class);

    private final ObjectMapper objectMapper;

    DeconvolutionHelper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    String getTileDeconvFile(Map<String, Object> tileConfig, String deconvOutputDir) {
        return mapToResultName(getTileFile(tileConfig), deconvOutputDir, "_decon");
    }

    String getTileFile(Map<String, Object> tileConfig) {
        return (String) tileConfig.get("file");
    }

    String mapToResultName(String inputFile, String resultDir, String resultSuffix) {
        Path inputPath = Paths.get(inputFile);
        String inputFileName = FileUtils.getFileNameOnly(inputPath);
        String inputFileExt = FileUtils.getFileExtensionOnly(inputPath);
        if (StringUtils.isBlank(resultDir)) {
            return inputPath.getParent().resolve(inputFileName + resultSuffix + inputFileExt).toString();
        } else {
            return Paths.get(resultDir, inputFileName + resultSuffix + inputFileExt).toString();
        }
    }

    String mapToDeconvOutputDir(String inputFile) {
        Path inputFilePath = Paths.get(inputFile);
        final String deconvOutputDirname = "matlab_decon";
        if (inputFilePath.getParent() == null) {
            return deconvOutputDirname;
        } else {
            return inputFilePath.getParent().resolve(deconvOutputDirname).toString();
        }
    }

    <T> Optional<T> loadJsonConfiguration(String jsonFileName, TypeReference<T> typeReference) {
        if (StringUtils.isBlank(jsonFileName)) {
            return Optional.empty();
        } else {
            InputStream jsonInputStream;
            try {
                jsonInputStream = new FileInputStream(jsonFileName);
                return Optional.of(objectMapper.readValue(jsonInputStream, typeReference));
            } catch (Exception e) {
                LOG.error("Error reading json config from {}", jsonFileName, e);
                throw new IllegalStateException("Error reading json config from " + jsonFileName, e);
            }
        }
    }


    <T> void saveJsonConfiguration(T config, String jsonFileName) {
        OutputStream jsonOutputStream;
        try {
            jsonOutputStream = new FileOutputStream(jsonFileName);
            objectMapper.writeValue(jsonOutputStream, config);
        } catch (Exception e) {
            LOG.error("Error writing json config to {}", jsonFileName, e);
            throw new IllegalStateException("Error reading json config from " + jsonFileName, e);
        }
    }
}

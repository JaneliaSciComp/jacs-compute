package org.janelia.jacs2.asyncservice.imagesearch;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.gui.colordepth.ColorDepthMask;
import org.janelia.model.domain.gui.colordepth.ColorDepthMatch;
import org.janelia.model.domain.gui.colordepth.ColorDepthResult;
import org.janelia.model.domain.gui.colordepth.ColorDepthSearch;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wraps the ColorDepthFileSearch service with integration with the Workstation via the domain model.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthObjectSearch")
public class ColorDepthObjectSearch extends AbstractServiceProcessor<Boolean> {

    static class IntegratedColorDepthSearchArgs extends ServiceArgs {
        @Parameter(names = "-searchId", description = "GUID of the ColorDepthFileSearch object to use", required = true)
        Long searchId;
    }

    private final WrappedServiceProcessor<ColorDepthFileSearch, List<File>> colorDepthFileSearch;
    private final String rootPath;
    private final LegacyDomainDao dao;

    @Inject
    ColorDepthObjectSearch(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           LegacyDomainDao dao,
                           @StrPropertyValue(name = "service.colorDepthSearch.filepath") String rootPath,
                           ColorDepthFileSearch colorDepthFileSearch,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.dao = dao;
        this.colorDepthFileSearch = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, colorDepthFileSearch);
        this.rootPath = rootPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthObjectSearch.class, new IntegratedColorDepthSearchArgs());
    }

    @Override
    public ServiceResultHandler<Boolean> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Boolean>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return true;
            }

            @Override
            public Boolean collectResult(JacsServiceResult<?> depResults) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Boolean getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleProcessorResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<Boolean>> process(JacsServiceData jacsServiceData) {
        IntegratedColorDepthSearchArgs args = getArgs(jacsServiceData);

        logger.info("Executing ColorDepthSearch#{}", args.searchId);

        ColorDepthSearch search = dao.getDomainObject(jacsServiceData.getOwner(),
                ColorDepthSearch.class, args.searchId);

        if (search==null) {
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#"+args.searchId+" not found");
        }

        if (search.getDataSets().isEmpty()) {
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#"+args.searchId+" has no data sets defined");
        }

        List<ColorDepthMask> masks = dao.getDomainObjectsAs(search.getMasks(), ColorDepthMask.class);

        if (masks.isEmpty()) {
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#"+args.searchId+" has no masks defined");
        }

        Map<String, ColorDepthMask> maskMap =
                masks.stream().collect(Collectors.toMap(ColorDepthMask::getFilepath,
                        Function.identity()));

        String inputFiles = String.join(",", masks.stream()
                .map(ColorDepthMask::getFilepath)
                .collect(Collectors.toList()));

        List<String> maskThresholds = masks.stream()
                .map(ColorDepthMask::getMaskThreshold).map(i -> i.toString())
                .collect(Collectors.toList());

        Path alignPath = Paths.get(rootPath).resolve(search.getAlignmentSpace());
        String searchDirs = search.getDataSets().stream()
                .map(dataSet -> alignPath.resolve(dataSet).toString())
                .reduce((p1, p2) -> p1 + "," + p2)
                .orElse("");

        List<ServiceArg> serviceArgList = new ArrayList<>();
        serviceArgList.add(new ServiceArg("-inputFiles", inputFiles));
        serviceArgList.add(new ServiceArg("-searchDirs", searchDirs));
        serviceArgList.add(new ServiceArg("-maskThresholds", maskThresholds));

        if (search.getDataThreshold() != null) {
            serviceArgList.add(new ServiceArg("-dataThreshold", search.getDataThreshold()));
        }

        if (search.getPixColorFluctuation() != null) {
            serviceArgList.add(new ServiceArg("-pixColorFluctuation", search.getPixColorFluctuation()));
        }

        if (search.getPctPositivePixels() != null) {
            serviceArgList.add(new ServiceArg("-pctPositivePixels", search.getPctPositivePixels()));
        }

        return colorDepthFileSearch.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                    .description("Color depth search")
                    .build(),
                serviceArgList.toArray(new ServiceArg[serviceArgList.size()]))
            .thenApply((JacsServiceResult<List<File>> result) -> {

                if (result.getResult().isEmpty()) {
                    throw new ComputationException(jacsServiceData, "Color depth search encountered an error");
                }

                try {
                    ColorDepthResult colorDepthResult = new ColorDepthResult();

                    for (File resultsFile : result.getResult()) {

                        logger.info("Processing result file: {}", resultsFile);

                        String maskFile;
                        try (Scanner scanner = new Scanner(resultsFile)) {
                            maskFile = scanner.nextLine();

                            ColorDepthMask colorDepthMask = maskMap.get(maskFile);
                            if (colorDepthMask==null) {
                                throw new IllegalStateException("Unrecognized mask path: "+maskFile);
                            }
                            Reference maskRef = Reference.createFor(colorDepthMask);

                            while (scanner.hasNext()) {
                                String line = scanner.nextLine();
                                String[] s = line.split("\t");
                                int i = 0;
                                int score = Integer.parseInt(s[i++].trim());
                                double scorePct = Double.parseDouble(s[i++].trim());
                                String filepath = s[i++].trim();

                                ColorDepthMatch match = new ColorDepthMatch();
                                match.setMaskRef(maskRef);
                                match.setFilepath(filepath);
                                match.setScore(score);
                                match.setScorePercent(scorePct);
                                colorDepthResult.addMatch(match);
                            }
                        }
                        catch (IOException e) {
                            throw new ComputationException(jacsServiceData, e);
                        }
                    }

                    colorDepthResult = dao.save(jacsServiceData.getOwner(), colorDepthResult);
                    dao.addColorDepthSearchResult(jacsServiceData.getOwner(), search.getId(), colorDepthResult);
                } catch (Exception e) {
                    throw new ComputationException(jacsServiceData, e);
                }

                return new JacsServiceResult<>(jacsServiceData, Boolean.TRUE);
        });
    }

    private IntegratedColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new IntegratedColorDepthSearchArgs());
    }
}

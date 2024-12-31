package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.converters.IParameterSplitter;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.service.ServiceArgDescriptor;
import org.janelia.model.service.ServiceMetaData;

import jakarta.inject.Named;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ServiceArgs {

    public static class ServiceArgSplitter implements IParameterSplitter {

        private static final char QUOTE = '\'';
        private static final char ESCAPE = '\\';

        private final char separator;
        private final boolean keepQuote;

        public ServiceArgSplitter() {
            this(',', true);
        }

        private ServiceArgSplitter(char separator, boolean keepQuote) {
            this.separator = separator;
            this.keepQuote = keepQuote;
        }

        private enum ArgSplitterState {
            ParsingArg, ParsingQuotedArg, EscapeChar, ConsumeWhitespace
        }

        @Override
        public List<String> split(String value) {
            List<String> args = new LinkedList<>();
            ArgSplitterState state = ArgSplitterState.ParsingArg;
            ArgSplitterState escapedState = ArgSplitterState.ParsingArg;
            StringBuilder tokenBuilder = new StringBuilder();
            for (char currentChar : value.toCharArray()) {
                switch (state) {
                    case ParsingArg:
                        switch (currentChar) {
                            case ESCAPE:
                                escapedState = state;
                                state = ArgSplitterState.EscapeChar;
                                break;
                            case QUOTE:
                                state = ArgSplitterState.ParsingQuotedArg;
                                if (keepQuote) {
                                    tokenBuilder.append(currentChar);
                                }
                                break;
                            default:
                                if (currentChar == this.separator) {
                                    args.add(tokenBuilder.toString().trim());
                                    tokenBuilder.setLength(0);
                                    state = ArgSplitterState.ConsumeWhitespace;
                                } else {
                                    tokenBuilder.append(currentChar);
                                }
                                break;
                        }
                        break;
                    case ParsingQuotedArg:
                        switch (currentChar) {
                            case ESCAPE:
                                escapedState = state;
                                state = ArgSplitterState.EscapeChar;
                                break;
                            case QUOTE:
                                state = ArgSplitterState.ParsingArg;
                                if (keepQuote) {
                                    tokenBuilder.append(currentChar);
                                }
                                break;
                            default:
                                tokenBuilder.append(currentChar);
                                break;
                        }
                        break;
                    case EscapeChar:
                        tokenBuilder.append(currentChar);
                        state = escapedState;
                        break;
                    case ConsumeWhitespace:
                        if (Character.isWhitespace(currentChar)) {
                            break;
                        } else if (currentChar == this.separator) {
                            args.add(tokenBuilder.toString().trim());
                            tokenBuilder.setLength(0);
                        } else if (currentChar == ESCAPE) {
                            escapedState = ArgSplitterState.ParsingArg;
                            state = ArgSplitterState.EscapeChar;
                        } else if (currentChar == QUOTE) {
                            state = ArgSplitterState.ParsingQuotedArg;
                            if (keepQuote) {
                                tokenBuilder.append(currentChar);
                            }
                        } else {
                            state = ArgSplitterState.ParsingArg;
                            tokenBuilder.append(currentChar);
                        }
                        break;
                }
            }
            if (tokenBuilder.length() > 0) {
                args.add(tokenBuilder.toString().trim());
                tokenBuilder.setLength(0);
            }
            return args;
        }
    }

    public static List<String> concatArgs(List<List<String>> listOfArgs) {
        ServiceArgSplitter argSplitter = new ServiceArgSplitter(' ', false);
        return listOfArgs.stream()
                .flatMap(args -> args.stream())
                .flatMap(arg -> argSplitter.split(arg).stream())
                .collect(Collectors.toList());
    }

    public static <A extends ServiceArgs> A parse(String[] argsList, A args) {
        new JCommander(args).parse(argsList);
        return args;
    }

    private static <A extends ServiceArgs> void populateArgumentDescriptors(A args, ServiceMetaData smd) {
        JCommander jc = new JCommander(args);
        List<ParameterDescription> parameterDescriptiontList = jc.getParameters();
        smd.setServiceArgsObject(args);
        smd.setDescription(args.getServiceDescription());
        smd.setServiceArgDescriptors(parameterDescriptiontList.stream()
                .filter(pd -> !pd.isHelp())
                .map(pd -> {
                    Parameter parameterAnnotation = pd.getParameterAnnotation();
                    return new ServiceArgDescriptor(
                            pd.getParameterized(),
                            parameterAnnotation.names(),
                            pd.getDefault(),
                            parameterAnnotation.arity(),
                            parameterAnnotation.required(),
                            pd.getDescription()
                    );
                })
                .collect(Collectors.toList())
        );
    }

    public static <P extends ServiceProcessor, A extends ServiceArgs> ServiceMetaData getMetadata(Class<P> processorClass, A args) {
        Named namedAnnotation = processorClass.getAnnotation(Named.class);
        Preconditions.checkArgument(namedAnnotation != null);
        String serviceName = namedAnnotation.value();
        return createMetadata(serviceName, args);
    }

    static <A extends ServiceArgs> ServiceMetaData createMetadata(String serviceName, A args) {
        ServiceMetaData smd = new ServiceMetaData();
        smd.setServiceName(serviceName);
        populateArgumentDescriptors(args, smd);
        return smd;
    }

    private final String serviceDescription;

    @Parameter(description = "Remaining positional container arguments")
    private List<String> remainingArgs = new ArrayList<>();

    public ServiceArgs() {
        this(null);
    }

    public ServiceArgs(String serviceDescription) {
        this.serviceDescription = serviceDescription;
    }

    String getServiceDescription() {
        return serviceDescription;
    }

    public List<String> getRemainingArgs() {
        return remainingArgs;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

package com.company.pipeline.jobcatalog.dto;

import com.company.pipeline.jobcatalog.EtlFormulaHelp;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public record EtlFormulaHelpResponse(
        Long id,
        String functionName,
        String category,
        String syntax,
        String summary,
        String usageText,
        List<Example> examples,
        String notice
) {
    private static final TypeReference<List<Example>> EXAMPLES_TYPE = new TypeReference<>() {};

    public static EtlFormulaHelpResponse from(EtlFormulaHelp help, ObjectMapper objectMapper) {
        return new EtlFormulaHelpResponse(
                help.getId(),
                help.getFunctionName(),
                help.getCategory(),
                help.getSyntax(),
                help.getSummary(),
                help.getUsageText(),
                readExamples(help.getExamplesJson(), objectMapper),
                help.getNotice());
    }

    private static List<Example> readExamples(String value, ObjectMapper objectMapper) {
        try {
            return objectMapper.readValue(value, EXAMPLES_TYPE);
        } catch (Exception ex) {
            return List.of();
        }
    }

    public record Example(String formula, String description, String result) {
    }
}

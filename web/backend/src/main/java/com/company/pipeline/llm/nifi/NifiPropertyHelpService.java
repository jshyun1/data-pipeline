package com.company.pipeline.llm.nifi;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.llm.LlmProviderConfig;
import com.company.pipeline.llm.LlmProviderConfigRepository;
import com.company.pipeline.llm.client.LlmChatClient;
import com.company.pipeline.llm.client.LlmChatResult;
import com.company.pipeline.llm.client.LlmMessage;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class NifiPropertyHelpService {

    private static final int MAX_OUTPUT_TOKENS = 512;

    private final LlmProviderConfigRepository configRepository;
    private final PasswordCryptoService cryptoService;
    private final LlmChatClient chatClient;

    public NifiPropertyHelpService(LlmProviderConfigRepository configRepository, PasswordCryptoService cryptoService,
                                   LlmChatClient chatClient) {
        this.configRepository = configRepository;
        this.cryptoService = cryptoService;
        this.chatClient = chatClient;
    }

    public NifiPropertyHelpResponse interpret(NifiPropertyHelpRequest request) {
        LlmProviderConfig config = configRepository.findByActiveTrue()
                .orElseThrow(() -> new BusinessException(ErrorCode.LLM_NOT_ENABLED));
        String apiKey = config.getEncryptedApiKey() == null ? null : cryptoService.decrypt(config.getEncryptedApiKey());
        StringBuilder text = new StringBuilder();
        LlmChatResult result = chatClient.stream(
                config.getProvider(),
                config.effectiveBaseUrl(),
                config.getModelName(),
                apiKey,
                messages(request),
                MAX_OUTPUT_TOKENS,
                text::append
        );
        String answer = text.toString().strip();
        if (answer.isBlank()) {
            answer = result.text() == null ? "" : result.text().strip();
        }
        return new NifiPropertyHelpResponse(answer, config.getName(), config.getModelName());
    }

    private static List<LlmMessage> messages(NifiPropertyHelpRequest request) {
        return List.of(
                LlmMessage.system("""
                        당신은 Apache NiFi Processor 설정을 한국어로 설명하는 ETL 운영 도우미입니다.
                        입력으로 받은 NiFi property descriptor 원문을 기준으로만 설명하십시오.
                        답변은 사용자가 설정 화면에서 바로 이해할 수 있게 2~4문장으로 간결하게 작성하십시오.
                        원문에 없는 동작을 추측하지 말고, 필요한 경우 주의점만 짧게 덧붙이십시오.
                        """),
                LlmMessage.user("""
                        Processor: %s
                        Property: %s
                        Current Value: %s
                        Expression Language Scope: %s
                        Sensitive: %s
                        Controller Service Property: %s

                        NiFi descriptor:
                        %s
                        """.formatted(
                        blankToDash(request.processorType()),
                        request.propertyName(),
                        blankToDash(request.propertyValue()),
                        blankToDash(request.expressionLanguageScope()),
                        request.sensitive() == null ? "-" : request.sensitive(),
                        request.identifiesControllerService() == null ? "-" : request.identifiesControllerService(),
                        request.description()
                ))
        );
    }

    private static String blankToDash(String value) {
        return value == null || value.isBlank() ? "-" : value.strip();
    }
}

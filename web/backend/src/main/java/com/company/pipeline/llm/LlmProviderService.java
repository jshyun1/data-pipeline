package com.company.pipeline.llm;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.llm.client.LlmConnectionTester;
import com.company.pipeline.llm.dto.LlmProviderRequest;
import com.company.pipeline.llm.dto.LlmProviderResponse;
import com.company.pipeline.llm.dto.LlmStatusResponse;
import com.company.pipeline.llm.dto.LlmTestResult;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * GPT 설정 등록·수정·삭제·선택. 설계: docs/2026-09-13-gpt-settings-design.md 3~4절.
 *
 * <p>검증 규칙(서버가 최종 판정한다 - 화면 규칙은 편의일 뿐):
 * <ul>
 *   <li>외부 제공자는 API 키 필수. 무료 등급이어도 키 없이는 호출할 수 없다.</li>
 *   <li>내부(OpenAI 호환)는 API 주소 필수, 키 선택.</li>
 *   <li>이름 중복 불가.</li>
 *   <li>수정 시 키 칸이 비어 있으면 기존 키 유지(연결정보 비밀번호와 같은 규칙).</li>
 * </ul>
 */
@Service
@Transactional(readOnly = true)
public class LlmProviderService {

    private final LlmProviderConfigRepository repository;
    private final PasswordCryptoService cryptoService;
    private final LlmConnectionTester connectionTester;

    public LlmProviderService(LlmProviderConfigRepository repository, PasswordCryptoService cryptoService,
                              LlmConnectionTester connectionTester) {
        this.repository = repository;
        this.cryptoService = cryptoService;
        this.connectionTester = connectionTester;
    }

    public List<LlmProviderResponse> list() {
        return repository.findAllByOrderByIdAsc().stream().map(LlmProviderResponse::from).toList();
    }

    public LlmProviderResponse get(Long id) {
        return LlmProviderResponse.from(find(id));
    }

    /** 선택된 LLM 이 없으면 disabled. 속성창 GPT 탭·오류 분석 버튼의 표시 여부가 이걸 따른다. */
    public LlmStatusResponse status() {
        return repository.findByActiveTrue()
                .map(c -> new LlmStatusResponse(true, c.getName(), c.getDeployment(), c.getProvider(),
                        c.getModelName(), c.isMaskSensitive()))
                .orElseGet(LlmStatusResponse::disabled);
    }

    @Transactional
    public LlmProviderResponse create(LlmProviderRequest request, String actor) {
        String name = request.name().trim();
        if (repository.existsByName(name)) {
            throw conflict("이미 같은 이름의 GPT 설정이 있습니다: " + name);
        }
        validate(request, false);
        LlmProviderConfig config = LlmProviderConfig.create(name, request.provider(), actor);
        apply(config, request);
        applyApiKey(config, request.apiKey());
        return LlmProviderResponse.from(repository.save(config));
    }

    @Transactional
    public LlmProviderResponse update(Long id, LlmProviderRequest request, String actor) {
        LlmProviderConfig config = find(id);
        String name = request.name().trim();
        if (repository.existsByNameAndIdNot(name, id)) {
            throw conflict("이미 같은 이름의 GPT 설정이 있습니다: " + name);
        }
        // 기존 키는 «같은 제공자 · 같은 주소»일 때만 유지한다. 제공자가 바뀌면 다른 회사 키라 쓸 수 없고,
        // 주소만 바꾸고 키를 유지하면 저장된 키가 새 주소로 흘러간다(테스트와 같은 규칙).
        boolean sameDestination = config.getProvider() == request.provider()
                && java.util.Objects.equals(config.effectiveBaseUrl(), trimSlash(effectiveBaseUrl(request)));
        boolean keepsKey = !hasText(request.apiKey()) && config.getEncryptedApiKey() != null && sameDestination;
        validate(request, keepsKey);
        if (!sameDestination) {
            config.setEncryptedApiKey(null);
            config.setApiKeyHint(null);
        }
        config.setName(name);
        config.setProvider(request.provider());
        config.setDeployment(request.provider().getDeployment());
        apply(config, request);
        if (hasText(request.apiKey())) {
            applyApiKey(config, request.apiKey());
        }
        // 설정이 바뀌었으니 예전 테스트 결과는 더 이상 이 설정의 결과가 아니다. 도구 호출 감지 결과도 모델 · 주소에 묶여 있다.
        config.setToolSupport(null);
        config.setToolCheckedAt(null);
        config.setLastTestStatus(null);
        config.setLastTestMessage(null);
        config.setLastTestedAt(null);
        config.touch(actor);
        return LlmProviderResponse.from(repository.save(config));
    }

    @Transactional
    public void delete(Long id) {
        LlmProviderConfig config = find(id);
        if (config.isActive()) {
            throw conflict("선택된 GPT 설정은 삭제할 수 없습니다. 다른 항목을 선택하거나 선택을 해제한 뒤 삭제하십시오.");
        }
        repository.delete(config);
    }

    /**
     * 이 항목을 선택한다. 한 트랜잭션에서 <b>기존 해제 → 새 선택</b> 순서로 flush 한다.
     * 유니크 인덱스는 행마다 즉시 검사되므로 순서가 바뀌면 둘이 동시에 true 인 순간이 생겨 실패한다.
     * 관리자 둘이 동시에 다른 항목을 누르면 늦은 쪽이 인덱스에 걸린다 - 컨트롤러가 409 로 바꾼다.
     */
    @Transactional
    public LlmProviderResponse activate(Long id, String actor) {
        LlmProviderConfig target = find(id);
        if (target.isActive()) {
            return LlmProviderResponse.from(target);
        }
        repository.findByActiveTrue().ifPresent(current -> {
            current.setActive(false);
            current.touch(actor);
            repository.saveAndFlush(current);
        });
        target.setActive(true);
        target.touch(actor);
        return LlmProviderResponse.from(repository.saveAndFlush(target));
    }

    /** 선택 해제. 0개 선택을 허용하므로 필요하다(라디오는 스스로 해제할 수 없다). */
    @Transactional
    public void deactivate(String actor) {
        repository.findByActiveTrue().ifPresent(current -> {
            current.setActive(false);
            current.touch(actor);
            repository.saveAndFlush(current);
        });
    }

    /**
     * 저장 전 입력값으로 테스트. {@code id} 가 있고 키 칸이 비었으면 저장된 키를 쓴다(수정 화면).
     * 결과를 저장하지 않는다 - 아직 저장되지 않은 설정의 결과이기 때문이다.
     */
    public LlmTestResult testDraft(LlmProviderRequest request, Long id) {
        String apiKey = request.apiKey();
        if (!hasText(apiKey) && id != null) {
            LlmProviderConfig saved = find(id);
            // 저장된 키는 <b>저장된 주소로만</b> 보낸다. 주소 칸만 바꿔 테스트하면 저장된 키가 임의 주소로
            // 전송되므로(키 유출 경로), 주소나 제공자가 달라졌으면 키를 다시 입력해야 한다.
            boolean sameDestination = saved.getProvider() == request.provider()
                    && java.util.Objects.equals(saved.effectiveBaseUrl(), trimSlash(effectiveBaseUrl(request)));
            if (saved.getEncryptedApiKey() != null && sameDestination) {
                apiKey = cryptoService.decrypt(saved.getEncryptedApiKey());
            } else if (saved.getEncryptedApiKey() != null) {
                return new LlmTestResult("INVALID_CONFIG", false,
                        "제공자나 API 주소를 바꿨으면 API 키를 다시 입력해야 테스트할 수 있습니다.", null, 0, null);
            }
        }
        if (request.provider().getDeployment() == LlmDeployment.EXTERNAL && !hasText(apiKey)) {
            return new LlmTestResult("INVALID_CONFIG", false, "외부 제공자는 API 키가 필요합니다.", null, 0, null);
        }
        LlmTestResult result = connectionTester.test(request.provider(), effectiveBaseUrl(request), request.modelName().trim(), apiKey);
        if (!result.success()) {
            return result;
        }
        // 저장 전 테스트라 감지 결과는 저장하지 않고 안내 문구에만 붙인다.
        String support = connectionTester.probeTools(request.provider(), effectiveBaseUrl(request), request.modelName().trim(), apiKey);
        return withToolNote(result, support);
    }

    /** 저장된 항목 테스트. 결과를 목록의 «최근 테스트»에 남긴다. */
    @Transactional
    public LlmProviderResponse testSaved(Long id) {
        LlmProviderConfig config = find(id);
        String apiKey = config.getEncryptedApiKey() == null ? null : cryptoService.decrypt(config.getEncryptedApiKey());
        LlmTestResult result = connectionTester.test(config.getProvider(), config.effectiveBaseUrl(),
                config.getModelName(), apiKey);
        if (result.success()) {
            // 연결이 되면 도구 호출(function calling) 지원 여부도 같이 확인해 둔다 - 모델을 바꾼 뒤 테스트하면 자동으로 갱신된다.
            String support = connectionTester.probeTools(config.getProvider(), config.effectiveBaseUrl(),
                    config.getModelName(), apiKey);
            if (support != null) {
                config.recordToolSupport(support);
            }
            result = withToolNote(result, support);
        }
        config.setLastTestStatus(result.status());
        String message = result.message();
        config.setLastTestMessage(message.length() > 500 ? message.substring(0, 500) : message);
        config.setLastTestedAt(OffsetDateTime.now());
        return LlmProviderResponse.from(repository.save(config));
    }

    // ---------------------------------------------------------------- 내부

    private void validate(LlmProviderRequest request, boolean keepsExistingKey) {
        LlmProvider provider = request.provider();
        if (provider.getDeployment() == LlmDeployment.EXTERNAL && !hasText(request.apiKey()) && !keepsExistingKey) {
            throw invalid("외부 제공자는 무료 등급이어도 API 키가 필요합니다.");
        }
        if (provider.getDeployment() == LlmDeployment.INTERNAL && !hasText(request.baseUrl())) {
            throw invalid("내부 LLM 은 API 주소가 필요합니다. 예) http://10.0.0.5:8000/v1");
        }
        if (hasText(request.baseUrl())) {
            String url = request.baseUrl().trim().toLowerCase();
            if (!url.startsWith("http://") && !url.startsWith("https://")) {
                throw invalid("API 주소는 http:// 또는 https:// 로 시작해야 합니다.");
            }
        }
    }

    private void apply(LlmProviderConfig config, LlmProviderRequest request) {
        config.setBillingType(request.billingType());
        config.setBaseUrl(hasText(request.baseUrl()) ? request.baseUrl().trim() : null);
        config.setModelName(request.modelName().trim());
        config.setMaskSensitive(Boolean.TRUE.equals(request.maskSensitive()));
        config.setMaxInputChars(request.maxInputChars() != null
                ? request.maxInputChars() : request.provider().getDefaultMaxInputChars());
        config.setDescription(hasText(request.description()) ? request.description().trim() : null);
        if (hasText(request.toolCalling())) {
            config.setToolCalling(request.toolCalling());
        }
    }

    /** 연결 테스트 안내 문구 끝에 도구 호출 확인 결과를 붙인다. 확인하지 않은 제공자(null)는 그대로. */
    private static LlmTestResult withToolNote(LlmTestResult result, String support) {
        if (support == null) {
            return result;
        }
        String note = LlmProviderConfig.TOOL_SUPPORTED.equals(support)
                ? " · 도구 호출 지원(질문에 맞춰 job · 파이프라인을 직접 조회합니다)"
                : " · 도구 호출 미지원(질문 속 대상 상세만 붙여 답합니다)";
        return new LlmTestResult(result.status(), result.success(), result.message() + note,
                result.httpStatus(), result.durationMs(), result.detail());
    }

    private void applyApiKey(LlmProviderConfig config, String apiKey) {
        if (!hasText(apiKey)) {
            config.setEncryptedApiKey(null);
            config.setApiKeyHint(null);
            return;
        }
        String key = apiKey.trim();
        config.setEncryptedApiKey(cryptoService.encrypt(key));
        config.setApiKeyHint(key.length() <= 4 ? "…" : "…" + key.substring(key.length() - 4));
    }

    private String effectiveBaseUrl(LlmProviderRequest request) {
        return hasText(request.baseUrl()) ? request.baseUrl().trim() : request.provider().getDefaultBaseUrl();
    }

    private static String trimSlash(String url) {
        if (url == null) {
            return null;
        }
        String out = url;
        while (out.endsWith("/")) {
            out = out.substring(0, out.length() - 1);
        }
        return out;
    }

    private LlmProviderConfig find(Long id) {
        return repository.findById(id).orElseThrow(() -> new BusinessException(ErrorCode.LLM_PROVIDER_NOT_FOUND));
    }

    private static BusinessException invalid(String message) {
        return new BusinessException(ErrorCode.VALIDATION_ERROR, message);
    }

    private static BusinessException conflict(String message) {
        return new BusinessException(ErrorCode.LLM_PROVIDER_CONFLICT, message);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}

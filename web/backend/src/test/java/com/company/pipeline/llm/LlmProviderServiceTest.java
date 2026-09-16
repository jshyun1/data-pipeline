package com.company.pipeline.llm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.llm.client.LlmConnectionTester;
import com.company.pipeline.llm.dto.LlmProviderRequest;
import com.company.pipeline.llm.dto.LlmProviderResponse;
import com.company.pipeline.llm.dto.LlmTestResult;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LlmProviderServiceTest {

    @Mock
    private LlmProviderConfigRepository repository;
    @Mock
    private PasswordCryptoService cryptoService;
    @Mock
    private LlmConnectionTester tester;

    private LlmProviderService service;

    @BeforeEach
    void setUp() {
        service = new LlmProviderService(repository, cryptoService, tester);
    }

    private static LlmProviderRequest external(String apiKey) {
        return new LlmProviderRequest("운영용 Claude", LlmProvider.ANTHROPIC, LlmBillingType.PAID,
                null, "claude-model", apiKey, true, null, null);
    }

    private static LlmProviderRequest internal(String baseUrl) {
        return new LlmProviderRequest("내부 Llama", LlmProvider.OPENAI_COMPATIBLE, LlmBillingType.FREE,
                baseUrl, "llama-3-70b", null, false, null, null);
    }

    private static LlmProviderConfig saved(Long id, LlmProvider provider, String baseUrl, String encryptedKey, boolean active) {
        LlmProviderConfig c = LlmProviderConfig.create("saved-" + id, provider, "admin");
        c.setId(id);
        c.setBillingType(LlmBillingType.PAID);
        c.setBaseUrl(baseUrl);
        c.setModelName("m");
        c.setMaskSensitive(true);
        c.setMaxInputChars(1000);
        c.setEncryptedApiKey(encryptedKey);
        c.setActive(active);
        return c;
    }

    @Test
    void create_external_requiresApiKey_evenWhenFree() {
        LlmProviderRequest free = new LlmProviderRequest("gemini", LlmProvider.GEMINI, LlmBillingType.FREE,
                null, "gemini-model", "  ", true, null, null);

        assertThatThrownBy(() -> service.create(free, "admin"))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("API 키");
        verify(repository, never()).save(any());
    }

    @Test
    void create_internal_requiresBaseUrl_butNotKey() {
        assertThatThrownBy(() -> service.create(internal(null), "admin"))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("API 주소");

        when(repository.save(any(LlmProviderConfig.class))).thenAnswer(inv -> inv.getArgument(0));
        LlmProviderResponse ok = service.create(internal("http://10.0.0.5:8000/v1/"), "admin");
        assertThat(ok.deployment()).isEqualTo(LlmDeployment.INTERNAL);
        assertThat(ok.apiKeySet()).isFalse();
        assertThat(ok.maxInputChars()).isEqualTo(LlmProvider.OPENAI_COMPATIBLE.getDefaultMaxInputChars());
        verifyNoInteractions(cryptoService);
    }

    @Test
    void create_encryptsKey_andExposesOnlyHint() {
        when(cryptoService.encrypt("sk-secret-abcd")).thenReturn("cipher");
        when(repository.save(any(LlmProviderConfig.class))).thenAnswer(inv -> inv.getArgument(0));

        LlmProviderResponse res = service.create(external("sk-secret-abcd"), "admin");

        assertThat(res.apiKeySet()).isTrue();
        assertThat(res.apiKeyHint()).isEqualTo("…abcd");
        assertThat(res.deployment()).isEqualTo(LlmDeployment.EXTERNAL);
        assertThat(res.toString()).doesNotContain("sk-secret").doesNotContain("cipher");
    }

    @Test
    void create_rejectsDuplicateName() {
        when(repository.existsByName("운영용 Claude")).thenReturn(true);

        assertThatThrownBy(() -> service.create(external("k"), "admin"))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("이미 같은 이름");
    }

    @Test
    void update_blankKey_keepsStoredKey_whenSameProviderAndUrl() {
        LlmProviderConfig current = saved(1L, LlmProvider.ANTHROPIC, null, "old-cipher", false);
        when(repository.findById(1L)).thenReturn(Optional.of(current));
        when(repository.save(any(LlmProviderConfig.class))).thenAnswer(inv -> inv.getArgument(0));

        LlmProviderResponse res = service.update(1L, external(""), "admin");

        assertThat(res.apiKeySet()).isTrue();
        assertThat(current.getEncryptedApiKey()).isEqualTo("old-cipher");
    }

    @Test
    void update_changedUrlWithBlankKey_dropsStoredKey() {
        // 주소만 바꾸고 키를 유지하면 저장된 키가 새 주소로 흘러간다 - 외부는 키 재입력을 요구한다.
        LlmProviderConfig current = saved(1L, LlmProvider.ANTHROPIC, null, "old-cipher", false);
        when(repository.findById(1L)).thenReturn(Optional.of(current));
        LlmProviderRequest moved = new LlmProviderRequest("운영용 Claude", LlmProvider.ANTHROPIC, LlmBillingType.PAID,
                "https://attacker.example", "claude-model", "", true, null, null);

        assertThatThrownBy(() -> service.update(1L, moved, "admin"))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("API 키");
    }

    @Test
    void testDraft_doesNotSendStoredKey_toDifferentUrl() {
        LlmProviderConfig current = saved(1L, LlmProvider.ANTHROPIC, null, "old-cipher", false);
        when(repository.findById(1L)).thenReturn(Optional.of(current));
        LlmProviderRequest moved = new LlmProviderRequest("운영용 Claude", LlmProvider.ANTHROPIC, LlmBillingType.PAID,
                "https://attacker.example", "claude-model", null, true, null, null);

        LlmTestResult result = service.testDraft(moved, 1L);

        assertThat(result.success()).isFalse();
        assertThat(result.status()).isEqualTo("INVALID_CONFIG");
        verifyNoInteractions(tester);
        verify(cryptoService, never()).decrypt(anyString());
    }

    @Test
    void testDraft_reusesStoredKey_forSameDestination() {
        LlmProviderConfig current = saved(1L, LlmProvider.ANTHROPIC, null, "old-cipher", false);
        when(repository.findById(1L)).thenReturn(Optional.of(current));
        when(cryptoService.decrypt("old-cipher")).thenReturn("plain-key");
        LlmTestResult ok = new LlmTestResult("SUCCESS", true, "ok", 200, 10, null);
        when(tester.test(eq(LlmProvider.ANTHROPIC), eq("https://api.anthropic.com"), eq("claude-model"), eq("plain-key")))
                .thenReturn(ok);

        assertThat(service.testDraft(external(null), 1L).success()).isTrue();
    }

    @Test
    void activate_clearsCurrentBeforeSettingTarget() {
        LlmProviderConfig current = saved(1L, LlmProvider.ANTHROPIC, null, "c", true);
        LlmProviderConfig target = saved(2L, LlmProvider.OPENAI_COMPATIBLE, "http://x/v1", null, false);
        when(repository.findById(2L)).thenReturn(Optional.of(target));
        when(repository.findByActiveTrue()).thenReturn(Optional.of(current));
        when(repository.saveAndFlush(any(LlmProviderConfig.class))).thenAnswer(inv -> inv.getArgument(0));

        LlmProviderResponse res = service.activate(2L, "admin");

        assertThat(res.active()).isTrue();
        assertThat(current.isActive()).isFalse();
        InOrder order = inOrder(repository);
        order.verify(repository).saveAndFlush(current);
        order.verify(repository).saveAndFlush(target);
    }

    @Test
    void delete_rejectsActiveProvider() {
        when(repository.findById(1L)).thenReturn(Optional.of(saved(1L, LlmProvider.ANTHROPIC, null, "c", true)));

        assertThatThrownBy(() -> service.delete(1L))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("선택된");
        verify(repository, never()).delete(any());
    }

    @Test
    void status_disabled_whenNothingSelected() {
        when(repository.findByActiveTrue()).thenReturn(Optional.empty());

        assertThat(service.status().enabled()).isFalse();
    }
}

package com.company.pipeline.authz.portal;

import com.company.pipeline.authz.provisioning.AirflowUserSyncService;
import com.company.pipeline.authz.provisioning.NifiTenantSyncService;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.user.dto.LoginResponse;
import com.company.pipeline.user.dto.UserResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * MSA 포털(app02) 전용 토큰 교환 종단점 (P4.5).
 *
 * <p>{@code portal.integration.enabled=true} 일 때만 빈이 만들어진다. 기본값 off 에서는
 * 이 경로가 아예 없어 404 가 되므로, 로컬·타 폐쇄망 단독 배포에는 아무 영향이 없다.
 *
 * <p>호출자는 app02 서버다(브라우저가 아니다). 신뢰 근거가 내부망 + 공유 서명키이므로
 * <b>이 종단점을 외부에 노출하지 말 것</b>.
 */
@RestController
@RequestMapping("/api/authz")
@ConditionalOnProperty(name = "portal.integration.enabled", havingValue = "true")
public class PortalExchangeController {

    private final PortalTokenExchangeService exchangeService;
    private final NifiTenantSyncService nifiSync;
    private final AirflowUserSyncService airflowSync;

    public PortalExchangeController(PortalTokenExchangeService exchangeService,
                                    NifiTenantSyncService nifiSync,
                                    AirflowUserSyncService airflowSync) {
        this.exchangeService = exchangeService;
        this.nifiSync = nifiSync;
        this.airflowSync = airflowSync;
    }

    /** 포털 JWT. 본문 대신 Authorization 헤더로 보내도 된다(app02 프록시가 그 형태로 갖고 있다). */
    public record ExchangeRequest(String token) {
    }

    @PostMapping("/portal-exchange")
    public ApiResponse<LoginResponse> exchange(
            @RequestBody(required = false) ExchangeRequest request, HttpServletRequest servletRequest) {
        String token = request != null ? request.token() : null;
        if (token == null || token.isBlank()) {
            token = servletRequest.getHeader(HttpHeaders.AUTHORIZATION);
        }
        LoginResponse response = exchangeService.exchange(token);

        // NiFi/Airflow 콘솔은 사용자 본인 신원으로 대행 접속하므로(P5b), 그쪽에도 계정이 있어야
        // 열린다. 관리자가 만든 계정은 AdminAccountController 가 같은 방식으로 동기화하는데,
        // 포털 경유 사용자는 관리자가 존재조차 모르는 채로 들어오므로 여기서 맞춰 준다.
        //
        // 매 교환마다 부르는 이유: syncUser 는 현재 권한에 맞추는 멱등 연산이라, 한 번 실패했거나
        // 권한이 바뀌어도 다음 접속에서 저절로 복구된다(교환은 app02 가 캐시하므로 요청마다가
        // 아니라 사용자당 토큰 만료 주기마다 일어난다). 실패는 삼켜지므로 로그인을 막지 않는다.
        // identity-sync 가 off 면 syncUser 가 즉시 return 하여 아무 일도 하지 않는다.
        UserResponse user = response.user();
        nifiSync.syncUser(user.userId());
        airflowSync.syncUser(user.userId(), user.userNm(), user.email());

        return ApiResponse.success(response);
    }
}

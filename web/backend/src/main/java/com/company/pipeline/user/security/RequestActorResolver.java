package com.company.pipeline.user.security;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * 지금 요청을 보낸 주체 - 등록자 · 요청자로 남길 값(2026-10-13).
 *
 * <p>업무 API 는 아직 permitAll 이라(인가 강제 off) 컨트롤러에 사람 신원이 없을 수 있다. 그래서 등록 · 제어 이력이 늘 비어 있었다
 * (CDC 파이프라인 등록자 · 제어 이력 요청자 전부). 판단 순서:
 * <ol>
 *   <li>로그인 사용자(JWT) - 사용자 ID</li>
 *   <li>서비스 토큰({@code X-Service-Token})이 맞는 호출 - Airflow DAG 다. 호출한 DAG 가 {@code X-Requested-By} 로 밝힌
 *       이름이 있으면 «airflow:그 이름», 없으면 «airflow». 서비스 토큰이 맞을 때만 그 머리글을 믿는다.</li>
 *   <li>둘 다 아니면 모른다(empty).</li>
 * </ol>
 */
@Component
public class RequestActorResolver {

    static final String SERVICE_TOKEN_HEADER = "X-Service-Token";
    static final String REQUESTED_BY_HEADER = "X-Requested-By";
    private static final String SERVICE_ACTOR = "airflow";
    /** 컬럼 길이(created_by · requested_by 100자)에 맞춘다. */
    private static final int MAX_LENGTH = 100;
    private static final Set<String> READ_METHODS = Set.of("GET", "HEAD", "OPTIONS");

    private final String serviceToken;

    public RequestActorResolver(@Value("${authz.service-token:}") String serviceToken) {
        this.serviceToken = serviceToken;
    }

    /** 지금 요청의 주체. 요청 밖(스케줄러 등)이거나 신원이 없으면 empty. */
    public Optional<String> current() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof AppUser user && StringUtils.hasText(user.getUserId())) {
            return Optional.of(user.getUserId());
        }
        HttpServletRequest request = currentRequest();
        if (request != null && StringUtils.hasText(serviceToken)
                && serviceToken.equals(request.getHeader(SERVICE_TOKEN_HEADER))) {
            String declared = request.getHeader(REQUESTED_BY_HEADER);
            String actor = StringUtils.hasText(declared) ? SERVICE_ACTOR + ":" + declared.trim() : SERVICE_ACTOR;
            return Optional.of(actor.length() > MAX_LENGTH ? actor.substring(0, MAX_LENGTH) : actor);
        }
        return Optional.empty();
    }

    /**
     * 변경 요청(POST · PUT · DELETE · PATCH)의 주체. 조회 중에 남는 기록에 «보던 사람»이 요청자로 찍히지 않게 조회는 empty 다.
     */
    public Optional<String> currentForChange() {
        HttpServletRequest request = currentRequest();
        if (request == null || READ_METHODS.contains(request.getMethod())) {
            return Optional.empty();
        }
        return current();
    }

    /**
     * 등록자로 남길 주체. 모르면 등록하지 않는다 - 등록자는 반드시 알 수 있어야 한다(2026-10-13 요청).
     *
     * @param what 화면에 보일 대상 이름(«CDC 파이프라인», «워크플로우»)
     */
    public String requireForRegistration(String what) {
        return current().orElseThrow(() -> new BusinessException(ErrorCode.UNAUTHORIZED,
                "로그인 정보가 없어 " + what + " 등록자를 알 수 없습니다. 다시 로그인한 뒤 등록해 주세요."));
    }

    private static HttpServletRequest currentRequest() {
        return RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes
                ? attributes.getRequest() : null;
    }
}

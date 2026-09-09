package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.pipeline.dto.PipelineGroupRequest;
import com.company.pipeline.pipeline.dto.PipelineGroupResponse;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

/**
 * CDC 파이프라인 그룹(관리 화면 트리의 폴더) CRUD.
 *
 * <p>계층이라 실수 하나가 트리를 통째로 망가뜨린다. 그래서 세 가지를 서비스에서 막는다 -
 * 같은 부모 밑 이름 중복, 자기 자신(또는 자기 하위)을 부모로 삼는 순환, 비어 있지 않은
 * 그룹 삭제. 이름 중복은 DB 유니크 인덱스가 한 번 더 막지만, 사용자에게는 여기서
 * 읽을 수 있는 메시지로 알려준다.
 */
@Service
public class PipelineGroupService {

    private final PipelineGroupRepository groupRepository;
    private final PipelineDefinitionRepository pipelineRepository;

    public PipelineGroupService(PipelineGroupRepository groupRepository,
                                PipelineDefinitionRepository pipelineRepository) {
        this.groupRepository = groupRepository;
        this.pipelineRepository = pipelineRepository;
    }

    @Transactional(readOnly = true)
    public List<PipelineGroupResponse> list() {
        return groupRepository.findAllByOrderByNameAsc().stream()
                .map(PipelineGroupResponse::from)
                .toList();
    }

    @Transactional
    public PipelineGroupResponse create(PipelineGroupRequest request) {
        String name = normalizedName(request.name());
        Long parentId = request.parentId();
        requireExists(parentId);
        requireNameFree(parentId, name, null);
        return PipelineGroupResponse.from(groupRepository.save(new PipelineGroup(parentId, name)));
    }

    /**
     * 이름과 위치를 고친다. parentId 를 안 보내면(=null) «최상단으로 옮긴다»는 뜻이라,
     * 이름만 바꾸려는 호출이 그룹을 최상단으로 끌어올리지 않도록 화면에서 현재 값을 함께 보낸다.
     */
    @Transactional
    public PipelineGroupResponse update(Long id, PipelineGroupRequest request) {
        PipelineGroup group = require(id);
        String name = normalizedName(request.name());
        Long parentId = request.parentId();
        if (id.equals(parentId) || descendantIds(id).contains(parentId)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "그룹을 자기 자신이나 자기 하위 그룹으로 옮길 수 없습니다.");
        }
        requireExists(parentId);
        requireNameFree(parentId, name, id);
        group.setParentId(parentId);
        group.setName(name);
        return PipelineGroupResponse.from(group);
    }

    /**
     * 그룹 삭제. 하위 그룹이나 파이프라인이 남아 있으면 거절한다.
     *
     * <p>딸린 것까지 지우면 파이프라인이 «폴더를 지웠더니 같이 사라지는» 사고가 된다.
     * CDC 파이프라인은 커넥터까지 딸린 실체라 그렇게 지워지면 안 된다. 옮기고 나서 지운다.
     */
    @Transactional
    public void delete(Long id) {
        require(id);
        if (!groupRepository.findByParentId(id).isEmpty()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "하위 그룹이 있어 삭제할 수 없습니다. 하위 그룹을 먼저 옮기거나 지워주세요.");
        }
        long pipelines = pipelineRepository.countByGroupId(id);
        if (pipelines > 0) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "이 그룹에 파이프라인 " + pipelines + "건이 있어 삭제할 수 없습니다. 먼저 다른 그룹으로 옮겨주세요.");
        }
        groupRepository.deleteById(id);
    }

    /** 그룹 지정이 유효한지 본다(파이프라인 생성·수정에서 쓴다). */
    @Transactional(readOnly = true)
    public void requireExists(Long groupId) {
        if (groupId != null && !groupRepository.existsById(groupId)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "그룹을 찾을 수 없습니다.");
        }
    }

    private PipelineGroup require(Long id) {
        return groupRepository.findById(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR, "그룹을 찾을 수 없습니다."));
    }

    private String normalizedName(String raw) {
        String name = raw == null ? "" : raw.trim();
        if (!StringUtils.hasText(name)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "그룹 이름을 입력해주세요.");
        }
        return name;
    }

    private void requireNameFree(Long parentId, String name, Long selfId) {
        // parent_id 가 NULL 인 형제는 findByParentId(null) 로 못 찾는다(SQL 에서 = NULL 은 늘 거짓).
        // 최상단 그룹끼리도 이름이 겹치면 안 되므로 전체에서 걸러 본다.
        boolean taken = siblingsOf(parentId).stream()
                .anyMatch(g -> g.getName().equalsIgnoreCase(name) && !g.getId().equals(selfId));
        if (taken) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "같은 위치에 같은 이름의 그룹이 이미 있습니다.");
        }
    }

    private List<PipelineGroup> siblingsOf(Long parentId) {
        return parentId == null
                ? groupRepository.findAllByOrderByNameAsc().stream()
                        .filter(g -> g.getParentId() == null)
                        .toList()
                : groupRepository.findByParentId(parentId);
    }

    /** 순환 방지용. id 아래에 달린 모든 그룹 id. */
    private Set<Long> descendantIds(Long id) {
        List<PipelineGroup> all = groupRepository.findAllByOrderByNameAsc();
        Set<Long> found = new HashSet<>();
        Set<Long> frontier = new HashSet<>(Set.of(id));
        while (!frontier.isEmpty()) {
            Set<Long> parents = frontier;      // 람다가 잡을 수 있도록 매 바퀴 새 참조로 고정한다
            Set<Long> next = all.stream()
                    .filter(g -> g.getParentId() != null && parents.contains(g.getParentId()))
                    .map(PipelineGroup::getId)
                    .filter(candidate -> !found.contains(candidate))
                    .collect(Collectors.toSet());
            found.addAll(next);
            frontier = next;
        }
        return found;
    }
}

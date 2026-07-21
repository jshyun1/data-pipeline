package com.company.pipeline.logpipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * pipeline-api와 filebeat 컨테이너가 공유하는 볼륨(filebeat-inputs)에 파이프라인별
 * Filebeat input YAML 파일을 쓰고 지우는, 이 기능에서 유일하게 파일시스템을 만지는 클래스.
 * filebeat.yml의 filebeat.config.inputs.reload가 이 디렉터리를 주기적으로 스캔하므로
 * 컨테이너 재시작 없이 파일 추가/삭제만으로 Filebeat 입력이 동적으로 반영된다.
 */
@Component
@EnableConfigurationProperties(FilebeatProperties.class)
public class FilebeatInputFileService {

    private final Path inputsDir;

    public FilebeatInputFileService(FilebeatProperties properties) {
        this.inputsDir = Path.of(properties.inputsDir());
    }

    public void write(Long pipelineId, String yaml) {
        try {
            Files.createDirectories(inputsDir);
            Files.writeString(inputsDir.resolve(fileName(pipelineId)), yaml);
        } catch (IOException ex) {
            throw new BusinessException(ErrorCode.FILEBEAT_CONFIG_ERROR,
                    "Filebeat 입력 파일 쓰기 실패: " + ex.getMessage());
        }
    }

    public void delete(Long pipelineId) {
        try {
            Files.deleteIfExists(inputsDir.resolve(fileName(pipelineId)));
        } catch (IOException ex) {
            throw new BusinessException(ErrorCode.FILEBEAT_CONFIG_ERROR,
                    "Filebeat 입력 파일 삭제 실패: " + ex.getMessage());
        }
    }

    private String fileName(Long pipelineId) {
        return "pipeline-" + pipelineId + ".yml";
    }
}

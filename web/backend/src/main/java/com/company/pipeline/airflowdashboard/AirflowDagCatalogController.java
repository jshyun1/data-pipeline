package com.company.pipeline.airflowdashboard;

import com.company.pipeline.common.ApiResponse;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/airflow/dag-catalog")
public class AirflowDagCatalogController {

    private final AirflowDagCatalogRepository repository;

    public AirflowDagCatalogController(AirflowDagCatalogRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public ApiResponse<List<Response>> list() {
        return ApiResponse.success(repository
                .findByEnabledTrueOrderByBusinessGroupAscBusinessFolderAscSortOrderAscDisplayNameAsc()
                .stream().map(Response::from).toList());
    }

    public record Response(
            String dagId,
            String businessGroup,
            String businessFolder,
            String displayName,
            String description) {

        static Response from(AirflowDagCatalog catalog) {
            return new Response(
                    catalog.getDagId(),
                    catalog.getBusinessGroup(),
                    catalog.getBusinessFolder(),
                    catalog.getDisplayName(),
                    catalog.getDescription());
        }
    }
}

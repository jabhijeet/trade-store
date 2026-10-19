package com.tradestore.config;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tradestore.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

/**
 * The OpenAPI document and Swagger UI are served without a token and describe every REST endpoint with the bearer
 * scheme, while the endpoints themselves still require one.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class OpenApiDocsIT {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void apiDocs_ArePublicAndListEndpointsWithBearerScheme() throws Exception {
        mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/api/v1/trades'].post").exists())
                .andExpect(jsonPath("$.paths['/api/v1/trades'].get").exists())
                .andExpect(jsonPath("$.paths['/api/v1/trades/{tradeId}']").exists())
                .andExpect(jsonPath("$.components.securitySchemes['%s'].scheme", OpenApiConfig.BEARER_SCHEME)
                        .value("bearer"))
                .andExpect(jsonPath("$.security[0]['%s']", OpenApiConfig.BEARER_SCHEME).exists());
    }

    @Test
    void swaggerUi_IsPublic() throws Exception {
        mockMvc.perform(get("/swagger-ui/index.html"))
                .andExpect(status().isOk());
    }

    @Test
    void apiEndpoints_StillRequireToken() throws Exception {
        mockMvc.perform(get("/api/v1/trades"))
                .andExpect(status().isUnauthorized());
    }
}

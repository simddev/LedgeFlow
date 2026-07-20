package dev.simd.ledgeflow;

import dev.simd.ledgeflow.service.JwtService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"account.events", "account.alerts", "account.events.DLT"})
class SecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JwtService jwtService;

    @Test
    void unauthenticated_request_is_rejected() throws Exception {
        mockMvc.perform(get("/accounts/" + UUID.randomUUID()))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void user_role_cannot_access_admin_rebuild() throws Exception {
        String token = jwtService.generateToken("user", "USER");
        mockMvc.perform(post("/admin/rebuild")
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

    @Test
    void admin_role_can_access_admin_rebuild() throws Exception {
        String token = jwtService.generateToken("admin", "ADMIN");
        mockMvc.perform(post("/admin/rebuild")
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk());
    }

    @Test
    void malformed_jwt_is_rejected() throws Exception {
        mockMvc.perform(get("/accounts/" + UUID.randomUUID())
                        .header("Authorization", "Bearer not.a.real.token"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void blank_bearer_token_is_rejected() throws Exception {
        mockMvc.perform(get("/accounts/" + UUID.randomUUID())
                        .header("Authorization", "Bearer "))
                .andExpect(status().isUnauthorized());
    }
}

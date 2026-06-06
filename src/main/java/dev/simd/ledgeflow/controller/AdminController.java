package dev.simd.ledgeflow.controller;

import dev.simd.ledgeflow.service.AdminService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private final AdminService adminService;

    public AdminController(AdminService adminService) {
        this.adminService = adminService;
    }

    @PostMapping("/rebuild")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void rebuild() {
        adminService.rebuild();
    }
}

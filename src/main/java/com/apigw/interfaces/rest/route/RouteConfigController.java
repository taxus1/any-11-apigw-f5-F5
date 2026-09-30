package com.apigw.proxy.interfaces.rest.route;

import com.apigw.common.Result;
import com.apigw.proxy.route.RouteConfigCoordinator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * 路由热刷新排障接口。建议只暴露到运维网段；认证可复用网关现有的管理侧鉴权。
 */
@RestController
@RequestMapping("/api/gateway/route-config")
public class RouteConfigController {

    private final RouteConfigCoordinator coordinator;

    public RouteConfigController(RouteConfigCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    /** 手动通知三台重新拉最新配置，并返回可查询的命令号。 */
    @PostMapping("/refresh")
    public Mono<Result<Map<String, String>>> forceRefresh() {
        return coordinator.requestForceRefresh()
                .map(commandId -> Result.ok(Map.of("commandId", commandId)));
    }

    /** 查手动刷新命令：每台是否成功、当前 revision、checksum、没回应的是哪台。 */
    @GetMapping("/refresh/{commandId}")
    public Mono<Result<RouteConfigCoordinator.RefreshStatus>> refreshStatus(@PathVariable String commandId) {
        return coordinator.refreshStatus(commandId).map(Result::ok);
    }

    /** 三台总览：在线、激活版本、备齐版本、上次刷新时间和最近错误。 */
    @GetMapping("/status")
    public Mono<Result<RouteConfigCoordinator.RouteConfigStatus>> status() {
        return coordinator.status().map(Result::ok);
    }

    /** 本机当前版本，便于直接打某台定位。 */
    @GetMapping("/local")
    public Mono<Result<RouteConfigCoordinator.RouteConfigStatus>> local() {
        return coordinator.status().map(Result::ok);
    }
}

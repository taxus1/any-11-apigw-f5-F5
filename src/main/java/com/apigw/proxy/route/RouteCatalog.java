package com.apigw.proxy.route;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.infrastructure.store.RouteStore;
import com.apigw.proxy.config.GatewayProxyProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 转发侧路由目录：对匹配器只暴露当前已激活版本。
 *
 * 真正的版本读取、多实例 barrier、Pub/Sub 和心跳都在 {@link RouteConfigCoordinator}。
 * 这里保留旧组件名，减少转发链路改动；刷新失败时不换 coordinator 内的旧引用，所以不会把现有流量打挂。
 */
@Slf4j
@Component
public class RouteCatalog {

    private final RouteConfigCoordinator coordinator;

    @Autowired
    public RouteCatalog(RouteConfigCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    /** 仅供不需要多实例 barrier 的单测使用；生产装配只走 coordinator。 */
    public RouteCatalog(RouteStore routeStore, GatewayProxyProperties properties) {
        this.coordinator = new RouteConfigCoordinator(routeStore, null,
                new com.fasterxml.jackson.databind.ObjectMapper(),
                new com.apigw.proxy.config.RouteConfigProperties(
                        "test", 1, properties.routeRefreshInterval(),
                        properties.routeRefreshInterval(), properties.routeRefreshInterval(),
                        properties.routeRefreshInterval(), properties.routeRefreshInterval(),
                        properties.routeRefreshInterval(), properties.routeRefreshInterval(), null));
    }

    /** 当前已激活、且「启用 + 至少一条条件」的不可变路由列表。 */
    public Mono<List<GatewayRoute>> routes() {
        return coordinator.waitForRoutingGate().then(Mono.defer(() -> {
            if (!coordinator.hasActiveConfig()) {
                return Mono.error(new IllegalStateException("路由配置尚未加载成功"));
            }
            return Mono.just(coordinator.activeRoutes().stream()
                    .filter(r -> Integer.valueOf(1).equals(r.getEnabled()))
                    .filter(r -> r.getConditions() != null && !r.getConditions().isEmpty())
                    .toList());
        }));
    }

    /** 强制从 Redis 拉最新；未达到三机统一切换条件前，activeRoutes 仍是旧版本。 */
    public Mono<List<GatewayRoute>> refresh() {
        return coordinator.refreshNow().map(routes -> routes.stream()
                .filter(r -> Integer.valueOf(1).equals(r.getEnabled()))
                .filter(r -> r.getConditions() != null && !r.getConditions().isEmpty())
                .toList());
    }

    @EventListener(RoutesChangedEvent.class)
    public void onRoutesChanged(RoutesChangedEvent event) {
        coordinator.onLocalRoutesChanged(event).subscribe(
                config -> log.info("路由变更已进入协同刷新 revision={}", config.revision()),
                err -> log.warn("路由变更后协同刷新失败，继续沿用旧快照：{}", err.toString()));
    }

    @Scheduled(fixedDelayString = "${apigw.proxy.route-config.poll-interval-ms:5000}")
    public void scheduledRefresh() {
        coordinator.syncLatest().subscribe(
                config -> log.debug("路由定时同步完成 revision={}", config.revision()),
                err -> log.debug("路由定时同步失败，沿用旧快照：{}", err.toString()));
    }

    public void warmUp() {
        // RouteConfigCoordinator 在 ApplicationReadyEvent 中统一预热，避免重复打 Redis。
    }
}

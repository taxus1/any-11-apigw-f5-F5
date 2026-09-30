package com.apigw.proxy.route;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GatewayRule;
import com.apigw.proxy.config.GatewayProxyProperties;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * RouteCatalog 只做过滤与委托；版本一致性由 RouteConfigCoordinator 测试覆盖。
 */
class RouteCatalogTest {

    private final GatewayProxyProperties props =
            new GatewayProxyProperties(Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofHours(1));

    private GatewayRule pathPrefix(String v) {
        return GatewayRule.create(null, "PATH_PREFIX", null, v, 1);
    }

    private GatewayRoute route(String no, Integer enabled, int conditionCount) {
        GatewayRoute r = GatewayRoute.create(no, no, "http://h:1", enabled, null);
        List<GatewayRule> conds = conditionCount == 0 ? List.of() : List.of(pathPrefix("/" + no + "/"));
        r.replaceRules(conds, List.of());
        return r;
    }

    private RouteCatalog catalogOf(GatewayRoute... routes) {
        RouteConfigCoordinator coordinator = mock(RouteConfigCoordinator.class);
        when(coordinator.hasActiveConfig()).thenReturn(true);
        when(coordinator.waitForRoutingGate()).thenReturn(Mono.empty());
        when(coordinator.activeRoutes()).thenReturn(List.of(routes));
        when(coordinator.refreshNow()).thenReturn(Mono.fromSupplier(() -> List.of(routes)));
        return new RouteCatalog(coordinator);
    }

    @Test
    void routes_areReadFromCoordinatorSnapshot() {
        RouteCatalog catalog = catalogOf(route("a", 1, 1));
        assertEquals(1, catalog.routes().block().size());
        verify(coordinatorOf(catalog), times(1)).activeRoutes();
    }

    @Test
    void changedEvent_delegatesImmediateCoordinatorRefresh() {
        RouteConfigCoordinator coordinator = mock(RouteConfigCoordinator.class);
        when(coordinator.onLocalRoutesChanged(org.mockito.ArgumentMatchers.any()))
                .thenReturn(Mono.just(new RouteConfigCoordinator.LoadedConfig(
                        1, "x", List.of(), java.time.Instant.now(), "TEST")));
        RouteCatalog catalog = new RouteCatalog(coordinator);

        catalog.onRoutesChanged(RoutesChangedEvent.created("new"));

        verify(coordinator).onLocalRoutesChanged(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void noConfig_propagatesError() {
        RouteConfigCoordinator coordinator = mock(RouteConfigCoordinator.class);
        when(coordinator.hasActiveConfig()).thenReturn(false);
        when(coordinator.waitForRoutingGate()).thenReturn(Mono.empty());
        RouteCatalog catalog = new RouteCatalog(coordinator);

        assertThrows(RuntimeException.class, () -> catalog.routes().block());
    }

    @Test
    void disabledAndConditionlessRoutes_areExcluded() {
        RouteCatalog catalog = catalogOf(
                route("enabled-with-conds", 1, 1),
                route("disabled", 0, 1),
                route("enabled-no-conds", 1, 0));

        List<GatewayRoute> routes = catalog.routes().block();
        assertEquals(1, routes.size());
        assertEquals("enabled-with-conds", routes.get(0).getRouteNo());
    }

    private RouteConfigCoordinator coordinatorOf(RouteCatalog catalog) {
        try {
            java.lang.reflect.Field f = RouteCatalog.class.getDeclaredField("coordinator");
            f.setAccessible(true);
            return (RouteConfigCoordinator) f.get(catalog);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}

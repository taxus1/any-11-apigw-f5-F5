package com.apigw.proxy;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.infrastructure.store.RouteStore;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

/**
 * 转发链路测试用的「内存版」路由仓库：直接拿一组 GatewayRoute 当快照源，
 * 想模拟「新建/删除路由热生效」时替换列表后让 catalog 刷新即可，不依赖 Redis。
 */
class InMemoryRouteStore extends RouteStore {

    private volatile List<GatewayRoute> routes = new ArrayList<>();

    InMemoryRouteStore() {
        super(null, new com.fasterxml.jackson.databind.ObjectMapper());
    }

    void setRoutes(List<GatewayRoute> routes) {
        this.routes = new ArrayList<>(routes);
    }

    @Override
    public Flux<GatewayRoute> findAll() {
        return Flux.defer(() -> Flux.fromIterable(routes));
    }

    @Override
    public Mono<GatewayRoute> create(GatewayRoute route) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Mono<GatewayRoute> update(GatewayRoute route) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Mono<Long> delete(String routeNo, Integer expectVersion) {
        throw new UnsupportedOperationException();
    }
}

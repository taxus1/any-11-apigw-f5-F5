package com.apigw.application.route;

import com.apigw.common.exception.BizException;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GatewayRule;
import com.apigw.domain.route.GrayGroup;
import com.apigw.infrastructure.store.RouteStore;
import com.apigw.infrastructure.store.dto.PageResult;
import com.apigw.infrastructure.store.dto.RouteView;
import com.apigw.proxy.route.RoutesChangedEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * 路由配置的应用服务：编排用例、守住一致性边界。
 *
 * 业务规则放在 {@link GatewayRoute} 聚合里，这里只做：
 * - 用例编排：新建走 store 的原子占位；修改把编号钉死成路径参数、版本比对交给 store；
 * - 分页与关键字过滤（读多写少，过滤在内存里做，避免为模糊查引入额外索引）；
 * - 列表需要的子项计数，在这里一次算好，不让前端挨个再查。
 */
@Service
public class GatewayRouteAppService {

    private static final int MAX_PAGE_SIZE = 200;
    private static final int DEFAULT_PAGE_SIZE = 20;

    private final RouteStore routeStore;
    private final ApplicationEventPublisher eventPublisher;

    public GatewayRouteAppService(RouteStore routeStore, ApplicationEventPublisher eventPublisher) {
        this.routeStore = routeStore;
        this.eventPublisher = eventPublisher;
    }

    /**
     * 新建：聚合校验在 toDomain/assemble 阶段已完成，这里分配内部 id 后交给 store 原子占位。
     * 编号查重和写入在 Redis 里是一个原子动作，并发建同号只有一个能成。
     */
    public Mono<GatewayRoute> create(GatewayRoute input) {
        if (input.getId() == null) {
            input.setId(UUID.randomUUID().toString().replace("-", ""));
        }
        // 新建不接受客户端自带版本，一律从 0 开始；store.create 里也会再钉一次做双保险
        input.setVersion(0);
        return routeStore.create(input)
                .doOnSuccess(r -> eventPublisher.publishEvent(RoutesChangedEvent.created(r.getRouteNo())));
    }

    /**
     * 修改：编号以路径参数为准，body 里编号不一致直接拦（聚合兜底「建后不可改」）。
     * 路由存在性、版本冲突检测都在 store 的锁内完成，避免查改之间再被插队。
     */
    public Mono<GatewayRoute> update(String routeNo, GatewayRoute input) {
        // 编号以路径参数为准，body 里编号不一致直接拦（聚合兜底「建后不可改」）；
        // id 不接受客户端指定，store 里沿用现有 id
        input.assignRouteNo(routeNo);
        input.setId(null);
        return routeStore.update(input)
                .doOnSuccess(r -> eventPublisher.publishEvent(RoutesChangedEvent.updated(r.getRouteNo())));
    }

    public Mono<GatewayRoute> detail(String routeNo) {
        return routeStore.findByRouteNo(routeNo)
                .switchIfEmpty(Mono.error(new BizException(404, "路由不存在：" + routeNo)));
    }

    /** 删除：不存在、版本旧了都会拿到明确的失败结果，绝不静默当成功。 */
    public Mono<Void> delete(String routeNo, Integer expectVersion) {
        return routeStore.delete(routeNo, expectVersion)
                .then()
                .doOnSuccess(v -> eventPublisher.publishEvent(RoutesChangedEvent.deleted(routeNo)));
    }

    /**
     * 分页列表：先按关键字过滤（编号或名称，忽略大小写的包含匹配），再按编号稳定排序，最后切片。
     * 列表项带 conditionCount / actionCount，前端不用二次查询。
     * 每页条数封顶 {@link #MAX_PAGE_SIZE}，防止一次拖全量。
     */
    public Mono<PageResult<RouteView>> page(int pageNum, int pageSize, String keyword) {
        int pn = pageNum < 1 ? 1 : pageNum;
        int ps = pageSize < 1 ? DEFAULT_PAGE_SIZE : Math.min(pageSize, MAX_PAGE_SIZE);
        String kw = RouteStore.normalizeKeyword(keyword);

        return routeStore.findAll()
                .filter(r -> kw == null || matches(r, kw))
                .sort(Comparator.comparing(GatewayRoute::getRouteNo))
                .map(RouteView::of)
                .collectList()
                .map(list -> {
                    long total = list.size();
                    int from = Math.min((pn - 1) * ps, list.size());
                    int to = Math.min(from + ps, list.size());
                    List<RouteView> slice = list.subList(from, to);
                    return new PageResult<>(slice, total, pn, ps);
                });
    }

    private boolean matches(GatewayRoute r, String kw) {
        String lower = kw.toLowerCase(Locale.ROOT);
        return (r.getRouteNo() != null && r.getRouteNo().toLowerCase(Locale.ROOT).contains(lower))
                || (r.getName() != null && r.getName().toLowerCase(Locale.ROOT).contains(lower));
    }

    /** 把一份外部输入整理成聚合（聚合的全部校验在这里同步完成）。 */
    public GatewayRoute assemble(String routeNo, String name, String upstream, Integer enabled,
                                 Integer authRequired, String remark, Integer version,
                                 List<GatewayRule> conditions, List<GatewayRule> actions,
                                 List<GrayGroup> grayGroups) {
        GatewayRoute route = GatewayRoute.create(routeNo, name, upstream, enabled, remark);
        route.changeAuthRequired(authRequired);
        // version 原样带入：修改时必须等于当前版本；为空会在 store 被拒
        route.setVersion(version);
        route.replaceRules(conditions, actions);
        // 灰度分组：null/空 = 不做灰度；非空走聚合的整组校验（权重和必须恰好为 100）
        route.replaceGrayGroups(grayGroups);
        return route;
    }
}

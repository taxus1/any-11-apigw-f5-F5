package com.apigw.infrastructure.store;

import com.apigw.common.exception.BizException;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GrayGroup;
import com.apigw.domain.route.GatewayRule;
import com.apigw.domain.route.RuleTypes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 路由配置在 Redis 里的读写封装。
 *
 * 存储结构（一条路由 + 它的全部条件/动作 = Hash 里的一个 field）：
 *   key   = apigw:routes               （Hash）
 *   field = routeNo
 *   value = 该路由及其全部子项的 JSON
 *
 * 这样「整树保存、整树删除」天然就是原子的：
 * - 保存只有一次 HSET/HSETNX，删只有一次 HDEL，Redis 单命令不会插进半截，
 *   不可能出现「主记录进了、子记录没进」的残缺路由，也不需要手工回滚；
 * - 读出来永远是一整份完整配置，没有无主的子记录可留。
 *
 * 编号占用用 HSETNX 原子判定；同一条路由的「读版本→写回」用 Lua 脚本串行完成，
 * 同时推进全局 revision。revision 不是路由 JSON 里的字段，而是旁边的元数据，
 * 因此原有路由 Hash 的形状不变，后台/历史数据不需要迁移。
 *
 * 这里只负责序列化与并发控制，业务规则在 {@link GatewayRoute} 聚合里。
 */
@Component
public class RouteStore {

    public static final String ROUTES_KEY = "apigw:routes";
    public static final String REVISION_KEY = "apigw:routes:revision";

    private static final RedisScript<List> CREATE_SCRIPT = new DefaultRedisScript<>("""
            local exists = redis.call('HEXISTS', KEYS[1], ARGV[1])
            if exists == 1 then
                return {'BUSY', '-1', '-1', '-1'}
            end
            local revision = redis.call('INCR', KEYS[2])
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
            return {'OK', '0', tostring(revision), '-1'}
            """, List.class);

    private static final RedisScript<List> UPDATE_SCRIPT = new DefaultRedisScript<>("""
            local current = redis.call('HGET', KEYS[1], ARGV[1])
            if not current then
                return {'NOT_FOUND', '-1', '-1', '-1'}
            end
            local expected = tonumber(ARGV[3])
            local currentVersion = cjson.decode(current)['version'] or 0
            if currentVersion ~= expected then
                return {'CONFLICT', tostring(currentVersion), '-1', '-1'}
            end
            cjson.encode_empty_table_as_object(false)
            local nextVersion = currentVersion + 1
            local nextJson = cjson.decode(ARGV[2])
            nextJson['version'] = nextVersion
            local currentId = cjson.decode(current)['id']
            nextJson['id'] = currentId
            local revision = redis.call('INCR', KEYS[2])
            redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(nextJson))
            return {'OK', tostring(nextVersion), tostring(revision), tostring(currentId)}
            """, List.class);

    private static final RedisScript<List> DELETE_SCRIPT = new DefaultRedisScript<>("""
            local current = redis.call('HGET', KEYS[1], ARGV[1])
            if not current then
                return {'NOT_FOUND', '-1', '-1', '-1'}
            end
            if ARGV[3] ~= '' then
                local expected = tonumber(ARGV[3])
                local currentVersion = cjson.decode(current)['version'] or 0
                if expected ~= nil and currentVersion ~= expected then
                    return {'CONFLICT', tostring(currentVersion), '-1', '-1'}
                end
            end
            local revision = redis.call('INCR', KEYS[2])
            redis.call('HDEL', KEYS[1], ARGV[1])
            return {'OK', '-1', tostring(revision), '-1'}
            """, List.class);

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final ObjectMapper canonicalMapper;

    public RouteStore(ReactiveStringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.canonicalMapper = objectMapper.copy()
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    /** 读全部路由（含子项）。 */
    public Flux<GatewayRoute> findAll() {
        return redis.opsForHash().values(ROUTES_KEY)
                .map(v -> deserialize(v.toString()));
    }

    /** 按编号读一条。 */
    public Mono<GatewayRoute> findByRouteNo(String routeNo) {
        return redis.opsForHash().get(ROUTES_KEY, routeNo)
                .map(v -> deserialize(v.toString()));
    }

    /** 当前全局配置版本号；没有任何管理写入时可能为空。 */
    public Mono<Long> currentRevision() {
        return redis.opsForValue().get(REVISION_KEY)
                .map(Long::valueOf)
                .defaultIfEmpty(0L);
    }

    /**
     * 新建：HSETNX、写入全局 revision 在同一个 Lua 脚本里完成。
     *
     * 「先查有没有、再写」在两步之间有窗口，两个人同时建同一个编号会双双成功；
     * Lua 把查重、占位和版本推进合成 Redis 里的一个原子动作，谁先谁赢，后来者直接收占用错误。
     * 停用的路由也占着编号（field 还在），删除才真正释放编号。
     */
    public Mono<GatewayRoute> create(GatewayRoute route) {
        route.setVersion(0);
        return executeWrite(CREATE_SCRIPT, route.getRouteNo(), serialize(route), "")
                .flatMap(result -> {
                    if ("BUSY".equals(result.status())) {
                        return Mono.error(new BizException(
                                "路由编号已被占用（停用的路由也占号）：" + route.getRouteNo()));
                    }
                    route.setVersion((int) result.version());
                    return Mono.just(route);
                });
    }

    /**
     * 修改（整树覆盖）。必须显式带上读取时拿到的 version：
     * - Lua 脚本保证读当前版本、比较、整树覆盖和推进全局 revision 是一个临界区；
     * - version 比对保证旧版本提交进不来，写入后版本 +1。
     * 两个人同时改同一条，后到的拿到的版本已变，会收到明确的「你这份旧了」。
     */
    public Mono<GatewayRoute> update(GatewayRoute route) {
        Integer expectVersion = route.getVersion();
        if (expectVersion == null) {
            // 错误经 Mono 发出而非同步抛出：这是响应式契约，调用方的 onErrorResume 才接得住
            return Mono.error(new BizException(
                    "修改必须带上读取时拿到的版本号 version（首版也要显式传 0），用于并发冲突检测"));
        }
        return executeWrite(UPDATE_SCRIPT, route.getRouteNo(), serialize(route), expectVersion.toString())
                .flatMap(result -> switch (result.status()) {
                    case "NOT_FOUND" -> Mono.error(new BizException(404,
                            "路由不存在：" + route.getRouteNo()));
                    case "CONFLICT" -> Mono.error(versionConflict(result.version(), expectVersion));
                    default -> {
                        route.setId(result.id());
                        route.setVersion((int) result.version());
                        yield Mono.just(route);
                    }
                });
    }

    /**
     * 删除：存在性检查、版本检查、HDEL 与全局 revision 推进在同一个 Lua 脚本里。
     * 条件与动作跟主记录在同一个 field 里，一次 HDEL 整树清掉，不会留无主子记录。
     */
    public Mono<Long> delete(String routeNo, Integer expectVersion) {
        String expected = expectVersion == null ? "" : expectVersion.toString();
        return executeWrite(DELETE_SCRIPT, routeNo, "", expected)
                .flatMap(result -> switch (result.status()) {
                    case "NOT_FOUND" -> Mono.error(new BizException(404,
                            "路由不存在，删除未执行：" + routeNo));
                    case "CONFLICT" -> Mono.error(versionConflict(result.version(),
                            expectVersion == null ? -1 : expectVersion));
                    default -> Mono.just(result.revision());
                });
    }

    @SuppressWarnings("unchecked")
    private Mono<WriteResult> executeWrite(RedisScript<List> script, String routeNo, String json,
                                          String expectVersion) {
        return redis.execute(script, List.of(ROUTES_KEY, REVISION_KEY),
                        List.of(routeNo, json, expectVersion))
                .next()
                .map(raw -> {
                    List<Object> values = (List<Object>) raw;
                    String status = values.get(0).toString();
                    long version = Long.parseLong(values.get(1).toString());
                    long revision = Long.parseLong(values.get(2).toString());
                    String id = values.get(3).toString();
                    if ("-1".equals(id)) {
                        id = null;
                    }
                    return new WriteResult(status, version, revision, id);
                });
    }

    private BizException versionConflict(long currentVersion, int expectVersion) {
        return new BizException(409, "你这份配置已经旧了（当前版本 " + currentVersion
                + "，你手上是 " + expectVersion + "），请重新拉取后再提交");
    }

    /** 用一份完整配置替换整个 Hash；保留给导入/修复场景，普通管理接口不走这条非原子入口。 */
    public Mono<Void> replaceAll(Map<String, GatewayRoute> routes) {
        Map<String, String> raw = new TreeMap<>();
        routes.forEach((k, v) -> raw.put(k, serialize(v)));
        return redis.delete(ROUTES_KEY)
                .then(redis.opsForHash().putAll(ROUTES_KEY, raw))
                .then(redis.opsForValue().increment(REVISION_KEY))
                .then();
    }

    /** 对一份完整路由树算稳定校验值；条件、动作、灰度组任一变化都会变。 */
    public String checksum(GatewayRoute route) {
        try {
            byte[] bytes = canonicalMapper.writeValueAsBytes(Dto.from(route));
            return sha256(bytes);
        } catch (Exception e) {
            throw new BizException("路由校验值计算失败：" + e.getMessage());
        }
    }

    public String checksum(List<GatewayRoute> routes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            routes.stream()
                    .sorted(Comparator.comparing(GatewayRoute::getRouteNo))
                    .forEach(route -> {
                        digest.update(checksum(route).getBytes(StandardCharsets.UTF_8));
                        digest.update((byte) '\n');
                    });
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception e) {
            throw new BizException("路由快照校验值计算失败：" + e.getMessage());
        }
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    public record WriteResult(String status, long version, long revision, String id) {
    }

    // ---- 序列化：不用 Java 原生序列化，存 JSON，便于人工排查与后续版本迁移 ----

    public String serialize(GatewayRoute route) {
        try {
            return objectMapper.writeValueAsString(Dto.from(route));
        } catch (Exception e) {
            throw new BizException("路由序列化失败：" + e.getMessage());
        }
    }

    public GatewayRoute deserialize(String json) {
        try {
            Dto dto = objectMapper.readValue(json, new TypeReference<Dto>() {
            });
            return dto.toDomain();
        } catch (Exception e) {
            throw new BizException("路由反序列化失败：" + e.getMessage());
        }
    }

    /** 落 Redis 的形状：字段与领域对象一致，直接复用领域模型的公开 getter/setter。 */
    public static class Dto {
        public String id;
        public String routeNo;
        public String name;
        public String upstream;
        public Integer enabled;
        public Integer authRequired;
        public String remark;
        public Integer version;
        public List<RuleDto> conditions = new ArrayList<>();
        public List<RuleDto> actions = new ArrayList<>();
        /** 灰度分组：旧配置 JSON 没这个字段时为 null，toDomain 按「无灰度」落，向后兼容。 */
        public List<GrayGroupDto> grayGroups;

        static Dto from(GatewayRoute r) {
            Dto d = new Dto();
            d.id = r.getId();
            d.routeNo = r.getRouteNo();
            d.name = r.getName();
            d.upstream = r.getUpstream();
            d.enabled = r.getEnabled();
            d.authRequired = r.getAuthRequired();
            d.remark = r.getRemark();
            d.version = r.getVersion();
            d.conditions = r.getConditions().stream().map(RuleDto::from).toList();
            d.actions = r.getActions().stream().map(RuleDto::from).toList();
            d.grayGroups = r.getGrayGroups() == null ? List.of()
                    : r.getGrayGroups().stream().map(GrayGroupDto::from).toList();
            return d;
        }

        GatewayRoute toDomain() {
            GatewayRoute r = GatewayRoute.create(routeNo, name, upstream, enabled, remark);
            // 旧配置里没有这个字段：null 进 changeAuthRequired 按 0（开放）落，向后兼容
            r.changeAuthRequired(authRequired);
            r.setId(id);
            r.setVersion(version == null ? 0 : version);
            r.replaceRules(
                    conditions == null ? List.of() : conditions.stream().map(RuleDto::toDomain).toList(),
                    actions == null ? List.of() : actions.stream().map(RuleDto::toDomain).toList());
            // 灰度分组整体走聚合校验（权重和=100、组名/标记唯一、上游合法）；
            // 旧 JSON 缺字段（null）= 无灰度，全量流量回主上游
            r.replaceGrayGroups(
                    grayGroups == null ? List.of() : grayGroups.stream().map(GrayGroupDto::toDomain).toList());
            r.getConditions().forEach(x -> x.setRuleKind(RuleTypes.KIND_CONDITION));
            r.getActions().forEach(x -> x.setRuleKind(RuleTypes.KIND_ACTION));
            return r;
        }
    }

    /** 子项在 Redis 里的形状。 */
    public static class RuleDto {
        public String id;
        public String stage;
        public String type;
        public String name;
        public String value;
        public Integer sortNo;

        static RuleDto from(GatewayRule g) {
            RuleDto d = new RuleDto();
            d.id = g.getId();
            d.stage = g.getStage();
            d.type = g.getType();
            d.name = g.getName();
            d.value = g.getValue();
            d.sortNo = g.getSortNo();
            return d;
        }

        GatewayRule toDomain() {
            return GatewayRule.create(stage, type, name, value, sortNo);
        }
    }

    /** 灰度分组在 Redis 里的形状：组名/上游/权重/标记值原样存取，校验在聚合层。 */
    public static class GrayGroupDto {
        public String groupName;
        public String upstream;
        public Integer weight;
        public List<String> tags;

        static GrayGroupDto from(GrayGroup g) {
            GrayGroupDto d = new GrayGroupDto();
            d.groupName = g.getGroupName();
            d.upstream = g.getUpstream();
            d.weight = g.getWeight();
            d.tags = g.getTags() == null ? List.of() : new ArrayList<>(g.getTags());
            return d;
        }

        GrayGroup toDomain() {
            return GrayGroup.create(groupName, upstream, weight,
                    tags == null ? List.of() : new ArrayList<>(tags));
        }
    }

    /** 按顺序号排好的子项（对外返回时统一排序）。 */
    public static List<GatewayRule> sorted(List<GatewayRule> rules) {
        List<GatewayRule> copy = new ArrayList<>(rules);
        copy.sort(Comparator.comparing(GatewayRule::getSortNo, Comparator.nullsLast(Integer::compareTo)));
        return copy;
    }

    /** 供管理接口做「按编号模糊找」用：把 keyword 归一化，空串视为不过滤。 */
    public static String normalizeKeyword(String keyword) {
        if (keyword == null) {
            return null;
        }
        String k = keyword.trim();
        return k.isEmpty() ? null : k;
    }

    /** 编号集合，供统计用。 */
    public Mono<Set<String>> allRouteNos() {
        return redis.opsForHash().keys(ROUTES_KEY).collectList()
                .map(list -> new java.util.HashSet<>(list.stream().map(Object::toString).toList()));
    }
}

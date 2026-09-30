package com.apigw.proxy.route;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.infrastructure.store.RouteStore;
import com.apigw.proxy.config.RouteConfigProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 多实例路由版本协调器。
 *
 * 读路径始终只读本机 {@link ActiveSnapshot} 里的不可变 List；任何一次刷新都先在后台把
 * 「全部路由 + 每条路由的条件/动作/灰度组」读齐，再用一个 volatile 引用整体替换。
 * 因此请求不会看到主表改了、子项仍旧的半棵树，也不会在同一次请求里中途换版本。
 *
 * 多实例采用「先推、后拉兜底」：后台写库后发布 Redis Pub/Sub；推送丢失、实例短暂断连时，
 * 实例按 pollInterval 主动拉 revision。新版本先标记为 STAGED，等固定成员（三台）都备齐后，
 * 写入同一个未来激活点；有一台没跟上就不切，继续用旧快照。
 */
@Slf4j
@Component
public class RouteConfigCoordinator {

    public static final String CHANGED_CHANNEL = "apigw:route-config:changed";
    public static final String ACTIVATION_CHANNEL = "apigw:route-config:activation";
    public static final String COMMAND_CHANNEL = "apigw:route-config:commands";

    private static final String NODE_PREFIX = "apigw:route-config:nodes:";
    private static final String STAGED_PREFIX = "apigw:route-config:staged:";
    private static final String FENCE_KEY = "apigw:route-config:fence";
    private static final String COMMAND_PREFIX = "apigw:route-config:refresh-commands:";
    private static final String ACK_PREFIX = "apigw:route-config:refresh-acks:";

    private static final RedisScript<List> FENCE_SCRIPT = new DefaultRedisScript<>("""
            local currentRevision = tonumber(redis.call('HGET', KEYS[1], 'revision') or '0')
            local revision = tonumber(ARGV[1])
            if revision < currentRevision then
                return {'NEWER'}
            end
            redis.call('DEL', KEYS[2])
            redis.call('HSET', KEYS[1], 'revision', revision,
                       'checksum', ARGV[2],
                       'fenceAt', ARGV[3],
                       'commitDeadline', ARGV[4],
                       'memberHash', ARGV[5],
                       'required', ARGV[7],
                       'state', 'FENCING',
                       'updatedAt', ARGV[6])
            redis.call('EXPIRE', KEYS[1], 604800)
            return {'SET'}
            """, List.class);

    private static final RedisScript<List> FENCE_JOIN_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('HGET', KEYS[1], 'revision') ~= ARGV[1] then
                return {'STALE'}
            end
            redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
            redis.call('EXPIRE', KEYS[2], 3600)
            return {'JOINED'}
            """, List.class);

    private static final RedisScript<List> FENCE_COMMIT_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('HGET', KEYS[1], 'revision') ~= ARGV[1] then
                return {'STALE'}
            end
            if redis.call('HGET', KEYS[1], 'state') ~= 'FENCING' then
                return {'DONE'}
            end
            local joined = tonumber(redis.call('HLEN', KEYS[2]) or '0')
            local required = tonumber(ARGV[4])
            if joined < required then
                return {'WAIT', tostring(joined)}
            end
            redis.call('HSET', KEYS[1], 'state', 'COMMITTED', 'committedAt', ARGV[3])
            return {'COMMIT', tostring(joined)}
            """, List.class);

    private static final RedisScript<List> FENCE_ABORT_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('HGET', KEYS[1], 'revision') ~= ARGV[1] then
                return {'STALE'}
            end
            if redis.call('HGET', KEYS[1], 'state') ~= 'FENCING' then
                return {'DONE'}
            end
            redis.call('HSET', KEYS[1], 'state', 'ABORTED', 'abortedAt', ARGV[2],
                       'reason', ARGV[3])
            return {'ABORTED'}
            """, List.class);

    private final RouteStore routeStore;
    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final RouteConfigProperties properties;
    private final String nodeId;
    private final String host;
    private final String pid;
    private final Instant startedAt = Instant.now();

    private volatile ActiveSnapshot active = new ActiveSnapshot(0, null, List.of(), startedAt, "EMPTY");
    private volatile LoadedConfig staged;
    private volatile Instant lastRefreshAt;
    private volatile String lastError;
    private volatile String phase = "BOOTING";
    private volatile FenceState localFence;
    private final Map<Long, Disposable> scheduledActivations = new ConcurrentHashMap<>();

    public RouteConfigCoordinator(RouteStore routeStore,
                                  ReactiveStringRedisTemplate redis,
                                  ObjectMapper objectMapper,
                                  RouteConfigProperties properties) {
        this.routeStore = routeStore;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.properties = properties;
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        this.pid = runtimeName.contains("@") ? runtimeName.substring(0, runtimeName.indexOf('@')) : runtimeName;
        this.host = resolveHost();
        this.nodeId = properties.nodeId() == null || properties.nodeId().isBlank()
                ? host + "-" + pid + "-" + UUID.randomUUID().toString().substring(0, 8)
                : properties.nodeId().trim();
        loadPersistedSnapshot();
    }

    public boolean hasActiveConfig() {
        return active.revision() > 0 || !active.routes().isEmpty();
    }

    /** 单测专用：跳过 Redis/多实例 barrier，直接替换整份快照。 */
    void useSnapshotForTest(List<GatewayRoute> routes, boolean loaded) {
        if (!loaded) {
            active = new ActiveSnapshot(0, null, List.of(), Instant.now(), "EMPTY");
            return;
        }
        active = new ActiveSnapshot(1, "test-" + routes.size(), List.copyOf(routes), Instant.now(), "TEST");
    }

    /** 单测专用：直接从 store 拉整份快照。 */
    Mono<LoadedConfig> loadFromStoreForTest() {
        if (redis == null) {
            return routeStore.findAll().collectList().map(routes -> {
                List<GatewayRoute> copy = List.copyOf(routes);
                LoadedConfig config = new LoadedConfig(1, "test", copy, Instant.now(), "TEST");
                active = new ActiveSnapshot(config.revision(), config.checksum(),
                        List.copyOf(config.routes()), Instant.now(), "TEST");
                return config;
            });
        }
        return loadConsistent(0).doOnNext(config -> active = new ActiveSnapshot(
                config.revision(), config.checksum(), List.copyOf(config.routes()), Instant.now(), "TEST"));
    }

    public List<GatewayRoute> activeRoutes() {
        return active.routes();
    }

    public boolean routingPaused() {
        FenceState fence = localFence;
        return fence != null && fence.state().equals("FENCING")
                && System.currentTimeMillis() >= fence.fenceAt();
    }

    public Mono<Void> waitForRoutingGate() {
        FenceState fence = localFence;
        if (fence == null) {
            return Mono.empty();
        }
        if (!"FENCING".equals(fence.state())
                || System.currentTimeMillis() > fence.commitDeadline() + properties.maxClockSkew().toMillis()) {
            localFence = null;
            phase = "RUNNING";
            return Mono.empty();
        }
        if (System.currentTimeMillis() < fence.fenceAt()) {
            return Mono.empty();
        }
        long delay = Math.min(50, Math.max(5, fence.commitDeadline() - System.currentTimeMillis()));
        return Mono.delay(Duration.ofMillis(Math.max(5, delay)))
                .then(Mono.defer(() -> routingPaused() ? waitForRoutingGate() : Mono.empty()));
    }

    public Mono<List<GatewayRoute>> refreshNow() {
        if (redis == null) {
            return loadFromStoreForTest().map(LoadedConfig::routes);
        }
        return syncLatest().map(LoadedConfig::routes);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        heartbeat().subscribe(
                ok -> { },
                err -> log.warn("首个路由心跳写入失败，将按周期重试：{}", err.toString()));
        Flux.defer(() -> redis.listenToChannel(CHANGED_CHANNEL, ACTIVATION_CHANNEL, COMMAND_CHANNEL))
                .onErrorResume(err -> {
                    log.warn("路由配置订阅断开，将靠 {}/次轮询追齐：{}", properties.pollInterval(), err.toString());
                    return Mono.delay(properties.pollInterval()).then(Mono.empty());
                })
                .repeat()
                .subscribe(this::handleRedisMessage,
                        err -> log.warn("路由配置订阅异常：{}", err.toString()));

        Flux.interval(properties.heartbeatInterval())
                .concatMap(tick -> heartbeat().onErrorResume(err -> {
                    lastError = "heartbeat: " + err;
                    return Mono.empty();
                }))
                .subscribe();
        // 兜底轮询由 RouteCatalog.scheduledRefresh 统一触发，避免同一实例打两遍 Redis；
        // 这里只保留心跳与订阅。
        syncLatest()
                .subscribe(config -> {
                    if (staged == null) {
                        phase = "RUNNING";
                    }
                    log.info("路由初始版本已加载 revision={} routes={} checksum={}",
                            config.revision(), config.routes().size(), config.checksum());
                },
                        err -> {
                            phase = hasActiveConfig() ? "DISK_FALLBACK" : "UNAVAILABLE";
                            log.warn("路由初始版本加载失败{}：{}",
                                    hasActiveConfig() ? "，已使用本机旧快照顶住" : "，当前无可用配置",
                                    err.toString());
                        });
    }

    /** 本机后台写库成功后的事件：先自己备齐，再通知另外两台，避免通知早于新数据可见。 */
    public Mono<LoadedConfig> onLocalRoutesChanged(RoutesChangedEvent event) {
        if (redis == null) {
            return loadFromStoreForTest()
                    .onErrorResume(err -> Mono.empty());
        }
        return syncLatest()
                .doOnNext(config -> publish(CHANGED_CHANNEL, new ChangeMessage("CHANGED",
                        config.revision(), config.checksum(), event.routeNo(), event.reason(),
                        Instant.now().toEpochMilli())).subscribe())
                .doOnNext(config -> log.info("本机路由变更已备齐 revision={} checksum={} route={} reason={}",
                        config.revision(), config.checksum(), event.routeNo(), event.reason()))
                .onErrorResume(err -> {
                    lastError = "local-change: " + err;
                    log.warn("本机路由变更后备齐失败，继续使用旧快照：{}", err.toString());
                    return Mono.empty();
                });
    }

    /** 拉最新配置：失败不替换旧引用；新版本只进入 staged，激活由 barrier 决定。 */
    public Mono<LoadedConfig> syncLatest() {
        return loadConsistent(0)
                .flatMap(config -> {
                    lastRefreshAt = Instant.now();
                    lastError = null;
                    ActiveSnapshot current = active;
                    if (config.revision() < current.revision()) {
                        log.warn("读到更旧的路由 revision={}，当前 {}，拒绝降级",
                                config.revision(), current.revision());
                        return Mono.just(new LoadedConfig(current.revision(), current.checksum(),
                                current.routes(), current.loadedAt(), "ACTIVE"));
                    }
                    if (config.revision() == current.revision()) {
                        if ("EMPTY".equals(current.source()) && !config.routes().isEmpty()) {
                            // 兼容热刷新时代之前已经写进 Hash、但还没有全局 revision 的存量路由
                            active = new ActiveSnapshot(0, config.checksum(),
                                    List.copyOf(config.routes()), Instant.now(), "BASELINE");
                            phase = "RUNNING";
                            persistSnapshot(active);
                        } else {
                            active = current.withLoadedAt(lastRefreshAt);
                        }
                        return reconcileActivationMarker(config)
                                .then(heartbeat())
                                .thenReturn(config);
                    }

                    staged = config;
                    phase = "STAGED";
                    return saveStaged(config)
                            .then(writeNodeState())
                            .then(readFence().flatMap(marker -> marker.revision() == config.revision()
                                    ? joinFenceIfReady(config, marker)
                                    : tryBarrier(config)))
                            .thenReturn(config);
                })
                .doOnError(err -> {
                    lastError = err.toString();
                    log.debug("拉取路由新版本失败，保留旧版本 revision={}：{}",
                            active.revision(), err.toString());
                });
    }

    public Mono<String> requestForceRefresh() {
        String commandId = UUID.randomUUID().toString();
        RefreshCommand command = new RefreshCommand(commandId, nodeId, Instant.now().toEpochMilli());
        return writeJson(COMMAND_PREFIX + commandId, command, properties.refreshAckTtl())
                .then(publish(COMMAND_CHANNEL, command))
                .thenReturn(commandId);
    }

    public Mono<RefreshStatus> refreshStatus(String commandId) {
        return liveNodes().map(this::freshNodes).flatMap(nodes -> readAcks(commandId).map(acks -> {
            List<String> missing = nodes.stream().map(NodeState::nodeId)
                    .filter(id -> !acks.containsKey(id)).sorted().toList();
            int required = properties.expectedInstances() > 0
                    ? properties.expectedInstances()
                    : Math.max(1, nodes.size());
            boolean complete = nodes.size() >= required && missing.isEmpty();
            return new RefreshStatus(commandId, required, nodes.size(),
                    complete, new ArrayList<>(acks.values()), missing);
        }));
    }

    public Mono<RouteConfigStatus> status() {
        return liveNodes().map(this::freshNodes).map(nodes -> new RouteConfigStatus(nodeId, active.revision(),
                active.checksum(), active.routes().size(), staged == null ? null : staged.revision(),
                phase, lastRefreshAt, lastError,
                properties.expectedInstances() <= 0 ? nodes.size() : properties.expectedInstances(),
                nodes));
    }

    private Mono<LoadedConfig> loadConsistent(int attempt) {
        return routeStore.currentRevision()
                .flatMap(before -> routeStore.findAll().collectList()
                        .flatMap(routes -> routeStore.currentRevision().flatMap(after -> {
                            if (!Objects.equals(before, after)) {
                                if (attempt >= 5) {
                                    return Mono.error(new IllegalStateException("路由配置连续变更，无法读到稳定快照"));
                                }
                                return Mono.delay(Duration.ofMillis(20)).then(loadConsistent(attempt + 1));
                            }
                            List<GatewayRoute> copy = List.copyOf(routes);
                            return Mono.just(new LoadedConfig(after, routeStore.checksum(copy), copy,
                                    Instant.now(), "STAGED"));
                        })));
    }

    /** 实例重连/重启后即使没收到推送，也能靠 fence 状态加入或追赶。 */
    private Mono<Void> reconcileActivationMarker(LoadedConfig config) {
        return readFence().flatMap(marker -> joinFenceIfReady(config, marker));
    }

    private Mono<Void> tryBarrier(LoadedConfig config) {
        return liveNodes().flatMap(nodes -> {
            List<NodeState> candidates = freshNodes(nodes);
            int required = properties.expectedInstances() > 0
                    ? properties.expectedInstances()
                    : Math.max(1, candidates.size());
            if (candidates.size() < required) {
                log.info("路由 revision={} 已在本机备齐，但在线 {} 台 < 预期 {} 台，继续沿用旧版本",
                        config.revision(), candidates.size(), required);
                return Mono.empty();
            }
            List<String> missing = candidates.stream()
                    .filter(n -> n.stagedRevision() == null || n.stagedRevision() < config.revision()
                            || !config.checksum().equals(n.stagedChecksum()))
                    .map(NodeState::nodeId).sorted().toList();
            if (!missing.isEmpty()) {
                log.info("路由 revision={} 等待实例备齐：{}，未齐前继续沿用 revision={}",
                        config.revision(), missing, active.revision());
                return Mono.empty();
            }

            List<String> ids = candidates.stream().map(NodeState::nodeId).sorted().toList();
            String memberHash = sha256(ids.stream().collect(java.util.stream.Collectors.joining("\n", "", "\n")));
            long fenceAt = System.currentTimeMillis() + properties.switchDelay().toMillis();
            long deadline = fenceAt + properties.fenceGrace().toMillis();
            return openFence(config, fenceAt, deadline, memberHash, required);
        }).then();
    }

    @SuppressWarnings("unchecked")
    private Mono<Void> openFence(LoadedConfig config, long fenceAt, long deadline,
                                 String memberHash, int required) {
        return redis.execute(FENCE_SCRIPT, List.of(FENCE_KEY,
                        FENCE_KEY + ":members:" + config.revision()),
                        List.of(Long.toString(config.revision()), config.checksum(),
                                Long.toString(fenceAt), Long.toString(deadline),
                                memberHash, Long.toString(System.currentTimeMillis()),
                                Integer.toString(required)))
                .next()
                .flatMap(raw -> {
                    String result = ((List<Object>) raw).get(0).toString();
                    if ("NEWER".equals(result)) {
                        return Mono.empty();
                    }
                    FenceState fence = new FenceState(config.revision(), config.checksum(), fenceAt,
                            deadline, memberHash, "FENCING", required);
                    scheduleFenceMonitor(config.revision(), config.checksum(), fenceAt, deadline, memberHash, required);
                    return publish(ACTIVATION_CHANNEL, new ActivationMessage("FENCE", config.revision(),
                            config.checksum(), fenceAt, memberHash, Instant.now().toEpochMilli(),
                            deadline, required))
                            .then(joinFence(fence, required));
                }).then();
    }

    private Mono<Void> joinFenceIfReady(LoadedConfig config, FenceState marker) {
        if (marker == null || marker.revision() <= active.revision()) {
            return Mono.empty();
        }
        if (config.revision() < marker.revision()) {
            return Mono.empty();
        }
        if ("COMMITTED".equals(marker.state())) {
            return completeActivation(marker.revision(), marker.checksum(), marker);
        }
        if (!"FENCING".equals(marker.state())) {
            return Mono.empty();
        }
        return joinFence(marker, requiredFromMarker(marker));
    }

    @SuppressWarnings("unchecked")
    private Mono<Void> joinFence(FenceState marker, int required) {
        if (localFence != null && localFence.revision() == marker.revision()) {
            scheduleFenceMonitor(marker.revision(), marker.checksum(), marker.fenceAt(),
                    marker.commitDeadline(), marker.memberHash(), required);
            return Mono.empty();
        }
        String memberKey = FENCE_KEY + ":members:" + marker.revision();
        localFence = marker;
        phase = "FENCING";
        return redis.execute(FENCE_JOIN_SCRIPT, List.of(FENCE_KEY, memberKey),
                        List.of(Long.toString(marker.revision()), nodeId,
                                Long.toString(System.currentTimeMillis())))
                .next()
                .doOnNext(raw -> {
                    String result = ((List<Object>) raw).get(0).toString();
                    if ("JOINED".equals(result)) {
                        scheduleFence(marker, required);
                    } else {
                        localFence = null;
                        phase = "RUNNING";
                    }
                })
                .onErrorResume(err -> {
                    localFence = null;
                    phase = "RUNNING";
                    return Mono.error(err);
                })
                .then(writeNodeState());
    }

    private void scheduleFence(FenceState marker, int required) {
        localFence = marker;
        phase = "FENCING";
        scheduleFenceMonitor(marker.revision(), marker.checksum(), marker.fenceAt(),
                marker.commitDeadline(), marker.memberHash(), required);
    }

    private void scheduleFenceMonitor(long revision, String checksum, long fenceAt, long deadline,
                                      String memberHash, int required) {
        long delay = Math.max(0, fenceAt - System.currentTimeMillis());
        scheduledActivations.compute(revision, (key, old) -> {
            if (old != null) {
                old.dispose();
            }
            return Mono.delay(Duration.ofMillis(delay))
                    .flatMap(x -> monitorFence(revision, checksum, deadline, memberHash, required))
                    .subscribe(v -> { }, err -> log.warn("路由栅栏 revision={} 监控失败：{}", revision, err));
        });
    }

    @SuppressWarnings("unchecked")
    private Mono<Void> monitorFence(long revision, String checksum, long deadline,
                                    String memberHash, int required) {
        return readFence().flatMap(marker -> {
            if (marker == null || marker.revision() != revision) {
                localFence = null;
                return Mono.empty();
            }
            if ("COMMITTED".equals(marker.state())) {
                return completeActivation(revision, checksum, marker);
            }
            if ("ABORTED".equals(marker.state())) {
                localFence = null;
                phase = "RUNNING";
                log.warn("路由栅栏 revision={} 超时/失败，已放弃本次切换并继续旧版本", revision);
                return writeNodeState();
            }
            return redis.execute(FENCE_COMMIT_SCRIPT,
                            List.of(FENCE_KEY, FENCE_KEY + ":members:" + revision),
                            List.of(Long.toString(revision), Long.toString(System.currentTimeMillis()),
                                    Long.toString(System.currentTimeMillis()), Integer.toString(required)))
                    .next()
                    .flatMap(raw -> {
                        String result = ((List<Object>) raw).get(0).toString();
                        return switch (result) {
                            case "COMMIT", "DONE" -> publish(ACTIVATION_CHANNEL,
                                    new ActivationMessage("COMMIT", revision, checksum,
                                            marker.fenceAt(), memberHash, Instant.now().toEpochMilli(),
                                            deadline, required))
                                    .then(completeActivation(revision, checksum, marker));
                            case "STALE" -> Mono.empty();
                            default -> System.currentTimeMillis() < deadline
                                    ? Mono.delay(Duration.ofMillis(25)).then(
                                            monitorFence(revision, checksum, deadline, memberHash, required))
                                    : abortFence(revision, "FENCE_JOIN_TIMEOUT");
                        };
                    });
        });
    }

    @SuppressWarnings("unchecked")
    private Mono<Void> abortFence(long revision, String reason) {
        return redis.execute(FENCE_ABORT_SCRIPT, List.of(FENCE_KEY),
                        List.of(Long.toString(revision), Long.toString(System.currentTimeMillis()), reason))
                .next()
                .flatMap(raw -> {
                    String result = ((java.util.List<Object>) raw).get(0).toString();
                    if ("DONE".equals(result) || "STALE".equals(result)) {
                        return Mono.empty();
                    }
                    return publish(ACTIVATION_CHANNEL, new ActivationMessage("ABORT", revision,
                            null, 0, null, Instant.now().toEpochMilli(), 0, 0));
                })
                .then();
    }

    private Mono<Void> completeActivation(long revision, String checksum, FenceState marker) {
        LoadedConfig config = staged;
        Mono<LoadedConfig> ready;
        if (config != null && config.revision() == revision && checksum.equals(config.checksum())) {
            ready = Mono.just(config);
        } else {
            ready = loadConsistent(0)
                    .doOnNext(loaded -> staged = loaded)
                    .flatMap(loaded -> loaded.revision() == revision
                            ? Mono.just(loaded)
                            : Mono.error(new IllegalStateException(
                                    "栅栏 revision=" + revision + " 已过期，最新为 " + loaded.revision())));
        }
        return ready.flatMap(loaded -> {
            ActiveSnapshot snapshot = new ActiveSnapshot(revision, checksum, List.copyOf(loaded.routes()),
                    Instant.now(), "ACTIVATED");
            active = snapshot;
            staged = null;
            localFence = null;
            phase = "RUNNING";
            scheduledActivations.remove(revision);
            log.info("路由整体切换完成 revision={} routes={}", revision, snapshot.routes().size());
            persistSnapshot(snapshot);
            return writeNodeState();
        }).onErrorResume(err -> {
            localFence = null;
            phase = "RUNNING";
            log.warn("提交路由 revision={} 失败，继续旧版本：{}", revision, err.toString());
            return writeNodeState();
        }).then();
    }

    private int requiredFromMarker(FenceState marker) {
        return marker.required() > 0 ? marker.required() : 3;
    }

    private void handleRedisMessage(org.springframework.data.redis.connection.ReactiveSubscription.Message<String, String> message) {
        String body = message.getMessage();
        switch (message.getChannel()) {
            case CHANGED_CHANNEL -> parse(body, ChangeMessage.class)
                    .flatMap(msg -> syncLatest())
                    .subscribe(v -> { }, err -> log.debug("处理路由变更推送失败：{}", err.toString()));
            case ACTIVATION_CHANNEL -> parse(body, ActivationMessage.class)
                    .flatMap(this::handleActivationMessage)
                    .subscribe(v -> { }, err -> log.debug("处理路由栅栏推送失败：{}", err.toString()));
            case COMMAND_CHANNEL -> parse(body, RefreshCommand.class)
                    .flatMap(this::handleRefreshCommand)
                    .subscribe(v -> { }, err -> log.warn("处理手动刷新命令失败：{}", err.toString()));
            default -> log.debug("忽略未知路由配置频道 {}", message.getChannel());
        }
    }

    private Mono<Void> handleActivationMessage(ActivationMessage msg) {
        if (msg.revision() <= active.revision()) {
            return Mono.empty();
        }
        FenceState marker = new FenceState(msg.revision(), msg.checksum(), msg.activateAt(),
                msg.commitDeadline(), msg.memberHash(), "FENCE".equals(msg.type()) ? "FENCING" : msg.type(),
                msg.required());
        if ("ABORT".equals(msg.type())) {
            if (localFence != null && localFence.revision() == msg.revision()) {
                localFence = null;
                phase = "RUNNING";
                return writeNodeState();
            }
            return Mono.empty();
        }
        if ("COMMIT".equals(msg.type())) {
            if (staged != null && staged.revision() == msg.revision()) {
                return completeActivation(msg.revision(), msg.checksum(), marker);
            }
            return syncLatest().then();
        }
        if (staged == null || staged.revision() < msg.revision()) {
            return syncLatest().then();
        }
        return joinFence(marker, msg.required());
    }

    private Mono<Void> handleRefreshCommand(RefreshCommand command) {
        return syncLatest()
                .flatMap(config -> writeAck(command.commandId(), new RefreshAck(nodeId, true,
                        active.revision(), active.checksum(), active.routes().size(),
                        Instant.now().toEpochMilli(), null)))
                .onErrorResume(err -> writeAck(command.commandId(), new RefreshAck(nodeId, false,
                        active.revision(), active.checksum(), active.routes().size(),
                        Instant.now().toEpochMilli(), err.toString())))
                .then();
    }

    private Mono<List<NodeState>> liveNodes() {
        return redis.scan(ScanOptions.scanOptions().match(NODE_PREFIX + "*").count(100).build())
                .flatMap(key -> redis.opsForValue().get(key).flatMap(json -> parse(json, NodeState.class)))
                .collectList();
    }

    private List<NodeState> freshNodes(List<NodeState> nodes) {
        long cutoff = System.currentTimeMillis() - properties.heartbeatTtl().toMillis();
        return nodes.stream().filter(n -> n.heartbeatAt() >= cutoff).toList();
    }

    private Mono<Void> heartbeat() {
        return writeNodeState();
    }

    private Mono<Void> writeNodeState() {
        NodeState state = new NodeState(nodeId, host, pid, startedAt.toEpochMilli(),
                Instant.now().toEpochMilli(), active.revision(), active.checksum(),
                active.routes().size(), staged == null ? null : staged.revision(),
                staged == null ? null : staged.checksum(), localFence == null ? null : localFence.revision(),
                phase,
                lastRefreshAt == null ? null : lastRefreshAt.toEpochMilli(), lastError);
        return writeJson(NODE_PREFIX + nodeId, state, properties.heartbeatTtl()).then();
    }

    private Mono<Void> saveStaged(LoadedConfig config) {
        return writeJson(STAGED_PREFIX + nodeId + ":" + config.revision(),
                new StagedState(config.revision(), config.checksum(), Instant.now().toEpochMilli()),
                Duration.ofHours(1)).then();
    }

    private Mono<FenceState> readFence() {
        return redis.opsForHash().entries(FENCE_KEY).collectMap(e -> e.getKey().toString(),
                e -> e.getValue() == null ? null : e.getValue().toString())
                .filter(map -> map.containsKey("revision"))
                .map(map -> new FenceState(
                        Long.parseLong(map.get("revision")),
                        map.get("checksum"),
                        parseLong(map.get("fenceAt")),
                        parseLong(map.get("commitDeadline")),
                        map.get("memberHash"),
                        map.getOrDefault("state", "FENCING"),
                        (int) parseLong(map.get("required"))))
                .defaultIfEmpty(new FenceState(0, null, 0, 0, null, "NONE", 0));
    }

    private static long parseLong(String value) {
        return value == null ? 0L : Long.parseLong(value);
    }

    private Mono<Map<String, RefreshAck>> readAcks(String commandId) {
        return redis.scan(ScanOptions.scanOptions().match(ACK_PREFIX + commandId + ":*").count(100).build())
                .flatMap(key -> redis.opsForValue().get(key).flatMap(json -> parse(json, RefreshAck.class)))
                .collectMap(RefreshAck::nodeId, ack -> ack);
    }

    private Mono<Void> writeAck(String commandId, RefreshAck ack) {
        return writeJson(ACK_PREFIX + commandId + ":" + nodeId, ack, properties.refreshAckTtl()).then();
    }

    private Mono<Boolean> writeJson(String key, Object value, Duration ttl) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(value))
                .flatMap(json -> redis.opsForValue().set(key, json, ttl));
    }

    private Mono<Long> publish(String channel, Object message) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(message))
                .flatMap(json -> redis.convertAndSend(channel, json));
    }

    private <T> Mono<T> parse(String json, Class<T> type) {
        return Mono.fromCallable(() -> objectMapper.readValue(json, type))
                .onErrorMap(err -> new IllegalStateException("解析路由配置消息失败：" + err.getMessage(), err));
    }

    private String resolveHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public record LoadedConfig(long revision, String checksum, List<GatewayRoute> routes,
                               Instant loadedAt, String state) {
    }

    public record ActiveSnapshot(long revision, String checksum, List<GatewayRoute> routes,
                                 Instant loadedAt, String source) {
        ActiveSnapshot withLoadedAt(Instant at) {
            return new ActiveSnapshot(revision, checksum, routes, at, source);
        }
    }

    private record PersistedSnapshot(long revision, String checksum, List<GatewayRoute> routes,
                                     Instant savedAt) {
    }

    private void loadPersistedSnapshot() {
        try {
            String configured = properties.snapshotFile();
            Path file = Path.of(configured == null || configured.isBlank()
                    ? "data/route-snapshot-" + sha256(nodeId).substring(0, 12) + ".json"
                    : configured);
            if (!Files.exists(file)) {
                return;
            }
            PersistedSnapshot persisted = objectMapper.readValue(Files.readAllBytes(file),
                    PersistedSnapshot.class);
            if (persisted.routes() == null || persisted.checksum() == null) {
                return;
            }
            active = new ActiveSnapshot(persisted.revision(), persisted.checksum(),
                    List.copyOf(persisted.routes()), persisted.savedAt(), "DISK");
            phase = "DISK_FALLBACK";
            log.info("已从本机快照恢复路由 revision={} routes={}",
                    persisted.revision(), persisted.routes().size());
        } catch (Exception e) {
            log.warn("读取本机路由快照失败，将等待 Redis：{}", e.toString());
        }
    }

    private void persistSnapshot(ActiveSnapshot snapshot) {
        if (snapshot.routes().isEmpty()) {
            return;
        }
        try {
            String configured = properties.snapshotFile();
            Path file = Path.of(configured == null || configured.isBlank()
                    ? "data/route-snapshot-" + sha256(nodeId).substring(0, 12) + ".json"
                    : configured);
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            byte[] bytes = objectMapper.writeValueAsBytes(new PersistedSnapshot(
                    snapshot.revision(), snapshot.checksum(), snapshot.routes(), Instant.now()));
            Files.write(tmp, bytes);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            log.warn("持久化路由快照失败（不影响当前转发）：{}", e.toString());
        }
    }

    public record NodeState(String nodeId, String host, String pid, long startedAt, long heartbeatAt,
                            long activeRevision, String activeChecksum, int routeCount,
                            Long stagedRevision, String stagedChecksum, Long fenceRevision,
                            String phase,
                            Long lastRefreshAt, String lastError) {
    }

    public record RouteConfigStatus(String localNodeId, long activeRevision, String activeChecksum,
                                    int routeCount, Long stagedRevision, String phase,
                                    Instant lastRefreshAt, String lastError, int expectedInstances,
                                    List<NodeState> nodes) {
    }

    public record RefreshStatus(String commandId, int expectedInstances, int liveInstances,
                                boolean complete, List<RefreshAck> acks, List<String> missingNodes) {
    }

    public record RefreshAck(String nodeId, boolean success, long activeRevision, String activeChecksum,
                             int routeCount, long ackAt, String error) {
    }

    private record ChangeMessage(String type, long revision, String checksum, String routeNo,
                                 String reason, long at) {
    }

    private record ActivationMessage(String type, long revision, String checksum, long activateAt,
                                     String memberHash, long at, long commitDeadline, int required) {
    }

    private record RefreshCommand(String commandId, String requestedBy, long at) {
    }

    private record StagedState(long revision, String checksum, long stagedAt) {
    }

    private record FenceState(long revision, String checksum, long fenceAt, long commitDeadline,
                              String memberHash, String state, int required) {
    }
}

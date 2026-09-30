package com.apigw.proxy.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 路由热刷新与多实例协同参数。
 *
 * @param nodeId 当前实例编号；不传则按 hostname/pid/随机后缀生成
 * @param expectedInstances 预期在线实例数。0=心跳动态成员；生产三台建议固定成 3
 * @param pollInterval 拉取兜底周期。推送丢了也会在这个周期内发现新 revision
 * @param heartbeatInterval 实例心跳周期
 * @param heartbeatTtl 心跳过期时间，超过即认为实例暂时不在线
 * @param switchDelay 三台都备齐后，统一切到新版本的提前量
 * @param maxClockSkew 仅用于日志/观测；两阶段栅栏不依赖机器时钟严格同步
 * @param fenceGrace 到点后等待所有实例进入流量栅栏的最长时间，超时放弃本次切换
 * @param refreshAckTtl 手动刷新命令等待/保留结果的时间
 * @param snapshotFile 本机最后一份好快照的落盘缓存；Redis 重启不可用时先用它顶住
 */
@ConfigurationProperties(prefix = "apigw.proxy.route-config")
public record RouteConfigProperties(String nodeId,
                                    int expectedInstances,
                                    Duration pollInterval,
                                    Duration heartbeatInterval,
                                    Duration heartbeatTtl,
                                    Duration switchDelay,
                                    Duration maxClockSkew,
                                    Duration fenceGrace,
                                    Duration refreshAckTtl,
                                    String snapshotFile) {

    public RouteConfigProperties {
        if (pollInterval == null) {
            pollInterval = Duration.ofSeconds(5);
        }
        if (heartbeatInterval == null) {
            heartbeatInterval = Duration.ofSeconds(3);
        }
        if (heartbeatTtl == null) {
            heartbeatTtl = Duration.ofSeconds(12);
        }
        if (switchDelay == null) {
            switchDelay = Duration.ofSeconds(3);
        }
        if (maxClockSkew == null) {
            maxClockSkew = Duration.ofSeconds(1);
        }
        if (fenceGrace == null) {
            fenceGrace = Duration.ofSeconds(2);
        }
        if (refreshAckTtl == null) {
            refreshAckTtl = Duration.ofMinutes(10);
        }
    }
}

# 名创核销回调实施计划

状态：已按用户“继续”完成代码与本地验证。在当前dev目录工作，未提交、推送或部署；本轮未派生子代理。

本轮目标：以已核对旧卡管key2协议为兼容基线可靠接收核销事件，并事务生成V1.2商户通知；供应商真实验收另行记录。
架构：API阻塞调度隔离，application端口，infrastructure验签与MySQL持久接收箱，job恢复处理，复用已有商户通知Worker。
技术：Java21、WebFlux、MyBatis、Spring TransactionTemplate、Liquibase、JUnit5、H2。

- [x] 任务1 更新两份主设计和完整spec，明确当前卡管路径、通知地址、T1/T2/T3、锁序、version CAS、待处理核销事件的退款防护及作用范围。
- [x] 任务2 后续先写失败测试并验证，再实现application通知/验签/存储端口、接收服务；infrastructure独立key2验签、事件Mapper与事务适配器、Liquibase0021、V1.2核销签名器和作废并发防护。修改MinisoReceiptStore和相关测试；移除Gateway入站TODO。
- [x] 任务3 后续先写入口/装配失败测试，再实现API Controller、bootstrap配置、独立回调配置与XXL恢复handler。不得将API模块反向依赖infrastructure，不阻塞Netty线程。
- [x] 任务4 更新仓库可执行示例、供应商配置模板、集成指导及测试记录；示例只用固定测试key2。
- [x] 任务5 对照spec做设计符合性与代码质量检查，修复发现并复验；不自行派生代理。
- [x] 任务6 运行JDK21针对性测试、全模块Maven测试、模拟器回归及git diff --check，记录证据后交付。

核心跨模块合同（所有实现方使用同名签名）：

```java
public record MinisoWriteOffNotification(String couponCode, java.time.LocalDateTime useTime) {}
public interface MinisoWriteOffAuthenticator {
    MinisoWriteOffNotification authenticate(String rawBody, java.util.Map<String, java.util.List<String>> headers);
}
public interface MinisoWriteOffStore {
    void accept(MinisoWriteOffNotification notice);
    MinisoWriteOffBatchResult processBatch(int shardTotal, int shardIndex, int limit, String workerId);
}
public final class ReceiveMinisoWriteOffService {
    public ReceiveMinisoWriteOffService(MinisoWriteOffAuthenticator authenticator, MinisoWriteOffStore store) { /* 验签与持久化端口 */ }
    public void receive(String rawBody, java.util.Map<String, java.util.List<String>> headers) { /* 验签后持久提交 */ }
}
```

接口位于application.recharge；验签器位于infrastructure.supplier.miniso，构造参数String key2；存储适配器位于infrastructure.persistence。新增专属V12RedemptionNotifyPayloadSigner避免改变原作废通知合同。

RED/GREEN命令：`JAVA_HOME=$(/usr/libexec/java_home -v21) mvn -pl cardshop-trade-engine-bootstrap -am test -Dtest=Miniso*Test,V12Redemption*Test -Dsurefire.failIfNoSpecifiedTests=false -q`。先观察新增测试因缺失目标行为而失败，之后实现并确认全部通过。

全量：`JAVA_HOME=$(/usr/libexec/java_home -v21) mvn test -q`；模拟器按独立pom运行。失败必须区分已有和本次变更，不能忽略。

本轮补充的实施前提：callback.enabled/processing-enabled分离；通知Worker重试次数上限；已接收未处理事件阻止自动退款；全局锁序禁止先锁事件再等待主单；详细类/参数/DDL以spec第5至11节为准。

最终验证：主工程644项（19条件跳过）、模拟器74项，失败/错误均为0；永久资金保护与晚装配凭据Store已覆盖。执行记录见docs/集成测试.md。

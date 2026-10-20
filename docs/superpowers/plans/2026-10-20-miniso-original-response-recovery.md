# 名创原始成功流水恢复执行计划

> 在当前工作区串行执行，保留已有未提交改动；使用TDD，不提交或推送Git。用户已确认严格恢复原订单、不再次发券。

**Goal:** 使用成功流水10016的原始响应，受控恢复A35原订单202610200826272563779785。

**Architecture:** 独立Java21/JDBC维护工具，复用在线Gateway严格凭据解码，单连接事务和version校验；无供应商调用、无应用启动、无余额修改。默认dry-run。设计见两份主设计的2026-10-20原始流水恢复章节。

**Tech Stack:** Java21、Jackson、JDBC、H2/JUnit5、现有MySQL连接驱动。

## 执行步骤

- [x] 下载Amp完整序列化上下文，核对历史授权、未提交代码和现场状态；重跑根构建698例及模拟器77例。
- [x] 获得用户明确恢复口径；先更新两份主设计。原手机号必须匹配，不兼容phone=null查单。
- [x] 为infrastructure的`maintenance/MinisoOriginalResponseRecoveryTest.java`添加真实H2事务测试：成功、dry-run、幂等、缺手机号、身份/金额/批次错配、状态/version冲突、退款/核销、写入中途失败回滚。先运行定向Maven并记录缺实现类的失败。
- [x] 新增`maintenance/MinisoOriginalResponseRecovery.java`，提供有界stdin CLI及单连接事务；Gateway暴露无网络的严格发券响应验证函数。23例测试通过，含双连接并发及重复JSON键拒绝。
- [x] 根执行`mvn clean package`721例（702通过、19可选中间件测试跳过），独立模拟器77例通过，diff检查通过。
- [x] 仅在pro143受限目录准备6个编译类组成的工具JAR和现有JAR依赖；读取实际schema、原单、成功流水10016及查单version12；备份8张相关表到600文件。
- [x] 19:30:09 dry-run=VERIFIED，19:30:31以同一预期version单次apply=RECOVERED，未重新发券。
- [x] 读回凭据及主单/扩展/查单SUCCESS，查单version13/retry6、人工任务0；余额不变、通知0、A35订单1笔、流水仍7条。19:31:37签名/SM4 V1查单200/12/3/1。集成及部署记录已更新，未提交推送。

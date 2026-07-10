package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * Redis pending MQ 补投的真实 XXL-Job handler。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。这个类只做 XXL-Job SDK
 * 到 framework-free {@link RechargePersistMqRepublishJob#runOnce(int, int, int, String)} 门面的适配：
 * 分片、批量大小和 worker 标识在这里解析，Redis pending 扫描、claim、RabbitMQ 补投和 MySQL 二次确认仍只存在一套业务实现。
 */
public class TradeEnginePersistMqRepublishXxlJobHandler {

    static final String HANDLER_NAME = "rechargePersistMqRepublishJob";

    private static final Logger log = LoggerFactory.getLogger(TradeEnginePersistMqRepublishXxlJobHandler.class);
    private static final Pattern PARAM_SEPARATOR = Pattern.compile("[,;\\s]+");
    private static final Pattern WORKER_ID_PATTERN = Pattern.compile("[A-Za-z0-9._:-]{1,120}");
    private static final Pattern WORKER_ID_SAFE_CHARS = Pattern.compile("[^A-Za-z0-9._:-]");
    private static final int DEFAULT_HANDLER_LIMIT = 0;
    private static final int MAX_WORKER_ID_LENGTH = 120;

    private final RechargePersistMqRepublishJob persistMqRepublishJob;

    public TradeEnginePersistMqRepublishXxlJobHandler(RechargePersistMqRepublishJob persistMqRepublishJob) {
        this.persistMqRepublishJob = Objects.requireNonNull(
                persistMqRepublishJob,
                "persistMqRepublishJob must not be null");
    }

    /**
     * XXL-Job 调度中心调用入口。
     *
     * <p>XXL-Job 2.4.x 通过 {@link XxlJobHelper} 暴露分片和参数。这里必须记录
     * {@code shardIndex/shardTotal/workerId/batchId}，并把执行结果写入 XXL-Job 日志；异常路径使用 error
     * 级别记录堆栈后继续抛出，让调度中心把本次触发标记为失败，不能静默吞掉补偿异常。
     */
    @XxlJob(HANDLER_NAME)
    public void execute() {
        int shardTotal = XxlJobHelper.getShardTotal();
        int shardIndex = XxlJobHelper.getShardIndex();
        long jobId = XxlJobHelper.getJobId();
        String jobParam = XxlJobHelper.getJobParam();
        try {
            RechargeCompensationBatchResult result = runFromXxlContext(shardTotal, shardIndex, jobId, jobParam);
            String message = resultMessage(shardTotal, shardIndex, jobId, jobParam, result);
            XxlJobHelper.log(message);
            XxlJobHelper.handleSuccess(message);
        } catch (RuntimeException ex) {
            log.error("XXL-Job触发Redis pending补投失败 shardTotal={}, shardIndex={}, jobId={}, jobParam={}",
                    shardTotal,
                    shardIndex,
                    jobId,
                    jobParam,
                    ex);
            XxlJobHelper.log(ex);
            XxlJobHelper.handleFail(safeMessage(ex));
            throw ex;
        }
    }

    RechargeCompensationBatchResult runFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        XxlRunOptions options = XxlRunOptions.parse(jobId, jobParam);
        log.info("XXL-Job触发Redis pending补投 shardTotal={}, shardIndex={}, limit={}, workerId={}, batchId={}",
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.batchId());
        RechargeCompensationBatchResult result = persistMqRepublishJob.runOnce(
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId());
        log.info("XXL-Job触发Redis pending补投完成 shardTotal={}, shardIndex={}, limit={}, workerId={}, batchId={}, "
                        + "scanned={}, succeeded={}, failed={}, skipped={}",
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.batchId(),
                result.scanned(),
                result.succeeded(),
                result.failed(),
                result.skipped());
        return result;
    }

    private static String resultMessage(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam,
            RechargeCompensationBatchResult result) {
        XxlRunOptions options = XxlRunOptions.parse(jobId, jobParam);
        return "rechargePersistMqRepublishJob finished shardTotal=%d, shardIndex=%d, limit=%d, workerId=%s, batchId=%s, scanned=%d, succeeded=%d, failed=%d, skipped=%d"
                .formatted(
                        shardTotal,
                        shardIndex,
                        options.limit(),
                        options.workerId(),
                        options.batchId(),
                        result.scanned(),
                        result.succeeded(),
                        result.failed(),
                        result.skipped());
    }

    private static String safeMessage(RuntimeException ex) {
        String message = ex.getMessage();
        return StringUtils.hasText(message) ? message : ex.getClass().getSimpleName();
    }

    record XxlRunOptions(int limit, String workerId, String batchId) {

        static XxlRunOptions parse(long jobId, String jobParam) {
            Map<String, String> params = parseParams(jobParam);
            String batchId = valueOrDefault(params.get("batchid"), "job-" + jobId);
            int limit = parseLimit(params.get("limit"));
            String workerId = params.get("workerid");
            if (!StringUtils.hasText(workerId)) {
                workerId = defaultWorkerId(batchId);
            }
            workerId = validateWorkerId(workerId);
            return new XxlRunOptions(limit, workerId, batchId);
        }

        private static Map<String, String> parseParams(String jobParam) {
            Map<String, String> params = new LinkedHashMap<>();
            if (!StringUtils.hasText(jobParam)) {
                return params;
            }
            for (String token : PARAM_SEPARATOR.split(jobParam.trim())) {
                if (!StringUtils.hasText(token)) {
                    continue;
                }
                int equalsIndex = token.indexOf('=');
                if (equalsIndex <= 0 || equalsIndex == token.length() - 1) {
                    throw new IllegalArgumentException("XXL-Job参数必须使用 key=value 格式: " + token);
                }
                String key = token.substring(0, equalsIndex).trim().toLowerCase(Locale.ROOT);
                String value = token.substring(equalsIndex + 1).trim();
                params.put(key, value);
            }
            return params;
        }

        private static int parseLimit(String value) {
            if (!StringUtils.hasText(value)) {
                return DEFAULT_HANDLER_LIMIT;
            }
            try {
                int parsed = Integer.parseInt(value);
                if (parsed < 0) {
                    throw new IllegalArgumentException("limit must be non-negative");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("limit must be an integer", ex);
            }
        }

        private static String validateWorkerId(String workerId) {
            String value = workerId.trim();
            if (!WORKER_ID_PATTERN.matcher(value).matches()) {
                throw new IllegalArgumentException("workerId must match " + WORKER_ID_PATTERN.pattern());
            }
            return value;
        }

        private static String defaultWorkerId(String batchId) {
            String raw = "xxl:" + batchId + ":" + localIdentity();
            String sanitized = WORKER_ID_SAFE_CHARS.matcher(raw).replaceAll("-");
            if (sanitized.length() <= MAX_WORKER_ID_LENGTH) {
                return sanitized;
            }
            return sanitized.substring(0, MAX_WORKER_ID_LENGTH);
        }

        private static String valueOrDefault(String value, String defaultValue) {
            return StringUtils.hasText(value) ? value.trim() : defaultValue;
        }

        private static String localIdentity() {
            String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
            try {
                return InetAddress.getLocalHost().getHostName() + ":" + runtimeName;
            } catch (UnknownHostException ex) {
                return "unknown-host:" + runtimeName;
            }
        }
    }
}

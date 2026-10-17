package com.cardshop.trade.engine.job.recharge;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.recharge.*;

import org.junit.jupiter.api.Test;

/** 恢复门面参数与分片委派；参见详细设计9.5。 */
class MinisoWriteOffProcessJobTest {
    @Test
    void delegatesShardAndRejectsUnboundedOrMalformedWork() {
        var store = mock(MinisoWriteOffStore.class);
        var job = new MinisoWriteOffProcessJob(store);
        var result = new MinisoWriteOffBatchResult(1, 1, 0, 0, 0, 0);
        when(store.processBatch(4, 2, 100, "worker")).thenReturn(result);
        assertThat(job.runOnce(4, 2, 100, "worker")).isEqualTo(result);
        assertThatThrownBy(() -> job.runOnce(0, 0, 10, "worker"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> job.runOnce(4, 4, 10, "worker"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> job.runOnce(1, 0, 1001, "worker"))
                .isInstanceOf(IllegalArgumentException.class);
        verify(store).processBatch(4, 2, 100, "worker");
        verifyNoMoreInteractions(store);
    }
}

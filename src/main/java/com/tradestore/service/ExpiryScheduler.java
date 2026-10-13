package com.tradestore.service;

import java.time.Clock;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Daily job that marks matured trades as expired. ShedLock ensures a single active instance.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExpiryScheduler {

    private final TradeExpiryService tradeExpiryService;
    private final Clock clock;

    @Scheduled(cron = "${trade-store.expiry.cron}", zone = "${trade-store.business-zone}")
    @SchedulerLock(name = "expire-trades")
    public void run() {
        int expired = expireMaturedTrades();
        log.info("Expiry run completed: expiredTrades={}", expired);
    }

    /**
     * Expires matured trades batch by batch until none are left.
     *
     * @return the total number of trades expired
     */
    public int expireMaturedTrades() {
        LocalDate today = LocalDate.now(clock);
        int total = 0;
        int expiredInBatch;
        do {
            expiredInBatch = tradeExpiryService.expireNextBatch(today);
            total += expiredInBatch;
        } while (expiredInBatch > 0);
        return total;
    }
}

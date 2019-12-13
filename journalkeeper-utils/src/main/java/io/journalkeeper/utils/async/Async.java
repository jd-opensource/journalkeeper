package io.journalkeeper.utils.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author LiYue
 * Date: 2019/12/13
 */
public class Async {
    public static <T> CompletableFuture<T> scheduleAsync (
            ScheduledExecutorService executor,
            Supplier<CompletableFuture<T>> command,
            long delay,
            TimeUnit unit
    ) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        executor.schedule(
                (() -> {
                    command.get().thenAccept(
                            completableFuture::complete
                    )
                            .exceptionally(
                                    t -> {completableFuture.completeExceptionally(t);return null;}
                            );
                }),
                delay,
                unit
        );
        return completableFuture;
    }

}

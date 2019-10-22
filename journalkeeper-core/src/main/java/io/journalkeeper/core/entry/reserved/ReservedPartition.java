package io.journalkeeper.core.entry.reserved;

import java.util.Collection;
import java.util.Collections;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public class ReservedPartition {
    public static final int RESERVED_PARTITION = 30000;

    public static void validatePartition(int partition) {
        if(partition >= RESERVED_PARTITION || partition < 0) {
            throw new IllegalArgumentException(
                    String.format("partition %d should be zero or positive number and less than %d",
                            partition, RESERVED_PARTITION)
            );
        }
    }

    public static void validatePartitions(int [] partitions) {
        for (int partition : partitions) {
            validatePartition(partition);
        }
    }

    public static void validatePartitions(Collection<Integer> partitions) {
        for (Integer partition : partitions) {
            validatePartition(partition);
        }
    }
}

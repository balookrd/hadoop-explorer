package org.apache.hadoop.explorer.replicator.orchestrator.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Высокопроизводительный потокобезопасный буфер накопления прогресса задач (PERF-06).
 * 
 * <p>Использует неблокирующие LongAdder со стрипированием кэш-линий, исключая межпоточную
 * конкуренцию ядер процессора при завершении тысяч мелких файлов в секунду.
 */
public class JobProgressBuffer {

    public static class ProgressDelta {
        private final LongAdder bytesAdder = new LongAdder();
        private final LongAdder filesAdder = new LongAdder();

        public void add(long bytes, int files) {
            if (bytes > 0) {
                bytesAdder.add(bytes);
            }
            if (files > 0) {
                filesAdder.add(files);
            }
        }

        public long getAndResetBytes() {
            return bytesAdder.sumThenReset();
        }

        public int getAndResetFiles() {
            return (int) filesAdder.sumThenReset();
        }

        public long getBytes() {
            return bytesAdder.sum();
        }

        public int getFiles() {
            return (int) filesAdder.sum();
        }

        public boolean isEmpty() {
            return bytesAdder.sum() == 0L && filesAdder.sum() == 0L;
        }
    }

    private final ConcurrentHashMap<String, ProgressDelta> deltas = new ConcurrentHashMap<>();

    public void record(String jobId, long bytes, int files) {
        if (jobId == null || (bytes <= 0 && files <= 0)) {
            return;
        }
        deltas.computeIfAbsent(jobId, k -> new ProgressDelta()).add(bytes, files);
    }

    public Map<String, long[]> drain() {
        Map<String, long[]> result = new HashMap<>();
        for (var entry : deltas.entrySet()) {
            String jobId = entry.getKey();
            ProgressDelta delta = entry.getValue();
            long b = delta.getAndResetBytes();
            int f = delta.getAndResetFiles();
            if (b > 0 || f > 0) {
                result.put(jobId, new long[]{b, f});
            }
        }
        return result;
    }

    public long[] drainJob(String jobId) {
        if (jobId == null) return null;
        ProgressDelta delta = deltas.get(jobId);
        if (delta != null) {
            long b = delta.getAndResetBytes();
            int f = delta.getAndResetFiles();
            if (b > 0 || f > 0) {
                return new long[]{b, f};
            }
        }
        return null;
    }

    public boolean hasPendingProgress(String jobId) {
        if (jobId == null) return false;
        ProgressDelta delta = deltas.get(jobId);
        return delta != null && !delta.isEmpty();
    }

    public void clear() {
        deltas.clear();
    }
}

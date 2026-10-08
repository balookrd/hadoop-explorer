"""Метрики Prometheus для сервиса Hadoop gRPC Replicator."""

from prometheus_client import Counter, Gauge, Histogram

# Суммарный объем переданных байтов
replication_bytes_total = Counter(
    "replication_bytes_total",
    "Суммарный объем успешно переданных байт репликации",
    ["status"],
)

# Количество активных воркеров
active_workers = Gauge(
    "active_workers",
    "Количество активных воркеров, выполняющих репликацию",
)

# Количество задач репликации по статусу и режиму выполнения
replication_jobs_total = Counter(
    "replication_jobs_total",
    "Количество созданных задач репликации",
    ["status", "mode"],
)

# Суммарное время задержки троттлинга (Token Bucket sleep)
throttling_delay_seconds_total = Counter(
    "throttling_delay_seconds_total",
    "Суммарное время ожидания воркеров из-за троттлинга полосы пропускания (сек)",
)

# Размер передаваемых файлов
file_size_bytes_histogram = Histogram(
    "replication_file_size_bytes",
    "Распределение размеров реплицируемых файлов",
    buckets=[1024 * 1024, 10 * 1024 * 1024, 100 * 1024 * 1024, 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024],
)

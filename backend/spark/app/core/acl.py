from typing import List
from app.core.config import settings, SparkClusterConfig
from app.core.security import UserSession


def check_cluster_access(user: UserSession, cluster: SparkClusterConfig) -> bool:
    if user.is_admin:
        return True
    allowed_groups = set(cluster.acl.allowed_groups)
    if "*" in allowed_groups or (set(user.groups) & allowed_groups):
        return True
    if "*" in cluster.acl.allowed_users or user.username in cluster.acl.allowed_users:
        return True
    return False


def check_yarn_queue_access(user: UserSession, cluster: SparkClusterConfig, queue: str) -> bool:
    if queue not in cluster.yarn.allowed_queues:
        return False
    if user.is_admin:
        return True
    queue_rule = cluster.yarn.queue_acl.get(queue)
    if not queue_rule:
        return True
    allowed_groups = set(queue_rule.allowed_groups)
    if "*" in allowed_groups or (set(user.groups) & allowed_groups):
        return True
    if "*" in queue_rule.allowed_users or user.username in queue_rule.allowed_users:
        return True
    return False


def filter_allowed_clusters(user: UserSession) -> List[SparkClusterConfig]:
    return [c for c in settings.clusters if check_cluster_access(user, c)]

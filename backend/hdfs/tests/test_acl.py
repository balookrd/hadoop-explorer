import pytest
from app.core.config import AppSettings, ClusterRegistry
from app.models.cluster import ClusterConfig, ClusterAcl
from app.core.acl import can_access_ui, can_access_cluster, is_cluster_read_only, is_cluster_admin, get_visible_clusters


def test_acl_ui_access():
    # Пользователь из allowed_groups
    assert can_access_ui("user1", ["data-engineers"]) is True
    # Пользователь из посторонней группы
    assert can_access_ui("guest", ["random-group"]) is False
    # Админ
    assert can_access_ui("superadmin", ["hadoop-admins"]) is True


def test_cluster_acl_rules():
    cluster = ClusterConfig(
        id="c1",
        name="Test Cluster",
        webhdfs_urls=["http://localhost:9870/webhdfs/v1"],
        acl=ClusterAcl(
            allowed_groups=["analytics", "engineers"], read_only_groups=["analytics"], admin_groups=["hadoop-admins"]
        ),
    )

    # 1. Аналитик имеет доступ, но Read-Only
    assert can_access_cluster(cluster, "analyst1", ["analytics"]) is True
    assert is_cluster_read_only(cluster, "analyst1", ["analytics"]) is True
    assert is_cluster_admin(cluster, "analyst1", ["analytics"]) is False

    # 2. Инженер имеет доступ на запись
    assert can_access_cluster(cluster, "eng1", ["engineers"]) is True
    assert is_cluster_read_only(cluster, "eng1", ["engineers"]) is False
    assert is_cluster_admin(cluster, "eng1", ["engineers"]) is False

    # 3. Админ кластера
    assert can_access_cluster(cluster, "admin1", ["hadoop-admins"]) is True
    assert is_cluster_read_only(cluster, "admin1", ["hadoop-admins"]) is False
    assert is_cluster_admin(cluster, "admin1", ["hadoop-admins"]) is True

    # 4. Неавторизованный пользователь
    assert can_access_cluster(cluster, "stranger", ["other-group"]) is False


def test_visible_clusters():
    c1 = ClusterConfig(
        id="c1", name="Cluster 1", webhdfs_urls=["http://nn1:9870"], acl=ClusterAcl(allowed_groups=["grp1"])
    )
    c2 = ClusterConfig(
        id="c2", name="Cluster 2", webhdfs_urls=["http://nn2:9870"], acl=ClusterAcl(allowed_groups=["grp2"])
    )

    clusters = [c1, c2]
    # Пользователь с grp1 видит только c1
    visible = get_visible_clusters(clusters, "userA", ["grp1"])
    assert len(visible) == 1
    assert visible[0].id == "c1"

    # Пользователь с обеими группами видит оба
    visible_both = get_visible_clusters(clusters, "userB", ["grp1", "grp2"])
    assert len(visible_both) == 2

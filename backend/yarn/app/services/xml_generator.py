import logging
from typing import List, Dict, Optional, Set, Tuple
from datetime import datetime, timezone
from collections import defaultdict
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape
import defusedxml.ElementTree as DefusedET
from defusedxml.common import DefusedXmlException

from app.models.yarn import QueueDraftItem
from app.models.cluster import ClusterConfig

logger = logging.getLogger(__name__)

# Суффиксы свойств очереди, которыми управляет YARN Explorer
MANAGED_QUEUE_SUFFIXES = {
    "queues",
    "capacity",
    "maximum-capacity",
    "state",
    "user-limit-factor",
    "ordering-policy",
    "maximum-applications",
    "maximum-am-resource-percent",
    "max-parallel-apps",
    "maximum-application-lifetime",
    "accessible-node-labels",
    "default-node-label-expression",
}

GLOBAL_MANAGED_PROPERTIES = {
    "yarn.scheduler.capacity.queue-mappings",
    "yarn.scheduler.capacity.queue-mappings-override.enable",
}


def is_managed_queue_property(prop_name: str, queue_path: str) -> bool:
    """Проверяет, является ли свойство управляемым параметром для заданной очереди."""
    prefix = f"yarn.scheduler.capacity.{queue_path}."
    if not prop_name.startswith(prefix):
        return False
    suffix = prop_name[len(prefix):]
    if suffix in MANAGED_QUEUE_SUFFIXES:
        return True
    if suffix.startswith("accessible-node-labels."):
        parts = suffix.split(".")
        if len(parts) == 3 and parts[2] in ("capacity", "maximum-capacity"):
            return True
    return False


def _compute_managed_properties(
    queues: List[QueueDraftItem],
    cluster: ClusterConfig,
    resource_mode: Optional[str] = None,
    queue_mappings: Optional[str] = None,
    queue_mappings_override: Optional[bool] = None,
) -> Tuple[Dict[str, str], Set[str], Dict[str, List[str]]]:
    """
    Вычисляет целевые значения управляемых свойств и списки удаленных очередей.
    Возвращает:
      - managed_props: Dict[prop_name, prop_value]
      - deleted_paths: Set[str]
      - children_by_parent: Dict[parent_path, List[child_name]]
    """
    mode = resource_mode or cluster.resource_mode
    total_mem = cluster.total_resources.memory_mb
    total_cores = cluster.total_resources.vcores

    deleted_paths: Set[str] = {q.path for q in queues if q.action == "delete"}
    active_queues: Dict[str, QueueDraftItem] = {
        q.path: q for q in queues if q.action != "delete"
    }

    # Группируем дочерние очереди
    children_by_parent: Dict[str, List[str]] = defaultdict(list)
    for q in active_queues.values():
        if q.parent_path:
            children_by_parent[q.parent_path].append(q.name)

    managed_props: Dict[str, str] = {}

    # 1. Глобальные свойства маппингов
    effective_mappings = queue_mappings if queue_mappings is not None else getattr(cluster, "queue_mappings", None)
    if effective_mappings:
        managed_props["yarn.scheduler.capacity.queue-mappings"] = effective_mappings.strip()
        effective_override = queue_mappings_override if queue_mappings_override is not None else getattr(cluster, "queue_mappings_override", False)
        managed_props["yarn.scheduler.capacity.queue-mappings-override.enable"] = "true" if effective_override else "false"

    # 2. Свойства активных очередей
    for path, q in active_queues.items():
        prop_prefix = f"yarn.scheduler.capacity.{path}"

        # Список дочерних очередей
        if path in children_by_parent:
            child_names = ",".join(sorted(set(children_by_parent[path])))
            managed_props[f"{prop_prefix}.queues"] = child_names

        # Партиции (capacity и maximum-capacity)
        for part_name, part_config in q.partitions.items():
            queue_mode = getattr(q, "resource_mode", None)
            effective_mode = resource_mode or queue_mode or mode
            if effective_mode == "absolute" and path != "root":
                mem = part_config.memory_mb if part_config.memory_mb is not None else int(total_mem * (part_config.capacity / 100.0))
                cores = part_config.vcores if part_config.vcores is not None else int(total_cores * (part_config.capacity / 100.0))
                max_mem = part_config.max_memory_mb if part_config.max_memory_mb is not None else int(total_mem * (part_config.max_capacity / 100.0))
                max_cores = part_config.max_vcores if part_config.max_vcores is not None else int(total_cores * (part_config.max_capacity / 100.0))
                cap_str = f"[memory={mem},vcores={cores}]"
                max_cap_str = f"[memory={max_mem},vcores={max_cores}]"
            else:
                cap_str = f"{part_config.capacity}"
                max_cap_str = f"{part_config.max_capacity}"

            if part_name == "DEFAULT" or part_name == cluster.default_partition:
                managed_props[f"{prop_prefix}.capacity"] = cap_str
                managed_props[f"{prop_prefix}.maximum-capacity"] = max_cap_str
            else:
                label_prefix = f"{prop_prefix}.accessible-node-labels.{part_name}"
                managed_props[f"{label_prefix}.capacity"] = cap_str
                managed_props[f"{label_prefix}.maximum-capacity"] = max_cap_str

        # State
        managed_props[f"{prop_prefix}.state"] = q.state.value

        # User Limit Factor & Ordering Policy (только для leaf очередей)
        is_leaf = getattr(q, "is_leaf", True) and (path not in children_by_parent or len(children_by_parent[path]) == 0)
        if is_leaf and path != "root":
            ulf = getattr(q, "user_limit_factor", None)
            if ulf is not None:
                managed_props[f"{prop_prefix}.user-limit-factor"] = f"{ulf}"
            ordering_policy = getattr(q, "ordering_policy", None)
            if ordering_policy:
                managed_props[f"{prop_prefix}.ordering-policy"] = str(ordering_policy).lower()

        # Application Limits & Lifetimes
        max_apps = getattr(q, "max_applications", None)
        if max_apps is not None:
            managed_props[f"{prop_prefix}.maximum-applications"] = str(max_apps)

        max_am_percent = getattr(q, "max_am_resource_percent", None)
        if max_am_percent is not None:
            managed_props[f"{prop_prefix}.maximum-am-resource-percent"] = str(max_am_percent)

        max_parallel = getattr(q, "max_parallel_apps", None)
        if max_parallel is not None:
            managed_props[f"{prop_prefix}.max-parallel-apps"] = str(max_parallel)

        lifetime = getattr(q, "max_application_lifetime", None)
        if lifetime is not None:
            managed_props[f"{prop_prefix}.maximum-application-lifetime"] = str(lifetime)

        # Node Labels / Partitioning
        labels = getattr(q, "accessible_node_labels", None)
        if labels is not None:
            labels_str = ",".join(labels) if isinstance(labels, list) else str(labels)
            managed_props[f"{prop_prefix}.accessible-node-labels"] = labels_str
        elif path == "root" and cluster.partitions:
            # Для root по умолчанию доступны все метки кластера, либо '*'
            non_default = [p for p in cluster.partitions if p != "DEFAULT" and p != cluster.default_partition]
            if non_default:
                managed_props[f"{prop_prefix}.accessible-node-labels"] = "*"

        default_label = getattr(q, "default_node_label_expression", None)
        if default_label is not None:
            managed_props[f"{prop_prefix}.default-node-label-expression"] = str(default_label).strip()

    return managed_props, deleted_paths, children_by_parent


def _sanitize_xml_comment(text: str) -> str:
    """Очищает строку для безопасного включения в XML-комментарий."""
    if not text:
        return ""
    # В стандарте XML запрещено вхождение '--' внутри комментария.
    # Заменяем '--', а также экранируем угловые скобки для нейтрализации инъекций.
    cleaned = text.replace("--", "- -").replace(">", "&gt;").replace("<", "&lt;")
    return cleaned.strip()


def update_capacity_scheduler_xml(
    base_xml: str,
    queues: List[QueueDraftItem],
    cluster: ClusterConfig,
    generated_by: str = "system",
    comment: str = "",
    resource_mode: Optional[str] = None,
    queue_mappings: Optional[str] = None,
    queue_mappings_override: Optional[bool] = None,
) -> str:
    """
    Модифицирует существующий capacity-scheduler.xml:
    - Сохраняет все необрабатываемые параметры (например, resource-calculator, acl_*, приоритеты и др.).
    - Сохраняет комментарии XML и структуру файла.
    - Обновляет / добавляет только управляемые параметры.
    - Удаляет свойства удаленных очередей и исключает их из .queues родителя.
    """
    try:
        # Проверяем base_xml на безопасность от XXE, External Entities и Entity Expansion
        DefusedET.fromstring(base_xml)
        parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
        root = ET.fromstring(base_xml, parser=parser)
    except DefusedXmlException as dxe:
        logger.error(f"Попытка передачи небезопасного XML (XXE/Entity Injection): {dxe}")
        raise ValueError(f"Небезопасный XML документ: обнаружена попытка XXE/Entity Expansion ({dxe})")
    except Exception as e:
        logger.warning(f"Ошибка разбора base_xml: {e}. Переключаемся на генерацию с чистого листа.")
        return generate_capacity_scheduler_xml(
            queues=queues,
            cluster=cluster,
            generated_by=generated_by,
            comment=comment,
            resource_mode=resource_mode,
            queue_mappings=queue_mappings,
            queue_mappings_override=queue_mappings_override,
            base_xml=None
        )

    managed_props, deleted_paths, children_by_parent = _compute_managed_properties(
        queues=queues,
        cluster=cluster,
        resource_mode=resource_mode,
        queue_mappings=queue_mappings,
        queue_mappings_override=queue_mappings_override,
    )

    # 1. Обход существующих свойств в XML
    elements_to_remove = []
    found_managed_names: Set[str] = set()

    for elem in list(root):
        if elem.tag != "property":
            continue

        name_el = elem.find("name")
        val_el = elem.find("value")
        if name_el is None or not name_el.text:
            continue

        prop_name = name_el.text.strip()

        # Проверка: относится ли свойство к удаляемой очереди?
        should_delete = False
        for del_path in deleted_paths:
            del_prefix = f"yarn.scheduler.capacity.{del_path}"
            if prop_name == del_prefix or prop_name.startswith(f"{del_prefix}."):
                should_delete = True
                break

        if should_delete:
            elements_to_remove.append(elem)
            continue

        # Если это управляемое свойство, для которого есть новое значение
        if prop_name in managed_props:
            found_managed_names.add(prop_name)
            if val_el is not None:
                val_el.text = managed_props[prop_name]
            else:
                new_val_el = ET.SubElement(elem, "value")
                new_val_el.text = managed_props[prop_name]
        elif prop_name.startswith("yarn.scheduler.capacity."):
            # Проверяем, не является ли это свойством .queues, из которого нужно убрать удаленную очередь
            if prop_name.endswith(".queues"):
                parent_path = prop_name[len("yarn.scheduler.capacity."):-len(".queues")]
                if parent_path in children_by_parent:
                    # Актуализируем список детей из draft
                    child_names = ",".join(sorted(set(children_by_parent[parent_path])))
                    found_managed_names.add(prop_name)
                    if val_el is not None:
                        val_el.text = child_names
                elif any(del_p.startswith(f"{parent_path}.") for del_p in deleted_paths):
                    # Если в этом .queues были удаленные дети, отфильтруем их
                    current_children = [c.strip() for c in (val_el.text or "").split(",") if c.strip()]
                    del_names = {del_p.split(".")[-1] for del_p in deleted_paths if del_p.rsplit(".", 1)[0] == parent_path}
                    remaining = [c for c in current_children if c not in del_names]
                    if remaining:
                        if val_el is not None:
                            val_el.text = ",".join(sorted(remaining))
                    else:
                        # Детей больше нет
                        elements_to_remove.append(elem)

    # Удаляем помеченные элементы
    for elem in elements_to_remove:
        root.remove(elem)

    # 2. Добавляем новые управляемые свойства, которых не было в base_xml
    for prop_name, prop_val in managed_props.items():
        if prop_name not in found_managed_names:
            prop_elem = ET.SubElement(root, "property")
            name_elem = ET.SubElement(prop_elem, "name")
            name_elem.text = prop_name
            val_elem = ET.SubElement(prop_elem, "value")
            val_elem.text = str(prop_val)

    # 3. Форматируем отступы
    ET.indent(root, space="    ")
    xml_str = ET.tostring(root, encoding="utf-8").decode("utf-8")

    # 4. Формируем финальный XML с стандартным заголовком
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    safe_generated_by = _sanitize_xml_comment(generated_by)
    safe_comment = _sanitize_xml_comment(comment)

    header_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>',
        '',
        '<!--',
        f'  Generated by YARN Queue Explorer',
        f'  Cluster: {cluster.name} ({cluster.id})',
        f'  Generated at: {now}',
        f'  Generated by: {safe_generated_by}',
    ]
    if safe_comment:
        header_lines.append(f'  Comment: {safe_comment}')
    header_lines.extend([
        '-->',
        '',
    ])

    return "\n".join(header_lines) + xml_str + "\n"


def generate_capacity_scheduler_xml(
    queues: List[QueueDraftItem],
    cluster: ClusterConfig,
    generated_by: str = "system",
    comment: str = "",
    resource_mode: Optional[str] = None,
    queue_mappings: Optional[str] = None,
    queue_mappings_override: Optional[bool] = None,
    base_xml: Optional[str] = None,
) -> str:
    """
    Генерирует или модифицирует capacity-scheduler.xml.
    Если передан base_xml — модифицирует его с сохранением всех необрабатываемых параметров.
    Если base_xml отсутствует — генерирует валидный файл с чистого листа.
    """
    if base_xml and base_xml.strip():
        return update_capacity_scheduler_xml(
            base_xml=base_xml,
            queues=queues,
            cluster=cluster,
            generated_by=generated_by,
            comment=comment,
            resource_mode=resource_mode,
            queue_mappings=queue_mappings,
            queue_mappings_override=queue_mappings_override,
        )

    # Fallback: генерация с чистого листа
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    safe_generated_by = _sanitize_xml_comment(generated_by)
    safe_comment = _sanitize_xml_comment(comment)

    managed_props, _, _ = _compute_managed_properties(
        queues=queues,
        cluster=cluster,
        resource_mode=resource_mode,
        queue_mappings=queue_mappings,
        queue_mappings_override=queue_mappings_override,
    )

    properties: List[Tuple[str, str, str]] = []

    # Глобальный лимит по умолчанию (если не задан)
    if "yarn.scheduler.capacity.maximum-applications" not in managed_props:
        properties.append((
            "yarn.scheduler.capacity.maximum-applications",
            "10000",
            "Maximum number of applications in the system"
        ))

    for name, value in managed_props.items():
        properties.append((name, value, ""))

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>',
        '',
        '<!--',
        f'  Generated by YARN Queue Explorer',
        f'  Cluster: {cluster.name} ({cluster.id})',
        f'  Generated at: {now}',
        f'  Generated by: {safe_generated_by}',
    ]
    if safe_comment:
        lines.append(f'  Comment: {safe_comment}')
    lines.extend([
        '-->',
        '',
        '<configuration>',
    ])

    for name, value, description in properties:
        lines.append('')
        lines.append('  <property>')
        lines.append(f'    <name>{escape(name)}</name>')
        lines.append(f'    <value>{escape(str(value))}</value>')
        if description:
            lines.append(f'    <description>{escape(description)}</description>')
        lines.append('  </property>')

    lines.extend([
        '',
        '</configuration>',
        '',
    ])

    return '\n'.join(lines)

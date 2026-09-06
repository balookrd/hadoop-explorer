import logging
from typing import List, Dict
from collections import defaultdict

from app.models.yarn import QueueDraftItem, BranchBalance

logger = logging.getLogger(__name__)


def validate_queue_balance(
    queues: List[QueueDraftItem],
    resource_mode: str,
    partition: str,
    parent_capacity: float = 100.0,
) -> List[BranchBalance]:
    """
    Валидирует баланс очередей:
    - В процентном режиме: сумма capacity дочерних = 100% от родителя.
    - Раздельный учет RAM и vCPU.
    """
    children_by_parent: Dict[str, List[QueueDraftItem]] = defaultdict(list)
    for q in queues:
        if q.action == "delete":
            continue
        if q.parent_path:
            children_by_parent[q.parent_path].append(q)

    balances: List[BranchBalance] = []

    for parent_path, children in children_by_parent.items():
        total_cap = 0.0
        total_mem = 0
        total_vcores = 0
        has_mem = False
        has_vcores = False

        for child in children:
            part_config = child.partitions.get(partition)
            if part_config:
                total_cap += part_config.capacity
                if part_config.memory_mb is not None:
                    total_mem += part_config.memory_mb
                    has_mem = True
                if part_config.vcores is not None:
                    total_vcores += part_config.vcores
                    has_vcores = True

        expected = 100.0
        unallocated = expected - total_cap
        tolerance = 0.01

        if abs(unallocated) <= tolerance:
            status = "ok"
            is_balanced = True
            message = f"Баланс ОК: сумма дочерних = {total_cap:.2f}%"
        elif unallocated > tolerance:
            status = "underallocated"
            is_balanced = False
            message = f"Недораспределено: {unallocated:.2f}% не назначено (сумма = {total_cap:.2f}%)"
        else:
            status = "overallocated"
            is_balanced = False
            message = f"Переподписка: перебор на {abs(unallocated):.2f}% (сумма = {total_cap:.2f}%)"

        balances.append(BranchBalance(
            parent_path=parent_path,
            partition=partition,
            total_children_capacity=round(total_cap, 2),
            unallocated_capacity=round(unallocated, 2),
            is_balanced=is_balanced,
            status=status,
            message=message,
            total_children_memory_mb=total_mem if has_mem else None,
            total_children_vcores=total_vcores if has_vcores else None,
            ram_is_balanced=is_balanced,
            vcpu_is_balanced=is_balanced,
        ))

    return balances


def compute_balances_from_tree(root_queue, partition: str) -> List[BranchBalance]:
    """
    Вычисляет балансы для всех веток рекурсивно из дерева QueueNode.
    """
    balances = []

    def recurse(node):
        if node.children:
            total_cap = 0.0
            total_mem = 0
            total_vcores = 0
            has_mem = False
            has_vcores = False

            for child in node.children:
                part_config = child.partitions.get(partition)
                if part_config:
                    total_cap += part_config.capacity
                    if part_config.memory_mb is not None:
                        total_mem += part_config.memory_mb
                        has_mem = True
                    if part_config.vcores is not None:
                        total_vcores += part_config.vcores
                        has_vcores = True

            unallocated = 100.0 - total_cap
            tolerance = 0.01

            if abs(unallocated) <= tolerance:
                status = "ok"
                is_balanced = True
                message = f"Balance OK: children sum = {total_cap:.2f}%"
            elif unallocated > tolerance:
                status = "underallocated"
                is_balanced = False
                message = f"Underallocated: {unallocated:.2f}% unassigned (sum = {total_cap:.2f}%)"
            else:
                status = "overallocated"
                is_balanced = False
                message = f"Overallocated: excess {abs(unallocated):.2f}% (sum = {total_cap:.2f}%)"

            balances.append(BranchBalance(
                parent_path=node.path,
                partition=partition,
                total_children_capacity=round(total_cap, 2),
                unallocated_capacity=round(unallocated, 2),
                is_balanced=is_balanced,
                status=status,
                message=message,
                total_children_memory_mb=total_mem if has_mem else None,
                total_children_vcores=total_vcores if has_vcores else None,
                ram_is_balanced=is_balanced,
                vcpu_is_balanced=is_balanced,
            ))

            for child in node.children:
                recurse(child)

    recurse(root_queue)
    return balances

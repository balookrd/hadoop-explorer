# Ansible Role: `yarn_capacity_scheduler`

Роль для безопасной доставки и горячего применения конфигурационного файла `capacity-scheduler.xml` на узлы **Apache Hadoop YARN ResourceManager (RM)**.

## Режимы

Роли передается ровно одна из переменных:

- **`capacity_scheduler_patch_b64`** — так работает YARN Explorer. Это JSON-список правок (`set`, `remove`, `remove_prefix`, `list_add`, `list_remove`) только для свойств, которые меняет заявка. Роль читает файл, развернутый на RM, применяет к нему правки скриптом [`files/apply_capacity_patch.py`](files/apply_capacity_patch.py) и раскладывает результат. Остальные свойства (ACL очередей, глобальные лимиты, `resource-calculator` и т. п.) не трогаются. Explorer не может прочитать развернутый файл: ResourceManager не отдает его по REST. Поэтому полный файл он не генерирует.
- **`capacity_scheduler_xml_b64`** — полная замена файла (ручной или аварийный путь).

В режиме патча роль останавливается, если `capacity-scheduler.xml` различается на узлах RM: неясно, к какому файлу применять правки.

## Возможности
1. **Предварительная валидация (Pre-flight check)**: проверка синтаксической корректности XML через Python `xml.etree.ElementTree` на хосте Ansible Controller перед подключением к кластеру.
2. **Резервное копирование (Backup)**: автоматическое сохранение текущего файла конфигурации с таймстемпом в директорию бэкапов перед внесением изменений на всех нодах RM.
3. **Отказоустойчивое распространение**: файл синхронно раскладывается на все ноды RM (и Active, и Standby), сохраняя консистентность при HA Failover.
4. **Определение активного узла**: выполнение команды горячего применения очередей `yarn rmadmin -refreshQueues` строго на Active RM.
5. **Автоматический откат (Rollback)**: в случае сбоя при выполнении `refreshQueues` (например, синтаксическая или ресурсная ошибка очередей YARN) роль автоматически восстанавливает бэкап на всех нодах и повторно применяет старый конфиг.
6. **Kerberos SPNEGO**: автоматический выпуск Kerberos-тикета (`kinit`) перед вызовом команд YARN rmadmin.

## Требования
- Ansible >= 2.14
- Python 3 на целевых хостах; на Controller (Execution Environment AWX) — Python 3.9+ для `apply_capacity_patch.py`
- Пользователь SSH (например, `deployer`) с правами `sudo` на пользователя `yarn` без пароля (`deployer ALL=(yarn) NOPASSWD: ALL`)
- Права администратора YARN (`yarn.admin.acl`) для сервисного аккаунта в `yarn-site.xml`

## Переменные роли

| Переменная | По умолчанию | Описание |
|---|---|---|
| `hadoop_conf_dir` | `/etc/hadoop/conf` | Директория с конфигурационными файлами Hadoop |
| `hadoop_user` | `yarn` | Системный пользователь процесса YARN RM |
| `hadoop_group` | `hadoop` | Системная группа |
| `backup_dir` | `/etc/hadoop/conf/backups` | Директория для сохранения бэкапов |
| `kerberos_enabled` | `true` | Включена ли аутентификация по Kerberos |
| `yarn_keytab` | `/etc/security/keytabs/yarn.service.keytab` | Путь к keytab-файлу |
| `yarn_principal` | `yarn/{{ inventory_hostname }}@CORP.DOMAIN` | Kerberos Principal |
| `capacity_scheduler_patch_b64` | `""` | Base64-кодированный JSON-список правок от YARN Explorer |
| `capacity_scheduler_xml_b64` | `""` | Base64-кодированный `capacity-scheduler.xml` целиком (полная замена) |
| `applied_by` | `"system"` | Имя пользователя/сервиса, инициировавшего применение |
| `change_request_id` | `"manual"` | Идентификатор Change Request для аудита |
| `cluster_id` | `""` | Идентификатор кластера |

## Пример запуска из CLI

Применение правок (как это делает YARN Explorer):

```bash
PATCH_B64=$(echo '[{"op":"set","name":"yarn.scheduler.capacity.root.dev.maximum-capacity","value":"60"}]' | base64 -w 0)
ansible-playbook -i inventory.ini playbooks/deploy_capacity_scheduler.yml \
  -e "capacity_scheduler_patch_b64=${PATCH_B64}" \
  -e "applied_by=admin_user" \
  -e "change_request_id=42"
```

Полная замена файла:

```bash
XML_B64=$(base64 -w 0 capacity-scheduler.xml)
ansible-playbook -i inventory.ini playbooks/deploy_capacity_scheduler.yml \
  -e "capacity_scheduler_xml_b64=${XML_B64}" \
  -e "applied_by=admin_user" \
  -e "change_request_id=42"
```

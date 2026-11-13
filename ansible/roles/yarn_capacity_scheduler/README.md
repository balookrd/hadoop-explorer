# Ansible Role: `yarn_capacity_scheduler`

Роль раскладывает `capacity-scheduler.xml` на узлы Apache Hadoop YARN ResourceManager (RM) и применяет его командой `yarn rmadmin -refreshQueues` без перезапуска RM. При ошибке `refreshQueues` роль восстанавливает предыдущий файл.

## Режимы

Роли передается ровно одна из переменных:

- `capacity_scheduler_patch_b64` — так работает Artel. Это JSON-список правок (`set`, `remove`, `remove_prefix`, `list_add`, `list_remove`) только для свойств, которые меняет заявка. Роль читает файл, развернутый на RM, применяет к нему правки скриптом [`files/apply_capacity_patch.py`](files/apply_capacity_patch.py) и раскладывает результат. Остальные свойства (ACL очередей, глобальные лимиты, `resource-calculator` и т. п.) не трогаются. Artel не может прочитать развернутый файл: ResourceManager не отдает его по REST. Поэтому полный файл он не генерирует.
- `capacity_scheduler_xml_b64` — полная замена файла (ручной или аварийный путь).

В режиме патча роль останавливается, если `capacity-scheduler.xml` различается на узлах RM: неясно, к какому файлу применять правки.

## Порядок работы роли

1. Предварительная проверка (pre-flight): синтаксис итогового XML проверяется через Python `xml.etree.ElementTree` на Ansible Controller до подключения к узлам RM.
2. Резервная копия: на каждом узле RM текущий файл копируется с меткой времени в `backup_dir`.
3. Раскладка: новый файл копируется на все узлы RM, Active и Standby, чтобы после HA failover новый Active RM читал тот же файл.
4. Применение: `yarn rmadmin -refreshQueues` выполняется только на Active RM. Active RM определяется командой `yarn rmadmin -getServiceState <rm_id>`, где `<rm_id>` — переменная хоста в инвентаре. Если у хоста `rm_id` не задан, он считается активным и `refreshQueues` выполняется на нём.
5. Откат: если `refreshQueues` завершился ошибкой (например, YARN отклонил параметры очередей), роль восстанавливает резервную копию на всех узлах, повторно выполняет `refreshQueues` и завершает задачу с ошибкой.
6. Kerberos: при `kerberos_enabled: true` перед вызовом `yarn rmadmin` роль получает тикет через `kinit` из `yarn_keytab`.

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
| `capacity_scheduler_patch_b64` | `""` | Base64-кодированный JSON-список правок от Artel |
| `capacity_scheduler_xml_b64` | `""` | Base64-кодированный `capacity-scheduler.xml` целиком (полная замена) |
| `applied_by` | `"system"` | Имя пользователя/сервиса, инициировавшего применение |
| `change_request_id` | `"manual"` | Идентификатор Change Request для аудита |
| `cluster_id` | `""` | Идентификатор кластера |

## Пример запуска из CLI

Применение правок (как это делает Artel):

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

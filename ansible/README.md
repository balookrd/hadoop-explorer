# Ansible & AWX Automation for Pristan

В каталоге лежат плейбук и роль, которые доставляют `capacity-scheduler.xml` на узлы ResourceManager кластера Apache Hadoop YARN и применяют его. Плейбук запускается из Ansible AWX / Red Hat Ansible Automation Platform (Job Template) или из CLI.

## Структура каталога

```text
ansible/
├── inventory.example.ini          # Пример инвентаря кластеров YARN
├── playbooks/
│   └── deploy_capacity_scheduler.yml  # Playbook для Job Template в AWX
└── roles/
    └── yarn_capacity_scheduler/       # Роль для валидации, бэкапа, раскладки и применения
        ├── defaults/main.yml
        ├── files/
        │   └── apply_capacity_patch.py  # Применение JSON-правок к развернутому файлу
        ├── meta/main.yml
        ├── tasks/
        │   ├── backup.yml
        │   ├── deploy.yml
        │   ├── main.yml
        │   ├── patch.yml
        │   └── refresh.yml
        └── README.md
```

Настройка интеграции AWX и YARN Explorer описана в [docs/awx-yarn-deployment.md](../docs/awx-yarn-deployment.md).

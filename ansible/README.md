# Ansible & AWX Automation for Hadoop Explorer

Данный каталог содержит артефакты автоматизации доставки и применения конфигураций в Apache Hadoop YARN через Ansible и Ansible AWX / Red Hat Automation Platform.

## Структура каталога

```text
ansible/
├── inventory.example.ini          # Пример инвентаря кластеров YARN
├── playbooks/
│   └── deploy_capacity_scheduler.yml  # Playbook для Job Template в AWX
└── roles/
    └── yarn_capacity_scheduler/       # Роль для валидации, бэкапа, раскладки и применения
        ├── defaults/main.yml
        ├── meta/main.yml
        ├── tasks/
        │   ├── backup.yml
        │   ├── deploy.yml
        │   ├── main.yml
        │   └── refresh.yml
        └── README.md
```

Подробное руководство по настройке интеграции AWX и Hadoop Explorer находится в [docs/awx-yarn-deployment.md](../docs/awx-yarn-deployment.md).

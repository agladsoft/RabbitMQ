# RabbitMQ Data Processing System

[![Python](https://img.shields.io/badge/Python-3.8-blue.svg)](https://www.python.org/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.x-orange.svg)](https://www.rabbitmq.com/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-latest-green.svg)](https://clickhouse.com/)
[![Docker](https://img.shields.io/badge/Docker-latest-blue.svg)](https://www.docker.com/)

## 📋 Описание

Система обработки сообщений из RabbitMQ для загрузки данных в ClickHouse с автоматической отправкой статистики в Telegram. Проект предназначен для обработки различных типов отчетов и данных из множественных очередей RabbitMQ с последующей нормализацией и загрузкой в базу данных.

## 🏗️ Архитектура системы

Система состоит из следующих основных компонентов:

### Основные модули:
- **receive.py** - Основной модуль для получения и обработки сообщений из RabbitMQ
- **rabbit_mq.py** - Класс для работы с RabbitMQ (подключение, публикация, получение сообщений)
- **tables.py** - Модели данных для различных типов таблиц и их обработки
- **send2telegram.py** - Модуль отправки статистики в Telegram
- **delete_deals.py** - Модуль очистки устаревших данных

### Конфигурационные файлы:
- **queues_config.json** - Конфигурация очередей и routing keys
- **tables_config.json** - Маппинг русских названий таблиц на английские

## 📊 Функциональность

### Обработка данных:
- Получение сообщений из 35+ очередей RabbitMQ
- Парсинг JSON данных различных форматов
- Нормализация и валидация данных
- Преобразование типов данных (даты, числа, boolean)
- Обработка устаревших дат (< 1925-01-01)

### Загрузка в ClickHouse:
- Поддержка двух баз данных: `DataCore` и `DO`
- Batch-загрузка данных (по 5000 записей)
- Дедупликация данных
- Обработка ошибок и retry логика

### Мониторинг:
- Логирование всех операций
- Сохранение статистики в SQLite
- Ежедневная отправка отчетов в Telegram
- Обработка ошибок с детализацией

## 🚀 Быстрый старт

### Предварительные требования

- Python 3.8+
- Docker и Docker Compose
- RabbitMQ сервер
- ClickHouse сервер
- Telegram бот (для уведомлений)

### Установка и настройка

1. **Клонирование репозитория:**
```bash
git clone <repository-url>
cd RabbitMQ
```

2. **Создание виртуального окружения:**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate  # Windows
```

3. **Установка зависимостей:**
```bash
pip install -r requirements.txt
```

4. **Настройка переменных окружения:**
Создайте файл `.env` в корне проекта:
```env
# RabbitMQ настройки
RABBITMQ_USER=your_username
RABBITMQ_PASSWORD=your_password
RABBITMQ_HOST=rabbitmq_host
RABBITMQ_PORT=5672
EXCHANGE_NAME=your_exchange_name

# ClickHouse настройки
HOST=clickhouse_host
DATABASE=your_database
USERNAME_DB=your_username
PASSWORD=your_password

# Telegram настройки
TOKEN_TELEGRAM=your_bot_token
CHAT_ID=your_chat_id
TOPIC=your_topic_id
MESSAGE_ID=your_message_id
HOST_HOSTNAME=your_server_name

# Пути
XL_IDP_PATH_RABBITMQ=/path/to/rabbitmq/files
XL_IDP_ROOT_RABBITMQ=/path/to/project/root
```

## 🔧 Сборка и развертывание

### Локальная разработка

1. **Запуск основного процесса обработки:**
```bash
python scripts/receive.py
```

2. **Тестовая отправка сообщений:**
```bash
python scripts/send.py
```

3. **Отправка статистики в Telegram:**
```bash
python scripts/send2telegram.py
```

### Docker развертывание

1. **Сборка основного контейнера:**
```bash
docker build -t rabbitmq-processor .
```

2. **Сборка контейнера с cron:**
```bash
docker build -f Dockerfile_cron -t rabbitmq-cron .
```

3. **Запуск с помощью docker-compose:**

**Создание docker-compose.yml:**
```yaml
version: '3.9'

services:
  rabbitmq:
    container_name: rabbitmq
    restart: always
    ports:
      - "8150:8150"
    volumes:
      - ${XL_IDP_PATH_RABBITMQ_SCRIPTS}:${XL_IDP_PATH_DOCKER}
      - ${XL_IDP_ROOT_RABBITMQ}:${XL_IDP_PATH_RABBITMQ}
    environment:
      TZ: Europe/Moscow
      XL_IDP_ROOT_RABBITMQ: ${XL_IDP_PATH_DOCKER}
      XL_IDP_PATH_RABBITMQ: ${XL_IDP_PATH_RABBITMQ}
      TOKEN_TELEGRAM: ${TOKEN_TELEGRAM}
      HOST_HOSTNAME: 127.0.0.1
    build:
      context: RabbitMQ
      dockerfile: ./Dockerfile
      args:
        XL_IDP_PATH_DOCKER: ${XL_IDP_PATH_DOCKER}
    logging:
      driver: "json-file"
      options:
        max-size: "20m"
        max-file: "3"
    command:
      bash -c "export PYTHONPATH="${XL_IDP_PATH_DOCKER}:${PYTHONPATH}" && python3 ${XL_IDP_PATH_DOCKER}/scripts/receive.py"
    networks:
      - postgres

  cron_rabbitmq:
    container_name: cron_rabbitmq
    restart: always
    ports:
      - "8152:8152"
    volumes:
      - ${XL_IDP_PATH_RABBITMQ_SCRIPTS}:${XL_IDP_PATH_DOCKER}
      - ${XL_IDP_ROOT_RABBITMQ}:${XL_IDP_PATH_RABBITMQ}
    environment:
      TZ: Europe/Moscow
      XL_IDP_ROOT_RABBITMQ: ${XL_IDP_PATH_DOCKER}
      XL_IDP_PATH_RABBITMQ: ${XL_IDP_PATH_RABBITMQ}
      TOKEN_TELEGRAM: ${TOKEN_TELEGRAM}
      HOST_HOSTNAME: 127.0.0.1
    build:
      context: RabbitMQ
      dockerfile: ./Dockerfile_cron
      args:
        XL_IDP_PATH_DOCKER: ${XL_IDP_PATH_DOCKER}
    networks:
      - postgres
```

```bash
docker-compose up -d
```

## 📝 Конфигурация

### Очереди RabbitMQ
Настройка очередей в `config/queues_config.json`:
```json
{
    "DC_QUEUE_NAME": "DC_ROUTING_KEY",
    "DO_QUEUE_NAME": "DO_ROUTING_KEY"
}
```

### Таблицы ClickHouse
Маппинг таблиц в `config/tables_config.json`:
```json
{
    "РусскоеНазваниеТаблицы": "english_table_name"
}
```

## 🧪 Тестирование

### Запуск тестов
```bash
# Установка тестовых зависимостей
pip install pytest pytest-rabbitmq pytest-mock

# Запуск тестов
pytest tests/

# Запуск с покрытием
pytest --cov=scripts tests/
```

### Тестовые данные
Используйте файл `config/test_deal.json` для создания тестовых сообщений:
```bash
python scripts/send.py
```

## 📊 Мониторинг и логирование

### Логи
- Файлы логов: `logging/receive_YYYY-MM-DD.log`
- Статистика: `logging/processed_messages.db`
- Ротация логов: максимум 20MB, 3 backup файла

### Telegram уведомления
- Ежедневная отправка статистики в 20:00
- Детализация по очередям и таблицам
- Уведомления об ошибках

### Cron задачи
- `20:00` - Отправка статистики в Telegram
- `21:00` - Очистка устаревших данных (> 7 дней)

## 🔍 Поддерживаемые типы данных

### Обрабатываемые очереди:
- **DataCore** (DC_*): Основные бизнес-данные
- **DO** (DO_*): Справочные данные

### Типы отчетов:
- Отчеты по сделкам и заказам
- Справочники контрагентов
- Логистические данные
- Финансовые отчеты
- Операционные метрики

## 🛠️ Разработка

### Структура проекта
```
RabbitMQ/
├── config/                 # Конфигурационные файлы
├── scripts/                # Основные модули
│   ├── receive.py         # Получение сообщений
│   ├── rabbit_mq.py       # RabbitMQ клиент
│   ├── tables.py          # Модели данных
│   └── send2telegram.py   # Telegram уведомления
├── tests/                 # Тесты
│   ├── test_receive.py    # Тестирование получения сообщений
├── logging/               # Логи и статистика
├── requirements.txt       # Зависимости
└── README.md             # Документация
```

### Добавление новой очереди
1. Добавьте конфигурацию в `config/queues_config.json`
2. Добавьте маппинг таблицы в `config/tables_config.json`
3. Создайте класс обработки в `scripts/tables.py`
4. Добавьте новый класс в переменную `CLASSES` в `scripts/receive.py`, соблюдая такой же порядок, как и в `config/tables_config.json`

### Добавление новой таблицы
1. Наследуйте от `DataCoreClient` или создайте новый базовый класс
2. Реализуйте методы `get_table_columns()` и `change_columns()`
3. Настройте обработку специфичных типов данных

## 📈 Производительность

- **Batch размер**: 5000 записей
- **Concurrent queues**: До 10 одновременно
- **Retry логика**: 3 попытки с экспоненциальным backoff
- **Heartbeat**: 600 секунд для RabbitMQ

## 🔧 Troubleshooting

### Частые проблемы:

1. **Ошибка подключения к RabbitMQ**
   - Проверьте настройки в `.env`
   - Убедитесь, что RabbitMQ доступен

2. **Ошибка подключения к ClickHouse**
   - Проверьте credentials
   - Убедитесь в доступности базы данных

3. **Ошибки парсинга данных**
   - Проверьте логи в `logging/`
   - Убедитесь в корректности JSON структуры

### Полезные команды:
```bash
# Проверка статуса очередей
python -c "from scripts.rabbit_mq import RabbitMQ; rmq = RabbitMQ(); print('Connected successfully')"

# Очистка логов
rm -f logging/*.log

# Проверка статистики
sqlite3 logging/processed_messages.db "SELECT * FROM stats;"
```
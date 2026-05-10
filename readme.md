markdown
# Docker & Kafka Transaction Processing System

## 📋 О проекте

Тестовый проект для изучения Docker, Kafka и микросервисной архитектуры на C#. 
Система обработки финансовых транзакций с асинхронным обменом сообщениями через Kafka.

### Архитектура
[HTTP API Gateway] → [Kafka] → [Transaction Processor] → [PostgreSQL]
(Producer) (Consumer)

text

### Компоненты

| Компонент | Технология | Описание |
|-----------|-----------|----------|
| Transaction API | .NET 8 + ASP.NET Core | REST API для запуска/остановки транзакций (Producer) |
| Kafka | Confluent Kafka | Брокер сообщений |
| Transaction Processor | .NET 8 + EF Core | Обработчик транзакций (Consumer) |
| PostgreSQL 14 | База данных | Хранение транзакций |

## 🚀 Быстрый старт

### Требования

- Docker Desktop 4.20+
- .NET 8 SDK
- Git

### Запуск

```bash
# Клонировать репозиторий
git clone <repository-url>
cd docker-kafka-project

# Запустить все сервисы
docker-compose up -d

# Проверить статус
docker-compose ps
Проверка работы
bash
# Health check
curl http://localhost:5000/health

# Создать транзакцию
curl -X POST http://localhost:5000/api/transaction/start \
  -H "Content-Type: application/json" \
  -d '{"amount": 1000, "currency": "RUB"}'

# Остановить транзакцию (используйте полученный ID)
curl -X POST http://localhost:5000/api/transaction/stop/{transactionId} \
  -H "Content-Type: application/json" \
  -d '{"reason": "completed"}'


📚 Полное руководство по командам


1. Docker базовые команды
Управление контейнерами

bash

# Показать все запущенные контейнеры
docker ps

# Показать все контейнеры (включая остановленные)
docker ps -a

# Запустить/остановить/перезапустить контейнер
docker start <container_name>
docker stop <container_name>
docker restart <container_name>

# Удалить контейнер
docker rm <container_name>

# Удалить все остановленные контейнеры
docker container prune

# Войти в контейнер (интерактивный режим)
docker exec -it <container_name> bash
docker exec -it <container_name> sh

# Просмотр логов контейнера
docker logs <container_name>
docker logs <container_name> --tail 50
docker logs <container_name> -f  # следить в реальном времени
Управление образами
bash
# Показать все образы
docker images

# Удалить образ
docker rmi <image_name_or_id>

# Удалить все неиспользуемые образы
docker image prune -a

# Собрать образ из Dockerfile
docker build -t <image_name> <path_to_dockerfile>

# Просмотр истории образа
docker history <image_name>
Сети и тома
bash
# Показать все сети и тома
docker network ls
docker volume ls

# Удалить неиспользуемые тома
docker volume prune

# Создать сеть
docker network create <network_name>

# Подключить контейнер к сети
docker network connect <network_name> <container_name>
2. Docker Compose команды
Основные команды
bash
# Запуск всех сервисов в фоновом режиме
docker-compose up -d

# Запуск с пересборкой образов
docker-compose up -d --build

# Запуск в интерактивном режиме (видеть логи)
docker-compose up

# Остановка всех сервисов
docker-compose down

# Остановка с удалением томов (удаляются данные БД!)
docker-compose down -v

# Остановка с удалением всех ресурсов
docker-compose down --rmi all -v

# Перезапуск всех сервисов или конкретного
docker-compose restart
docker-compose restart <service_name>
Работа с сервисами
bash
# Показать статус сервисов
docker-compose ps

# Показать логи всех сервисов
docker-compose logs

# Показать логи конкретного сервиса
docker-compose logs <service_name>
docker-compose logs <service_name> --tail 50
docker-compose logs -f <service_name>  # следить в реальном времени

# Выполнить команду в сервисе
docker-compose exec <service_name> <command>
docker-compose exec postgres psql -U admin -d transactions_db

# Масштабирование сервиса
docker-compose up -d --scale transaction-processor=3
Сборка
bash
# Собрать образы без запуска
docker-compose build

# Собрать конкретный сервис
docker-compose build <service_name>

# Собрать без использования кэша
docker-compose build --no-cache

# Пересобрать и запустить
docker-compose up -d --build
Проверка конфигурации
bash
# Проверить синтаксис docker-compose.yml
docker-compose config

# Показать переменные окружения
docker-compose config --services

3. Kafka команды

Управление топиками

bash
# Список всех топиков
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Создать топик
docker exec kafka kafka-topics --create \
  --topic <topic_name> \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Описание топика
docker exec kafka kafka-topics --describe \
  --topic <topic_name> \
  --bootstrap-server localhost:9092

# Удалить топик
docker exec kafka kafka-topics --delete \
  --topic <topic_name> \
  --bootstrap-server localhost:9092

# Изменить количество партиций
docker exec kafka kafka-topics --alter \
  --topic <topic_name> \
  --partitions 6 \
  --bootstrap-server localhost:9092
Публикация и потребление сообщений
bash
# Консольный producer (отправка сообщений)
docker exec -it kafka kafka-console-producer \
  --topic transactions \
  --bootstrap-server localhost:9092

# Консольный consumer (чтение сообщений с начала)
docker exec -it kafka kafka-console-consumer \
  --topic transactions \
 --bootstrap-server localhost:9092 \
  --from-beginning

# Consumer с указанием группы
docker exec -it kafka kafka-console-consumer \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --group my-group

# Consumer последних сообщений
docker exec -it kafka kafka-console-consumer \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --max-messages 10
Просмотр информации о группах
bash
# Список групп consumer'ов
docker exec kafka kafka-consumer-groups \
  --list \
  --bootstrap-server localhost:9092

# Описание группы (смещения)
docker exec kafka kafka-consumer-groups \
  --describe \
  --group transaction-processor-group \
  --bootstrap-server localhost:9092

# Сброс смещения для группы
docker exec kafka kafka-consumer-groups \
  --reset-offsets \
  --group transaction-processor-group \
  --topic transactions \
  --to-earliest \
  --execute \
  --bootstrap-server localhost:9092

Статистика мониторинг

bash

# Показать количество сообщений в топике
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic transactions

4. PostgreSQL команды

Подключение и базовые операции

bash
# Подключиться к PostgreSQL
docker exec -it postgres psql -U admin -d transactions_db

# Выполнить SQL запрос без входа в psql
docker exec -it postgres psql -U admin -d transactions_db -c "SELECT * FROM transactions;"

# Список баз данных
docker exec -it postgres psql -U admin -c "\l"

# Список таблиц
docker exec -it postgres psql -U admin -d transactions_db -c "\dt"

# Описание таблицы
docker exec -it postgres psql -U admin -d transactions_db -c "\d transactions"
Полезные SQL запросы
sql
-- Просмотр всех транзакций
SELECT * FROM transactions;

-- Просмотр всех событий
SELECT * FROM transaction_events;

-- Статистика по статусам
SELECT status, COUNT(*) FROM transactions GROUP BY status;

-- Транзакции со всеми событиями
SELECT t.transaction_id, t.status, e.event_type, e.event_time
FROM transactions t
LEFT JOIN transaction_events e ON t.transaction_id = e.transaction_id
ORDER BY e.event_time DESC;

-- Количество событий по транзакциям
SELECT transaction_id, COUNT(*) as event_count
FROM transaction_events
GROUP BY transaction_id;
Резервное копирование
bash

# Дамп базы данных

docker exec postgres pg_dump -U admin transactions_db > backup.sql

# Восстановление из дампа
docker exec -i postgres psql -U admin transactions_db < backup.sql

# Дамп только схемы
docker exec postgres pg_dump -U admin --schema-only transactions_db > schema.sql

5. .NET команды

Сборка и запуск

bash

# Восстановить зависимости
dotnet restore

# Собрать проект
dotnet build

# Собрать в Release конфигурации
dotnet build -c Release

# Запустить проект
dotnet run

# Запустить с определенной конфигурацией
dotnet run --configuration Release

# Очистить проект
dotnet clean
EF Core миграции
bash
# Создать миграцию
dotnet ef migrations add InitialCreate
dotnet ef migrations add <MigrationName>

# Применить миграции
dotnet ef database update

# Откатить миграцию
dotnet ef database update 0

# Удалить последнюю миграцию
dotnet ef migrations remove

# Показать список миграций
dotnet ef migrations list

# Сгенерировать SQL скрипт миграции
dotnet ef migrations script

Тестирование публикации

bash

# Публикация приложения
dotnet publish -c Release -o publish

# Запуск из опубликованных файлов
dotnet publish/TransactionAPI.dll

6. Сборка и очистка

Docker очистка
bash
# Показать использование диска Docker
docker system df

# Детальная информация
docker system df -v

# Удалить все неиспользуемые контейнеры, сети, образы
docker system prune

# Удалить всё включая тома (осторожно!)
docker system prune -a --volumes

# Очистить все остановленные контейнеры
docker container prune -f

# Очистить неиспользуемые образы
docker image prune -a -f

# Очистить кэш сборщика
docker builder prune -a -f
Полная переустановка проекта
bash
# 1. Остановить всё
docker-compose down -v

# 2. Удалить образы
docker rmi docker-kafka-project-transaction-api -f
docker rmi docker-kafka-project-transaction-processor -f

# 3. Очистить систему
docker system prune -a --volumes --force

# 4. Перезапустить Docker Desktop
# (правой кнопкой на иконке -> Restart)

# 5. Пересобрать
docker-compose build --no-cache

# 6. Запустить
docker-compose up -d
7. Диагностика и отладка
Проверка состояния
bash
# Статус всех контейнеров
docker-compose ps

# Использование ресурсов контейнерами
docker stats

# Сеть между контейнерами
docker network inspect docker-kafka-project_kafka-net

# Проверка подключения между контейнерами
docker exec transaction-api ping kafka
docker exec transaction-api nc -zv kafka 9092
Логирование
bash
# Логи всех сервисов
docker-compose logs

# Логи с таймстемпами
docker-compose logs --timestamps

# Логи с конца
docker-compose logs --tail 100

# Логи в реальном времени с фильтром
docker-compose logs -f | Select-String "ERROR"

# Логи одного сервиса
docker logs transaction-api --tail 50 -f
Health Check
bash
# Проверка API
curl http://localhost:5000/health

# Проверка Swagger
curl http://localhost:5000/swagger

# Проверка PostgreSQL
docker exec postgres pg_isready -U admin

# Проверка Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
Проверка портов
bash
# Какие порты слушает хост
netstat -ano | findstr :5000
netstat -ano | findstr :5432
netstat -ano | findstr :9092

# Убить процесс на порту (Windows PowerShell)
$pid = (Get-NetTCPConnection -LocalPort 5000).OwningProcess
Stop-Process -Id $pid -Force
8. Работа с диском Docker
Проблема и решение
При активной разработке Docker накапливает:

Старые образы (<none>:<none>)

Кэш сборщика (BuildKit)

Временные слои

Команды для очистки места
bash
# Быстрая очистка (безопасно)
docker system prune -a --volumes

# Очистить только висячие образы
docker image prune

# Очистить кэш сборщика
docker builder prune -a --force
Для Windows с WSL2 (уменьшение VHDX)
bash
# 1. Остановить WSL
wsl --shutdown

# 2. Оптимизировать VHDX (в PowerShell от администратора)
optimize-vhd -Path "$env:LOCALAPPDATA\Docker\wsl\disk\docker_data.vhdx" -Mode Full

# Или через diskpart
diskpart
select vdisk file="$env:LOCALAPPDATA\Docker\wsl\disk\docker_data.vhdx"
compact vdisk
exit
Настройка автоматической очистки
Создайте файл ~/.docker/daemon.json:

json
{
  "builder": {
    "gc": {
      "enabled": true,
      "defaultKeepStorage": "20GB",
      "policy": [
        {"keepStorage": "10GB", "filter": ["unused-for=168h"]},
        {"keepStorage": "5GB", "filter": ["unused-for=24h"]}
      ]
    }
  }
}
После создания файла перезапустите Docker Desktop.


Вот пошаговая инструкция, которая гарантированно сработает на вашей Windows 10, 11 Pro:

Шаг 1. Остановите всё
Если файл будет занят системой, мы его не сожмем.

Выйдите из Docker Desktop (нажмите правой кнопкой мыши на иконке в трее -> Quit Docker Desktop).

Запустите PowerShell или Командную строку от имени Администратора и выполните команду завершения WSL:

powershell
wsl --shutdown
Шаг 2. Найдите путь к файлу .vhdx
Это самый важный момент. У разных версий Docker путь может отличаться. Проверьте по очереди, какой файл у вас реально существует:

Самый частый путь (современные версии):
C:\Users\Алексей\AppData\Local\Docker\wsl\data\ext4.vhdx

Старый путь (Legacy):
C:\Users\Алексей\AppData\Local\Docker\wsl\disk\docker_data.vhdx

Шаг 3. Запустите сжатие через DiskPart
Вам нужно будет ввести команды вручную в открывшемся окне.

В той же PowerShell (администратор) наберите diskpart и нажмите Enter. Откроется новое черное окно с префиксом DISKPART>.

Последовательно введите следующие команды, подставив свой путь (обязательно в двойных кавычках):

text
select vdisk file="C:\Users\Алексей\AppData\Local\Docker\wsl\data\ext4.vhdx"
attach vdisk readonly
compact vdisk
detach vdisk
exit
Объяснение: attach vdisk readonly — присоединяем диск в режиме "только чтение" (это безопасно). compact vdisk — запускаем сжатие. Это может занять 5–15 минут, дождитесь завершения


🧪 Тестирование API
Создание и остановка транзакций
bash
# Создать транзакцию
curl -X POST http://localhost:5000/api/transaction/start \
  -H "Content-Type: application/json" \
  -d '{"amount": 1000, "description": "Test transaction"}'

# Остановить транзакцию (подставьте полученный ID)
curl -X POST http://localhost:5000/api/transaction/stop/ВАШ_ID \
  -H "Content-Type: application/json" \
  -d '{"reason": "completed"}'
Автоматические тесты
test.ps1 (PowerShell):

powershell
Write-Host "🚀 Starting test suite..." -ForegroundColor Green

# Создание транзакции
$response = curl -X POST http://localhost:5000/api/transaction/start `
  -H "Content-Type: application/json" `
  -d '{"amount": 100}' | ConvertFrom-Json

$transactionId = $response.transactionId
Write-Host "✅ Created: $transactionId"

Start-Sleep 2

# Остановка транзакции
curl -X POST "http://localhost:5000/api/transaction/stop/$transactionId" `
  -H "Content-Type: application/json" `
  -d '{}'

Write-Host "✅ Completed"
check.ps1 (PowerShell):

powershell
Write-Host "=== Checking services ===" -ForegroundColor Cyan

# Проверка API
try {
    $response = curl http://localhost:5000/health
    Write-Host "✅ API: OK" -ForegroundColor Green
} catch {
    Write-Host "❌ API: FAILED" -ForegroundColor Red
}

# Проверка PostgreSQL
$pgStatus = docker exec postgres pg_isready -U admin 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ PostgreSQL: OK" -ForegroundColor Green
} else {
    Write-Host "❌ PostgreSQL: FAILED" -ForegroundColor Red
}

# Проверка Kafka
$kafkaTopics = docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Kafka: OK" -ForegroundColor Green
} else {
    Write-Host "❌ Kafka: FAILED" -ForegroundColor Red
}
🐛 Типичные проблемы и решения
Ошибка: "environment variable KAFKA_PROCESS_ROLES is not set"
Решение: Используйте старую версию Kafka (7.3.0) или Bitnami образ:

yaml
kafka:
  image: confluentinc/cp-kafka:7.3.0
  # или
  image: bitnami/kafka:3.4
Ошибка: "Connection refused" к Kafka
Причина: Приложения внутри контейнеров пытаются подключиться к localhost:9092

Решение: В appsettings.json внутри контейнеров используйте имя сервиса:

json
"Kafka": {
  "BootstrapServers": "kafka:9092"  // внутри Docker
  // для локального запуска: "localhost:9092"
}
Ошибка: "Failed to load configuration from file '/app/appsettings.json'"
Решение: Убедитесь, что appsettings.json скопирован в Dockerfile:

dockerfile
COPY --from=build /src/appsettings.json .
COPY --from=build /src/appsettings.Development.json .
Контейнер Kafka постоянно падает
Решение: Очистите и перезапустите:

bash
docker-compose down -v
docker system prune -a --volumes
docker-compose up -d
📊 Полезные скрипты
Скрипт полной проверки системы
powershell
# full_check.ps1
Write-Host "=== DOCKER & KAFKA SYSTEM CHECK ===" -ForegroundColor Cyan
Write-Host ""

Write-Host "🔍 Containers Status:" -ForegroundColor Yellow
docker-compose ps

Write-Host "`n🔍 Docker Disk Usage:" -ForegroundColor Yellow
docker system df

Write-Host "`n🔍 Service Endpoints:" -ForegroundColor Yellow
Write-Host "Transaction API: http://localhost:5000"
Write-Host "Swagger: http://localhost:5000/swagger"
Write-Host "PostgreSQL: localhost:5432"
Write-Host "Kafka: localhost:9092"

Write-Host "`n🔍 Quick Test:" -ForegroundColor Yellow
curl -s http://localhost:5000/health
Скрипт полной очистки
powershell
# clean_all.ps1
Write-Host "⚠️  WARNING: This will delete ALL Docker data!" -ForegroundColor Red
$confirm = Read-Host "Type 'yes' to continue"

if ($confirm -eq 'yes') {
    Write-Host "Stopping all containers..." -ForegroundColor Yellow
    docker-compose down -v
    
    Write-Host "Removing containers..." -ForegroundColor Yellow
    docker container prune -f
    
    Write-Host "Removing images..." -ForegroundColor Yellow
    docker image prune -a -f
    
    Write-Host "Removing volumes..." -ForegroundColor Yellow
    docker volume prune -f
    
    Write-Host "Cleaning builder cache..." -ForegroundColor Yellow
    docker builder prune -a -f
    
    Write-Host "✅ Cleanup complete!" -ForegroundColor Green
} else {
    Write-Host "Operation cancelled" -ForegroundColor Yellow
}
📝 Краткая шпаргалка
Что нужно	Команда
Запустить всё	docker-compose up -d
Остановить всё	docker-compose down
Полностью очистить	docker system prune -a --volumes
Посмотреть логи	docker-compose logs -f <service>
Войти в контейнер	docker exec -it <name> bash
Список топиков Kafka	docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
Подключиться к PostgreSQL	docker exec -it postgres psql -U admin -d transactions_db
Пересобрать проект	docker-compose up -d --build
Проверить API	curl http://localhost:5000/health
Очистить образы	docker image prune -a
Показать использование диска	docker system df
📂 Структура проекта
text
docker-kafka-project/
├── docker-compose.yml
├── TransactionAPI/
│   ├── Dockerfile
│   ├── TransactionAPI.csproj
│   ├── Program.cs
│   ├── appsettings.json
│   ├── Controllers/
│   │   └── TransactionController.cs
│   ├── Models/
│   │   ├── TransactionEvent.cs
│   │   └── TransactionEventForKafka.cs
│   └── Services/
│       └── KafkaProducerService.cs
├── TransactionProcessor/
│   ├── Dockerfile
│   ├── TransactionProcessor.csproj
│   ├── Program.cs
│   ├── appsettings.json
│   ├── Models/
│   │   ├── Transaction.cs
│   │   ├── TransactionEvent.cs
│   │   ├── TransactionDbContext.cs
│   │   └── KafkaEvent.cs
│   └── Services/
│       ├── KafkaConsumerService.cs
│       └── TransactionService.cs
└── init-db/
    └── init.sql
🔗 Полезные ссылки
Docker Documentation

Apache Kafka Documentation

.NET Documentation

PostgreSQL Documentation

Entity Framework Core

Примечание: Заменяйте <service_name>, <container_name>, <topic_name> на ваши реальные имена. Все команды проверены и работают в Windows 10/11 с Docker Desktop.
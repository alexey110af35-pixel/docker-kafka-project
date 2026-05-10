-- Создаем базу данных если не существует
SELECT 'CREATE DATABASE transactions_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'transactions_db')\gexec

-- Подключаемся к БД
\c transactions_db;

-- Далее таблицы создадутся через EF Core Migrations
-- Но для инициализации можем создать пользователя
GRANT ALL PRIVILEGES ON DATABASE transactions_db TO admin;
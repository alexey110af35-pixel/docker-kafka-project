FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Копируем csproj
COPY TransactionAPI.csproj .
RUN dotnet restore

# Копируем ВСЁ
COPY . .

# Публикуем
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

# Копируем опубликованные файлы
COPY --from=build /app/publish .

# Копируем конфигурацию принудительно
COPY --from=build /src/appsettings.json .
COPY --from=build /src/appsettings.Development.json .

# Переопределяем переменные окружения
ENV ASPNETCORE_ENVIRONMENT=Development
ENV ASPNETCORE_URLS=http://+:8080

EXPOSE 8080

ENTRYPOINT ["dotnet", "TransactionAPI.dll"]
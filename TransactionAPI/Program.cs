using TransactionAPI.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Принудительно слушаем порт 5000 на всех сетевых интерфейсах.
// Без этого в Docker контейнер не принимает запросы с хоста,
// т.к. по умолчанию Kestrel слушает только localhost.
builder.WebHost.UseUrls("http://*:5000");

// Регистрируем сервис как Singleton
builder.Services.AddSingleton<KafkaProducerService>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "healthy", timestamp = DateTime.UtcNow }));

if (app.Environment.IsDevelopment())
{
	app.UseSwagger();
	app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
using RabbitMQ.Playground.Publisher.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<RabbitMqService>();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Middleware для Swagger
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "RabbitMQ API V1");
        c.RoutePrefix = string.Empty; // Swagger буде на корні, напр. http://localhost:5000/
    });
}

app.UseHttpsRedirection();
app.MapControllers();
app.Run();
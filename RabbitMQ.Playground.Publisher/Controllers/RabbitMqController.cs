using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Playground.Publisher.Services;

namespace RabbitMQ.Playground.Publisher.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RabbitMqController
    {
        private readonly RabbitMqService _rabbitService;

        public RabbitMqController(RabbitMqService rabbitService)
        {
            _rabbitService = rabbitService;
            // Запускаємо споживача на фоні
            //_rabbitService.ConsumeAsync();
        }

        [HttpPost("send")]
        public IActionResult Send([FromQuery] string message)
        {
            _rabbitService.PublishAsync(message);
            return new OkResult();
        }

        /// <summary>
        /// Вичитати всі повідомлення з черги та видалити їх
        /// </summary>
        [HttpPost("drain")]
        public async Task<IActionResult> DrainQueue()
        {
            var messages = await _rabbitService.ReadAndDeleteAllAsync();

            return new OkObjectResult(new
            {
                count = messages.Count,
                messages
            });
        }
    }
}

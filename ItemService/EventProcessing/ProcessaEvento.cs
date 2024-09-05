using System.Text.Json;
using AutoMapper;
using ItemService.Models;
using ItemService.Data;
using ItemService.Dtos;

namespace ItemService.EventProcessing
{
    public class ProcessaEvento : IProcessaEvento
    {
        private readonly IMapper _mapper;
        private readonly IServiceScopeFactory _scopeFactory;

        public ProcessaEvento(IMapper mapper, IServiceScopeFactory scopeFactory)
        {
            _mapper = mapper;
            _scopeFactory = scopeFactory;
        }

        public void Processa(string mensagemParaConsumir)
        {
            using var scope = _scopeFactory.CreateScope();

            var itemRepository = scope.ServiceProvider.GetRequiredService<IItemRepository>();

            var restauranteReadDto = JsonSerializer.Deserialize<RestauranteReadDto>(mensagemParaConsumir);

            var restaurante = _mapper.Map<Restaurante>(restauranteReadDto);

            Console.WriteLine("restaurante", restaurante);

            if (!itemRepository.ExisteRestauranteExterno(restaurante.Id))
            {
                itemRepository.CreateRestaurante(restaurante);
                itemRepository.SaveChanges();
            }
        }
    }
}

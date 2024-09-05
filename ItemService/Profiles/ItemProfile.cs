using AutoMapper;
using ItemService.Dtos;
using ItemService.Models;

namespace ItemService.Profiles
{
    public class ItemProfile : Profile
    {
        public ItemProfile()
        {
            CreateMap<Restaurante, RestauranteReadDto>();
            CreateMap<RestauranteReadDto, Restaurante>()
                .ForMember(dest => dest.IdExterno, opt => opt.MapFrom(src => src.Id));
            CreateMap<ItemCreateDto, Item>();
            CreateMap<Item, ItemCreateDto>();
            CreateMap<RestaurantePublishedDto, Restaurante>()
                .ForMember(restaurante => restaurante.IdExterno,
                            opt => opt.MapFrom(restaurantePublishedDto => restaurantePublishedDto.Id));
        }
    }
}
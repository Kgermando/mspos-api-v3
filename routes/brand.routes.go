package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/brand"
	"github.com/gofiber/fiber/v2"
)

func setupBrandRoutes(api fiber.Router) {
	// Brand controller
	br := api.Group("/brands")
	br.Get("/all", brand.GetAllBrands)
	br.Get("/all/paginate", brand.GetPaginatedBrands)
	br.Get("/all/paginate/country/:country_uuid", brand.GetPaginatedBrandsByCountryUUID)
	br.Get("/all/paginate/province/:province_uuid", brand.GetPaginatedBrandsByProvinceUUID)
	br.Get("/all/provinces/:province_uuid", brand.GetAllBrandsByProvince)
	br.Get("/get/:uuid", brand.GetOneBrand)
	br.Post("/create", brand.CreateBrand)
	br.Put("/update/:uuid", brand.UpdateBrand)
	br.Delete("/delete/:uuid", brand.DeleteBrand)
}

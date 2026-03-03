package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/area"
	"github.com/danny19977/mspos-api-v3/controllers/commune"
	"github.com/danny19977/mspos-api-v3/controllers/country"
	"github.com/danny19977/mspos-api-v3/controllers/province"
	Subarea "github.com/danny19977/mspos-api-v3/controllers/subarea"
	"github.com/gofiber/fiber/v2"
)

func setupGeographicRoutes(api fiber.Router) {
	// Countries controller
	co := api.Group("/countries")
	co.Get("/all", country.GetAllCountry)
	co.Get("/all/paginate", country.GetPaginatedCountry)
	// co.Get("/all/dropdown", country.GetCountryDropdown)
	co.Get("/get/:uuid", country.GetCountry)
	co.Post("/create", country.CreateCountry)
	co.Put("/update/:uuid", country.UpdateCountry)
	co.Delete("/delete/:uuid", country.DeleteCountry)

	// Province controller
	prov := api.Group("/provinces")
	prov.Get("/all", province.GetAllProvinces)
	prov.Get("/all/paginate", province.GetPaginatedProvince)
	prov.Get("/all/paginate/country/:country_uuid", province.GetPaginatedProvinceByCountry)
	prov.Get("/all/paginate/province/:province_uuid", province.GetPaginatedASM)
	prov.Get("/all/:country_uuid", province.GetAllProvinceByCountry)
	prov.Get("/get/:uuid", province.GetProvince)
	prov.Get("/get-by/:uuid", province.GetProvinceByName)
	prov.Post("/create", province.CreateProvince)
	prov.Put("/update/:uuid", province.UpdateProvince)
	prov.Delete("/delete/:uuid", province.DeleteProvince)

	// Areas controller
	ar := api.Group("/areas")
	ar.Get("/all", area.GetAllAreas)
	ar.Get("/all/paginate", area.GetPaginatedAreas)
	ar.Get("/all/paginate/country/:country_uuid", area.GetAreaByCountry)
	ar.Get("/all/paginate/province/:province_uuid", area.GetAreaByASM)
	ar.Get("/all/paginate/area/:area_uuid", area.GetAreaBySups)
	ar.Get("/all/:province_uuid", area.GetAllAreasByProvinceUUID)
	ar.Get("/all/:id", area.GetAreaByID)
	ar.Get("/all-area/:id", area.GetSupAreaByID)
	ar.Get("/get/:uuid", area.GetArea)
	ar.Get("/get-by/:uuid", area.GetAreaByName)
	ar.Post("/create", area.CreateArea)
	ar.Put("/update/:uuid", area.UpdateArea)
	ar.Delete("/delete/:uuid", area.DeleteArea)

	//SubArea controller
	sa := api.Group("/subareas")
	sa.Get("/all", Subarea.GetAllSubArea)
	sa.Get("/all/paginate", Subarea.GetPaginatedSubArea)
	sa.Get("/all/paginate/country/:country_uuid", Subarea.GetPaginatedSubAreaByCountry)
	sa.Get("/all/paginate/province/:province_uuid", Subarea.GetPaginatedSubAreaByASM)
	sa.Get("/all/paginate/area/:area_uuid", Subarea.GetPaginatedSubAreaBySup)
	sa.Get("/all/paginate/subarea/:sub_area_uuid", Subarea.GetAllSubAreaDr)
	sa.Get("/all/:area_uuid", Subarea.GetAllDataBySubAreaByAreaUUID)
	sa.Get("/get/:uuid", Subarea.GetSubArea)
	sa.Get("/get-by/:uuid", Subarea.GetSubAreaByName)
	sa.Post("/create", Subarea.CreateSubArea)
	sa.Put("/update/:uuid", Subarea.UpdateSubArea)
	sa.Delete("/delete/:uuid", Subarea.DeleteSubarea)

	// Commune controller
	com := api.Group("/communes")
	com.Get("/all", commune.GetAllCommunes)
	com.Get("/all/paginate", commune.GetPaginatedCommunes)
	com.Get("/all/paginate/country/:country_uuid", commune.GetPaginatedCommunesByCountryUUID)
	com.Get("/all/paginate/province/:province_uuid", commune.GetPaginatedCommunesByProvinceUUID)
	com.Get("/all/paginate/area/:area_uuid", commune.GetPaginatedCommunesByAreaUUID)
	com.Get("/all/paginate/subarea/:sub_area_uuid", commune.GetPaginatedCommunesBySubAreaUUID)
	com.Get("/all/paginate/commune/:commune_uuid", commune.GetPaginatedCommunesByCyclo)
	com.Get("/all/:sub_area_uuid", commune.GetAllCommunesBySubAreaUUID)
	com.Get("/all/:id", commune.GetCommune)
	com.Get("/get/:uuid", commune.GetCommune)
	com.Post("/create", commune.CreateCommune)
	com.Put("/update/:uuid", commune.UpdateCommune)
	com.Delete("/delete/:uuid", commune.DeleteCommune)
}

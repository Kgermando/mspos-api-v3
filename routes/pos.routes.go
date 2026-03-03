package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/pos"
	"github.com/danny19977/mspos-api-v3/controllers/posequiment"
	"github.com/gofiber/fiber/v2"
)

func setupPosRoutes(api fiber.Router) {
	// Pos controller
	po := api.Group("/pos")
	po.Get("/all", pos.GetAllPoss)
	po.Get("/all/paginate", pos.GetPaginatedPos)
	po.Get("/all/paginate/country/:country_uuid", pos.GetPaginatedPosByCountryUUID)
	po.Get("/all/paginate/province/:province_uuid", pos.GetPaginatedPosByProvinceUUID)
	po.Get("/all/paginate/area/:area_uuid", pos.GetPaginatedPosByAreaUUID)
	po.Get("/all/paginate/subarea/:sub_area_uuid", pos.GetPaginatedPosBySubAreaUUID)
	po.Get("/all/paginate/commune/:user_uuid", pos.GetPaginatedPosByCommuneUUID)
	po.Get("/all/paginate/commune-filter/:commune_uuid", pos.GetPaginatedPosByCommuneUserUUIDFilter)
	po.Get("/all/countries/:country_uuid", pos.GetAllPosByManager)
	po.Get("/all/provinces/:province_uuid", pos.GetAllPosByASM)
	po.Get("/all/areas/:area_uuid", pos.GetAllPosBySup)
	po.Get("/all/subareas/:sub_area_uuid", pos.GetAllPosByDR)
	po.Get("/all/cyclo/:user_uuid", pos.GetAllPosByCyclo)
	po.Get("/export/excel", pos.GeneratePosExcelReport)
	po.Get("/map-pos/:pos_uuid", pos.MapPos)
	po.Post("/create", pos.CreatePos)
	po.Get("/get/:uuid", pos.GetPos)
	po.Put("/update/:uuid", pos.UpdatePos)
	po.Delete("/delete/:uuid", pos.DeletePos)

	// POSEQUIPEMENT controller
	pe := api.Group("/pos-equipements")
	pe.Get("/all/paginate/:pos_uuid", posequiment.GetPaginatedPosEquipmentByPos)
	pe.Get("/all", posequiment.GetAllPosEquipments)
	pe.Post("/create", posequiment.CreatePosEquipment)
	pe.Get("/get/:uuid", posequiment.GetAllPosEquipments)
	pe.Get("/get/:uuid", posequiment.GetPosEquipment)
	pe.Put("/update/:uuid", posequiment.UpdatePosEquipment)
	pe.Delete("/delete/:uuid", posequiment.DeletePosEquipment)
}

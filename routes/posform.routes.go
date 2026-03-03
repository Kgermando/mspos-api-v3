package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/posform"
	PosFormItem "github.com/danny19977/mspos-api-v3/controllers/posformitem"
	"github.com/gofiber/fiber/v2"
)

func setupPosFormRoutes(api fiber.Router) {
	// Posforms controller
	posf := api.Group("/posforms")
	posf.Get("/all/paginate", posform.GetPaginatedPosForm)
	posf.Get("/all/paginate/country/:country_uuid", posform.GetPaginatedPosFormCountryUUID)
	posf.Get("/all/paginate/province/:province_uuid", posform.GetPaginatedPosFormProvine)
	posf.Get("/all/paginate/area/:area_uuid", posform.GetPaginatedPosFormArea)
	posf.Get("/all/paginate/subarea/:sub_area_uuid", posform.GetPaginatedPosFormSubArea)
	posf.Get("/all/paginate/commune/:user_uuid", posform.GetPaginatedPosFormCommune)
	posf.Get("/all/paginate/commune-filter/:commune_uuid", posform.GetPaginatedPosFormCommuneFilter)
	posf.Get("/all/paginate/:pos_uuid", posform.GetPaginatedPosFormByPOS)
	posf.Get("/all/paginate/user/:user_uuid", posform.GetPaginatedPosFormByUserUUID)
	posf.Get("/all", posform.GetAllPosforms)
	posf.Get("/export/excel", posform.GeneratePosFormExcelReport)
	posf.Post("/create", posform.CreatePosform)
	posf.Get("/get/:uuid", posform.GetPosForm)
	posf.Put("/update/:uuid", posform.UpdatePosform)
	posf.Delete("/delete/:uuid", posform.DeletePosform)

	// POSformItem controller
	posfi := api.Group("/posform-items")
	posfi.Get("/all/", PosFormItem.GetAllPosFormItems)
	posfi.Get("/all/paginate", PosFormItem.GetPaginatedPosformItem)
	posfi.Get("/all/:pos_form_uuid", PosFormItem.GetAllPosFormItemsByUUID)
	// posfi.Get("/get/:uuid", PosFormItem.Get)
	posfi.Post("/create", PosFormItem.CreatePosformItem)
	posfi.Put("/update/:uuid", PosFormItem.UpdatePosformItem)
	posfi.Delete("/delete/:uuid", PosFormItem.DeletePosformItem)
}

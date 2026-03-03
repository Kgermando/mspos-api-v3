package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/asm"
	"github.com/danny19977/mspos-api-v3/controllers/cyclo"
	"github.com/danny19977/mspos-api-v3/controllers/dr"
	"github.com/danny19977/mspos-api-v3/controllers/manager"
	"github.com/danny19977/mspos-api-v3/controllers/sup"
	"github.com/gofiber/fiber/v2"
)

func setupHierarchyRoutes(api fiber.Router) {
	// Manager controller
	ma := api.Group("/managers")
	ma.Get("/all", manager.GetAllManagers)
	ma.Get("/all/paginate", manager.GetPaginatedManager)
	ma.Get("/get/:uuid", manager.GetManager)
	// ma.Get("/all/:id", manager.GetManagerByID)
	ma.Post("/create", manager.CreateManager)
	ma.Put("/update/:uuid", manager.UpdateManager)
	ma.Delete("/delete/:uuid", manager.DeleteManager)

	// ASM controller
	as := api.Group("/asms")
	as.Get("/all/paginate", asm.GetPaginatedASM)
	as.Get("/all/paginate/province/:user_uuid", asm.GetPaginatedASMByProvince)

	// Sup controller
	su := api.Group("/sups")
	su.Get("/all/paginate", sup.GetPaginatedSups)
	su.Get("/all/paginate/province/:user_uuid", sup.GetPaginatedSupProvince)
	su.Get("/all/paginate/area/:user_uuid", sup.GetPaginatedSupArea)

	// DR Controller
	d := api.Group("/drs")
	d.Get("/all/paginate", dr.GetPaginatedDr)
	d.Get("/all/paginate/province/:user_uuid", dr.GetPaginatedDrByProvince)
	d.Get("/all/paginate/area/:user_uuid", dr.GetPaginatedDrByArea)
	d.Get("/all/paginate/subarea/:user_uuid", dr.GetPaginatedDrBySubArea)

	// Cyclo controller
	cy := api.Group("/cyclos")
	cy.Get("/all/paginate", cyclo.GetPaginatedCyclo)
	cy.Get("/all/paginate/province/:user_uuid", cyclo.GetPaginatedCycloProvinceByID)
	cy.Get("/all/paginate/area/:user_uuid", cyclo.GetPaginatedCycloByAreaUUID)
	cy.Get("/all/paginate/subarea/:user_uuid", cyclo.GetPaginatedSubAreaByID)
	cy.Get("/all/paginate/commune/:user_uuid", cyclo.GetPaginatedCycloCommune)
}

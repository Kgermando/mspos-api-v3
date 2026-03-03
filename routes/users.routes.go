package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/user"
	"github.com/gofiber/fiber/v2"
)

func setupUsersRoutes(api fiber.Router) {
	u := api.Group("/users")
	u.Get("/all", user.GetAllUsers)
	u.Get("/all/paginate", user.GetPaginatedUsers)
	u.Get("/all/paginate/nosearch", user.GetPaginatedNoSerach)
	u.Get("/all/:id", user.GetUserByID)
	u.Get("/get/:uuid", user.GetUser)
	u.Post("/create", user.CreateUser)
	u.Put("/update/:uuid", user.UpdateUser)
	u.Delete("/delete/:uuid", user.DeleteUser)

	// --- Custom UUID user routes ---
	u.Get("/by-country/:country_uuid", user.GetUsersByCountryUUID)
	u.Get("/by-country/one/:country_uuid", user.GetUserByCountryUUID)
	u.Get("/by-province/:province_uuid", user.GetUsersByProvinceUUID)
	u.Get("/by-province/one/:province_uuid", user.GetUserByProvinceUUID)
	u.Get("/by-area/:area_uuid", user.GetUsersByAreaUUID)
	u.Get("/by-area/one/:area_uuid", user.GetUserByAreaUUID)
	u.Get("/by-subarea/:sub_area_uuid", user.GetUsersBySubAreaUUID)
	u.Get("/by-subarea/one/:sub_area_uuid", user.GetUserBySubAreaUUID)
	u.Get("/by-commune/:commune_uuid", user.GetUsersByCommuneUUID)
	u.Get("/by-commune/one/:commune_uuid", user.GetUserByCommuneUUID)
	u.Get("/by-support/:support_uuid", user.GetUsersBySupportUUID)
	u.Get("/by-support/one/:support_uuid", user.GetUserBySupportUUID)
	u.Get("/by-manager/:manager_uuid", user.GetUsersByManagerUUID)
	u.Get("/by-manager/one/:manager_uuid", user.GetUserByManagerUUID)
	u.Get("/by-asm/:asm_uuid", user.GetUsersByAsmUUID)
	u.Get("/by-asm/one/:asm_uuid", user.GetUserByAsmUUID)
	u.Get("/by-sup/:sup_uuid", user.GetUsersBySupUUID)
	u.Get("/by-sup/one/:sup_uuid", user.GetUserBySupUUID)
	u.Get("/by-dr/:dr_uuid", user.GetUsersByDrUUID)
	u.Get("/by-dr/one/:dr_uuid", user.GetUserByDrUUID)
	u.Get("/by-cyclo/:cyclo_uuid", user.GetUsersByCycloUUID)
	u.Get("/by-cyclo/one/:cyclo_uuid", user.GetUserByCycloUUID)
}

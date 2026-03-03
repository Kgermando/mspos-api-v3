package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/routeplan.go"
	RoutePlanItem "github.com/danny19977/mspos-api-v3/controllers/routeplanitem"
	"github.com/gofiber/fiber/v2"
)

func setupRoutePlanRoutes(api fiber.Router) {
	// routeplan controller
	rp := api.Group("/routeplans")
	rp.Get("/all", routeplan.GetRouteplan)
	rp.Get("/all/paginate", routeplan.GetPaginatedRouteplan)
	rp.Get("/all/paginate/province/:province_uuid", routeplan.GetPaginatedRouthplaByProvinceUUID)
	rp.Get("/all/paginate/area/:area_uuid", routeplan.GetPaginatedRouthplaByareaUUID)
	rp.Get("/all/paginate/subarea/:sub_area_uuid", routeplan.GetPaginatedRouthplaBySubareaUUID)
	rp.Get("/all/paginate/:user_uuid", routeplan.GetPaginatedRouteplaBycommuneUUID)
	rp.Get("/all/:uuid", routeplan.GetRouteplan)
	rp.Get("/get-by-user/:user_uuid", routeplan.GetRouteplanByUserUUID)
	rp.Get("/get/:uuid", routeplan.GetRouteplan)
	rp.Post("/create", routeplan.CreateRouteplan)
	rp.Put("/update/:uuid", routeplan.UpdateRouteplan)
	rp.Delete("/delete/:uuid", routeplan.DeleteRouteplan)

	// routeplanitem controller
	rpi := api.Group("/routeplan-items")
	rpi.Get("/all/paginate", RoutePlanItem.GetPaginatedRoutePlanItem)
	rpi.Get("/all/:route_plan_uuid", RoutePlanItem.GetAllRoutePlanItem)
	rpi.Get("/get/:uuid", RoutePlanItem.GetOneByRouteItermUUID)
	rpi.Post("/create", RoutePlanItem.CreateRoutePlanItem)
	rpi.Put("/update/:uuid", RoutePlanItem.UpdateRoutePlanItem)
	rpi.Delete("/delete/:uuid", RoutePlanItem.DeleteRoutePlanItem)
}

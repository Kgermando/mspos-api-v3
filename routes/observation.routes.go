package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/observation"
	"github.com/gofiber/fiber/v2"
)

func setupObservationRoutes(api fiber.Router) {
	// Observations controller — comments/observations from visit forms
	// Role-based smart endpoint (reads JWT and applies filters automatically)
	obs := api.Group("/observations")
	obs.Get("/all/paginate", observation.GetObservationsByRole)
	obs.Get("/all/paginate/country/:country_uuid", observation.GetObservationsByCountry)
	obs.Get("/all/paginate/province/:province_uuid", observation.GetObservationsByProvince)
	obs.Get("/all/paginate/area/:area_uuid", observation.GetObservationsByArea)
	obs.Get("/all/paginate/subarea/:sub_area_uuid", observation.GetObservationsBySubArea)
	obs.Get("/all/paginate/commune/:commune_uuid", observation.GetObservationsByCommune)
}

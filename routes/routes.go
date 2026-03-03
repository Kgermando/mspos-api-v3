package routes

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

func Setup(app *fiber.App) {
	api := app.Group("/api", logger.New())

	// Setup all route groups
	setupAuthRoutes(api)
	setupUsersRoutes(api)
	setupGeographicRoutes(api)
	setupHierarchyRoutes(api)
	setupPosRoutes(api)
	setupRoutePlanRoutes(api)
	setupBrandRoutes(api)
	setupPosFormRoutes(api)
	setupObservationRoutes(api)
	setupUserLogsRoutes(api)
	setupDashboardRoutes(api)
}

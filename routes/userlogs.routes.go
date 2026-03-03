package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/user_logs"
	"github.com/gofiber/fiber/v2"
)

func setupUserLogsRoutes(api fiber.Router) {
	// UserLogs controller
	log := api.Group("/users-logs")
	log.Get("/all", user_logs.GetUserLogs)
	log.Get("/all/paginate", user_logs.GetPaginatedUserLogs)
	log.Get("/all/paginate/:user_uuid", user_logs.GetUserLogByID)
	log.Get("/get/:uuid", user_logs.GetUserLog)
	log.Post("/create", user_logs.CreateUserLog)
	log.Put("/update/:uuid", user_logs.UpdateUserLog)
	log.Delete("/delete/:uuid", user_logs.DeleteUserLog)
}

package dashboard

import (
	"fmt"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

func WdTableDash(c *fiber.Ctx) error {
	db := database.DB
	start_date := c.Params("start_date")
	end_date := c.Params("end_date")

	fmt.Println("db: ", db)
	fmt.Println("start_date: ", start_date)
	fmt.Println("end_date: ", end_date)

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    "",
	})
}

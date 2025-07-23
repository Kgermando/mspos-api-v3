package middlewares

import (
	"github.com/danny19977/mspos-api-v3/utils"
	"github.com/gofiber/fiber"
)

func IsAuthenticated(c *fiber.Ctx) error {

	cookie := c.Cookies("token")

	if _, err := utils.VerifyJwt(cookie); err != nil {
		c.Status(fiber.StatusUnauthorized)
		return c.JSON(fiber.Map{
			"message": "unauthenticated",
		})
	}
 c.Next()
 return nil
}

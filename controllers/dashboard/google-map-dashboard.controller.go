package dashboard

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

func GoogleMaps(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Latitude  float64 `json:"latitude"`  // Latitude of the user
		Longitude float64 `json:"longitude"` // Longitude of the user
		Signature string  `json:"signature"`
		PosName   string  `json:"pos_name"`   // Name of the POS
		PosUUID   string  `json:"pos_uuid"`   // UUID of the POS
		Postype   string  `json:"postype"`    // Type de POS
		Asm       string  `json:"asm"`        // Name of the ASM
		Sup       string  `json:"sup"`        // Name of the Supervisor
		Dr        string  `json:"dr"`         // Name of the DR
		Cyclo     string  `json:"cyclo"`      // Name of the Cyclo
		CreatedAt string  `json:"created_at"` // Creation date of the form
	}

	err := db.Table("pos_forms").
		Joins("JOIN pos ON pos.uuid = pos_forms.pos_uuid").
		Select(`
			pos_forms.latitude AS latitude,
			pos_forms.longitude AS longitude,
			pos_forms.signature AS signature,
			pos_forms.created_at AS created_at,
			pos.name AS pos_name,
			pos.uuid AS pos_uuid,
			pos.postype AS postype,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				ELSE pos_forms.asm 
			END AS asm,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN '' 
				ELSE pos_forms.sup 
			END AS sup,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				WHEN pos_forms.signature = pos_forms.sup THEN '' 
				ELSE pos_forms.dr 
			END AS dr,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				WHEN pos_forms.signature = pos_forms.sup THEN ''
				WHEN pos_forms.signature = pos_forms.dr THEN '' 
				ELSE pos_forms.cyclo 
			END AS cyclo
		`).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

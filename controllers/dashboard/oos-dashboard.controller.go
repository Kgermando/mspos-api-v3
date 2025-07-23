package dashboard

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// OosDashboardController handles the OOS dashboard requests for Province
func OosTableViewProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name       string  `json:"name"`
		BrandName  string  `json:"brand_name"`
		TotalCount int     `json:"total_count"`
		Percentage float64 `json:"percentage"`
		TotalPos   int     `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
		provinces.name AS name, 
		brands.name AS brand_name,
		SUM(pos_form_items.counter) AS total_count,
		ROUND((((SUM(SELECT SUM(pos_form_items.counter) / (counter) FROM pos_form_items INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid WHERE pos_forms.country_uuid = ? AND  pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?)) * 100) - 100)::numeric, 2) AS percentage,
		(SELECT SUM(counter) FROM pos_form_items INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid WHERE pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?) AS total_pos
		`, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date).
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Group("provinces.name, brands.name").
		Order("provinces.name, total_count DESC").
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

func OosTableViewArea(c *fiber.Ctx) error {

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    "",
	})
}

func OosTableViewSubArea(c *fiber.Ctx) error {

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    "",
	})
}

func OosTableViewCommune(c *fiber.Ctx) error {

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    "",
	})
}

// Line chart for sum brand by month
func OosTotalByBrandByMonth(c *fiber.Ctx) error {

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Total count by brand grouped by month for the year",
		"data":    "",
	})
}

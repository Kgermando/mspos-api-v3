package dashboard

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// Total POS Grosseste & and Detaillant per Area and SubArea
func TypePosTableProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	var results []struct {
		Name     string `json:"name"`
		UUID     string `json:"uuid"`
		TypePos  string `json:"type_pos"`
		TotalPos int    `json:"total_pos"`
	}

	err := db.Table("pos").
		Select(`
		provinces.name AS name,
		provinces.uuid AS uuid,
		pos.postype AS type_pos, 
		COUNT(*) as total_pos
		`).
		Joins("INNER JOIN provinces ON pos.province_uuid = provinces.uuid").
		Where("pos.country_uuid = ? AND pos.province_uuid = ?", country_uuid, province_uuid).
		Where("pos.deleted_at IS NULL").
		Group("provinces.name, provinces.uuid, pos.postype").
		Order("provinces.name, pos.postype DESC").
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

func TypePosTableArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	var results []struct {
		Name     string `json:"name"`
		UUID     string `json:"uuid"`
		TypePos  string `json:"type_pos"`
		TotalPos int    `json:"total_pos"`
	}

	err := db.Table("pos").
		Select(`
		areas.name AS name,
		areas.uuid AS uuid,
		pos.postype AS type_pos, 
		COUNT(*) as total_pos
		`).
		Joins("INNER JOIN areas ON pos.area_uuid = areas.uuid").
		Where("pos.country_uuid = ? AND pos.province_uuid = ?", country_uuid, province_uuid).
		Where("pos.deleted_at IS NULL").
		Group("areas.name, areas.uuid, pos.postype").
		Order("areas.name, pos.postype DESC").
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

func TypePosTableSubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")

	var results []struct {
		Name     string `json:"name"`
		UUID     string `json:"uuid"`
		TypePos  string `json:"type_pos"`
		TotalPos int    `json:"total_pos"`
	}

	err := db.Table("pos").
		Select(`
		sub_areas.name AS name, 
		sub_areas.uuid AS uuid,
		pos.postype AS type_pos, 
		COUNT(*) as total_pos
		`).
		Joins("INNER JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Where("pos.country_uuid = ? AND pos.province_uuid = ? AND pos.area_uuid = ?", country_uuid, province_uuid, area_uuid).
		Where("pos.deleted_at IS NULL").
		Group("sub_areas.name, sub_areas.uuid, pos.postype").
		Order("sub_areas.name, pos.postype DESC").
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

func TypePosTableCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")

	var results []struct {
		Name     string `json:"name"`
		TypePos  string `json:"type_pos"`
		TotalPos int    `json:"total_pos"`
	}

	err := db.Table("pos").
		Select(`
		communes.name AS name,
		communes.uuid AS uuid,
		pos.postype AS type_pos, 
		COUNT(*) as total_pos
		`).
		Joins("INNER JOIN communes ON pos.commune_uuid = communes.uuid").
		Where("pos.country_uuid = ? AND pos.province_uuid = ? AND pos.area_uuid = ? AND pos_forms.sub_area_uuid = ?", country_uuid, province_uuid, area_uuid, sub_area_uuid).
		Where("pos.deleted_at IS NULL").
		Group("communes.name, communes.uuid, pos.postype").
		Order("communes.name, pos.postype DESC").
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

// Price table for POS_Forms per tige
func PriceTableProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name       string `json:"name"`
		UUID       string `json:"uuid"`
		Price      string `json:"price"`
		CountPrice int    `json:"count_price"`
		Sold       int    `json:"sold"`
	}

	err := db.Table("pos_form_items").
		Select(`
		provinces.name AS name,
		provinces.uuid AS uuid,
		price AS price,
		COUNT(*) AS count_price,
		SUM(pos_form_items.sold) AS sold   
		`).
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group("provinces.name, provinces.uuid, pos_forms.price").
		Order("provinces.name, pos_forms.price DESC").
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

func PriceTableArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name       string `json:"name"`
		UUID       string `json:"uuid"`
		Price      string `json:"price"`
		CountPrice int    `json:"count_price"`
		Sold       int    `json:"sold"`
	}
	err := db.Table("pos_form_items").
		Select(`
		areas.name AS name,
		areas.uuid AS uuid,
		price AS price,
		COUNT(*) AS count_price,
		SUM(pos_form_items.sold) AS sold   
		`).
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group("areas.name, areas.uuid, pos_forms.price").
		Order("areas.name, pos_forms.price DESC").
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

func PriceTableSubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name       string `json:"name"`
		UUID       string `json:"uuid"`
		Price      string `json:"price"`
		CountPrice int    `json:"count_price"`
		Sold       int    `json:"sold"`
	}

	err := db.Table("pos_form_items").
		Select(`
		sub_areas.name AS name,
		sub_areas.uuid AS uuid,
		price AS price,
		COUNT(*) AS count_price,
		SUM(pos_form_items.sold) AS sold  
		`).
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ?", country_uuid, province_uuid, area_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group("sub_areas.name, sub_areas.uuid, pos_forms.price").
		Order("sub_areas.name, pos_forms.price DESC").
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

func PriceTableCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name       string `json:"name"`
		UUID       string `json:"uuid"`
		Price      string `json:"price"`
		CountPrice int    `json:"count_price"`
		Sold       int    `json:"sold"`
	}
	err := db.Table("pos_form_items").
		Select(`
		communes.name AS name,
		communes.uuid AS uuid,
		price AS price,
		COUNT(*) AS count_price,
		SUM(pos_form_items.sold) AS sold 
		`).
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.sub_area_uuid = ?", country_uuid, province_uuid, area_uuid, sub_area_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group("communes.name, communes.uuid, pos_forms.price").
		Order("communes.name, pos_forms.price DESC").
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

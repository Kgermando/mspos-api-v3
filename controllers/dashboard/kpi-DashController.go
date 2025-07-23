package dashboard

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// total visit per day 50 and per week 300 and 100%(percentage)
// total Visit per month 1400 and 100%(percentage)

func TotalVisitsByCountry(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name         string  `json:"name"`
		UUID         string  `json:"uuid"`
		CountryUUID  string  `json:"country_uuid"`
		ProvinceUUID string  `json:"province_uuid"`
		AreaUUID     string  `json:"area_uuid"`
		SubAreaUUID  string  `json:"sub_area_uuid"`
		CommuneUUID  string  `json:"commune_uuid"`
		Signature    string  `json:"signature"`
		Title        string  `json:"title"`
		TotalVisits  int     `json:"total_visits"`
		Objectif     float64 `json:"objectif"`
		Target       int     `json:"target"`
	}

	err := db.Table("pos_forms").
		Select(`
		countries.name AS name,
		countries.uuid AS uuid, 
		users.fullname AS signature,
		users.title AS title, 
		COUNT(pos_forms.uuid) AS total_visits,
		(ROUND((COUNT(pos_forms.uuid) / (
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) ::numeric) * 100, 2)) AS objectif,
		(
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) AS target
		`, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date).
		Joins("JOIN users ON users.uuid = pos_forms.user_uuid").
		Joins("JOIN countries ON countries.uuid = pos_forms.country_uuid").
		Where("pos_forms.country_uuid = ?", country_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group("countries.name, countries.uuid, users.fullname, users.title, users.uuid").
		Order("countries.name, users.fullname, users.title").
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

func TotalVisitsByProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name         string  `json:"name"`
		CountryUUID  string  `json:"country_uuid"`
		ProvinceUUID string  `json:"province_uuid"`
		AreaUUID     string  `json:"area_uuid"`
		SubAreaUUID  string  `json:"sub_area_uuid"`
		CommuneUUID  string  `json:"commune_uuid"`
		Signature    string  `json:"signature"`
		Title        string  `json:"title"`
		TotalVisits  int     `json:"total_visits"`
		Objectif     float64 `json:"objectif"`
		Target       int     `json:"target"`
	}

	err := db.Table("pos_forms").
		Select(`
		provinces.name AS name, 
		pos_forms.country_uuid AS country_uuid,
		pos_forms.province_uuid AS province_uuid,
		pos_forms.area_uuid AS area_uuid,
		pos_forms.sub_area_uuid AS sub_area_uuid,
		pos_forms.commune_uuid AS commune_uuid,
		users.fullname AS signature,
		users.title AS title,
		COUNT(pos_forms.uuid) AS total_visits,
		(ROUND((COUNT(pos_forms.uuid) / (
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) ::numeric) * 100, 2)) AS objectif,
		(
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) AS target
		`, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date).
		Joins("JOIN users ON users.uuid = pos_forms.user_uuid").
		Joins("JOIN provinces ON provinces.uuid = pos_forms.province_uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group(`
			provinces.name, 
			pos_forms.country_uuid, 
			pos_forms.province_uuid, 
			pos_forms.area_uuid, 
			pos_forms.sub_area_uuid, 
			pos_forms.commune_uuid, 
			users.fullname,
			users.title,
			users.uuid
		`).Order("provinces.name, users.fullname").
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

func TotalVisitsByArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name        string  `json:"name"`
		UUID        string  `json:"uuid"`
		Signature   string  `json:"signature"`
		Title       string  `json:"title"`
		TotalVisits int     `json:"total_visits"`
		Objectif    float64 `json:"objectif"`
		Target      int     `json:"target"`
	}

	err := db.Table("pos_forms").
		Select(`
		areas.name AS name,
		areas.uuid AS uuid,
		users.fullname AS signature,
		users.title AS title, 
		COUNT(pos_forms.uuid) AS total_visits,
		(ROUND((COUNT(pos_forms.uuid) / (
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) ::numeric) * 100, 2)) AS objectif,
		(
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) AS target
		`, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date).
		Joins("JOIN users ON users.uuid = pos_forms.user_uuid").
		Joins("JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group("areas.name, areas.uuid, users.fullname, users.title, users.uuid").
		Order("areas.name, users.fullname").
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

func TotalVisitsBySubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name        string  `json:"name"`
		UUID        string  `json:"uuid"`
		Signature   string  `json:"signature"`
		Title       string  `json:"title"`
		TotalVisits int     `json:"total_visits"`
		Objectif    float64 `json:"objectif"`
		Target      int     `json:"target"`
	}

	err := db.Table("pos_forms").
		Select(`
		sub_areas.name AS name,
		sub_areas.uuid AS uuid,
		users.fullname AS signature,
		users.title AS title, 
		COUNT(pos_forms.uuid) AS total_visits,
		(ROUND((COUNT(pos_forms.uuid) / (
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) ::numeric) * 100, 2)) AS objectif,
		(
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) AS target
		`, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date).
		Joins("JOIN users ON users.uuid = pos_forms.user_uuid").
		Joins("JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ?", country_uuid, province_uuid, area_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group("sub_areas.name, sub_areas.uuid, users.fullname, users.title, users.uuid").
		Order("sub_areas.name, users.fullname").
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

func TotalVisitsByCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name        string  `json:"name"`
		UUID        string  `json:"uuid"`
		Signature   string  `json:"signature"`
		Title       string  `json:"title"`
		TotalVisits int     `json:"total_visits"`
		Objectif    float64 `json:"objectif"`
		Target      int     `json:"target"`
	}

	err := db.Table("pos_forms").
		Select(`
		communes.name AS name,
		communes.uuid AS uuid,
		users.fullname AS signature,
		users.title AS title, 
		COUNT(pos_forms.uuid) AS total_visits,
		(ROUND((COUNT(pos_forms.uuid) / (
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) ::numeric) * 100, 2)) AS objectif,
		(
			CASE
					WHEN users.title = 'ASM'  THEN 10 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Supervisor'  THEN 20 * ((?::date - ?::date) + 1)
					WHEN users.title = 'DR'   THEN 40 * ((?::date - ?::date) + 1)
					WHEN users.title = 'Cyclo' THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1 
			END
		) AS target
		`, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date).
		Joins("JOIN users ON users.uuid = pos_forms.user_uuid").
		Joins("JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.sub_area_uuid = ?", country_uuid, province_uuid, area_uuid, sub_area_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Group("communes.name, communes.uuid, users.fullname, users.title, users.uuid").
		Order("communes.name, users.fullname").
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

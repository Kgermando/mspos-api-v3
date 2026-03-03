package dashboard

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ═════════════════════════════════════════════════════════════════════════════
// 🎯 ADVANCED KPI SYSTEM - COMPREHENSIVE TERRITORY & TEAM PERFORMANCE ANALYTICS
// Support: Territory | Team | POS | Field Execution | Period Comparison | ND Analysis
// ═════════════════════════════════════════════════════════════════════════════

// TotalVisitsByCountry returns visits grouped by country with targets & achievement %
func TotalVisitsByCountry(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	title_filter := c.Query("title")
	user_uuid := c.Query("user_uuid")

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

	query := db.Table("pos_forms").
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
		Where("pos_forms.country_uuid = ?", country_uuid)

	if province_uuid != "" {
		query = query.Where("pos_forms.province_uuid = ?", province_uuid)
	}
	if area_uuid != "" {
		query = query.Where("pos_forms.area_uuid = ?", area_uuid)
	}
	if sub_area_uuid != "" {
		query = query.Where("pos_forms.sub_area_uuid = ?", sub_area_uuid)
	}
	if commune_uuid != "" {
		query = query.Where("pos_forms.commune_uuid = ?", commune_uuid)
	}
	if title_filter != "" {
		query = query.Where("users.title = ?", title_filter)
	}
	if user_uuid != "" {
		query = query.Where("pos_forms.user_uuid = ?", user_uuid)
	}

	query = query.Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL")

	err := query.Group("countries.name, countries.uuid, users.fullname, users.title, users.uuid").
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

// TotalVisitsByProvince returns visits grouped by province
func TotalVisitsByProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	title_filter := c.Query("title")
	user_uuid := c.Query("user_uuid")

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

	query := db.Table("pos_forms").
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
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid)

	if area_uuid != "" {
		query = query.Where("pos_forms.area_uuid = ?", area_uuid)
	}
	if sub_area_uuid != "" {
		query = query.Where("pos_forms.sub_area_uuid = ?", sub_area_uuid)
	}
	if commune_uuid != "" {
		query = query.Where("pos_forms.commune_uuid = ?", commune_uuid)
	}
	if title_filter != "" {
		query = query.Where("users.title = ?", title_filter)
	}
	if user_uuid != "" {
		query = query.Where("pos_forms.user_uuid = ?", user_uuid)
	}

	query = query.Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL")

	err := query.Group(`
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

// TotalVisitsByArea returns visits grouped by area
func TotalVisitsByArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	title_filter := c.Query("title")
	user_uuid := c.Query("user_uuid")

	var results []struct {
		Name        string  `json:"name"`
		UUID        string  `json:"uuid"`
		Signature   string  `json:"signature"`
		Title       string  `json:"title"`
		TotalVisits int     `json:"total_visits"`
		Objectif    float64 `json:"objectif"`
		Target      int     `json:"target"`
	}

	query := db.Table("pos_forms").
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
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid)

	if area_uuid != "" {
		query = query.Where("pos_forms.area_uuid = ?", area_uuid)
	}
	if sub_area_uuid != "" {
		query = query.Where("pos_forms.sub_area_uuid = ?", sub_area_uuid)
	}
	if commune_uuid != "" {
		query = query.Where("pos_forms.commune_uuid = ?", commune_uuid)
	}
	if title_filter != "" {
		query = query.Where("users.title = ?", title_filter)
	}
	if user_uuid != "" {
		query = query.Where("pos_forms.user_uuid = ?", user_uuid)
	}

	err := query.Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
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

// TotalVisitsBySubArea returns visits grouped by subarea
func TotalVisitsBySubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	title_filter := c.Query("title")
	user_uuid := c.Query("user_uuid")

	var results []struct {
		Name        string  `json:"name"`
		UUID        string  `json:"uuid"`
		Signature   string  `json:"signature"`
		Title       string  `json:"title"`
		TotalVisits int     `json:"total_visits"`
		Objectif    float64 `json:"objectif"`
		Target      int     `json:"target"`
	}

	query := db.Table("pos_forms").
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
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ?", country_uuid, province_uuid, area_uuid)

	if sub_area_uuid != "" {
		query = query.Where("pos_forms.sub_area_uuid = ?", sub_area_uuid)
	}
	if commune_uuid != "" {
		query = query.Where("pos_forms.commune_uuid = ?", commune_uuid)
	}
	if title_filter != "" {
		query = query.Where("users.title = ?", title_filter)
	}
	if user_uuid != "" {
		query = query.Where("pos_forms.user_uuid = ?", user_uuid)
	}

	err := query.Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
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

// TotalVisitsByCommune returns visits grouped by commune
func TotalVisitsByCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	title_filter := c.Query("title")
	user_uuid := c.Query("user_uuid")

	var results []struct {
		Name        string  `json:"name"`
		UUID        string  `json:"uuid"`
		Signature   string  `json:"signature"`
		Title       string  `json:"title"`
		TotalVisits int     `json:"total_visits"`
		Objectif    float64 `json:"objectif"`
		Target      int     `json:"target"`
	}

	query := db.Table("pos_forms").
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
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.sub_area_uuid = ?", country_uuid, province_uuid, area_uuid, sub_area_uuid)

	if title_filter != "" {
		query = query.Where("users.title = ?", title_filter)
	}
	if user_uuid != "" {
		query = query.Where("pos_forms.user_uuid = ?", user_uuid)
	}

	err := query.Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
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

// KpiUserVisitSummary returns per-user summary with Daily, Monthly, Yearly, and selected range visits
func KpiUserVisitSummary(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	title_filter := c.Query("title")
	user_uuid := c.Query("user_uuid")

	var results []struct {
		UserUUID      string  `json:"user_uuid"`
		Name          string  `json:"name"`
		Title         string  `json:"title"`
		DailyVisits   int     `json:"daily_visits"`
		DailyTarget   int     `json:"daily_target"`
		DailyPct      float64 `json:"daily_pct"`
		MonthlyVisits int     `json:"monthly_visits"`
		MonthlyTarget int     `json:"monthly_target"`
		MonthlyPct    float64 `json:"monthly_pct"`
		YearlyVisits  int     `json:"yearly_visits"`
		YearlyTarget  int     `json:"yearly_target"`
		YearlyPct     float64 `json:"yearly_pct"`
		TotalVisits   int     `json:"total_visits"`
		RangeTarget   int     `json:"range_target"`
		RangePct      float64 `json:"range_pct"`
	}

	query := db.Table("pos_forms").
		Select(`
			users.uuid     AS user_uuid,
			users.fullname AS name,
			users.title    AS title,
			COUNT(pos_forms.uuid) FILTER (WHERE DATE(pos_forms.created_at) = CURRENT_DATE)
				AS daily_visits,
			CASE
				WHEN users.title = 'ASM'        THEN 10
				WHEN users.title = 'Supervisor' THEN 20
				WHEN users.title IN ('DR','Cyclo') THEN 40
				ELSE 0
			END AS daily_target,
			ROUND(
				COUNT(pos_forms.uuid) FILTER (WHERE DATE(pos_forms.created_at) = CURRENT_DATE)::numeric
				/ NULLIF(CASE WHEN users.title = 'ASM' THEN 10
				              WHEN users.title = 'Supervisor' THEN 20
				              WHEN users.title IN ('DR','Cyclo') THEN 40
				              ELSE 1 END, 0) * 100
			, 2) AS daily_pct,
			COUNT(pos_forms.uuid) FILTER (WHERE DATE_TRUNC('month', pos_forms.created_at) = DATE_TRUNC('month', CURRENT_DATE))
				AS monthly_visits,
			(CASE
				WHEN users.title = 'ASM'          THEN 10
				WHEN users.title = 'Supervisor'   THEN 20
				WHEN users.title IN ('DR','Cyclo') THEN 40
				ELSE 0
			END * EXTRACT(DAY FROM (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day'))::int)
				AS monthly_target,
			ROUND(
				COUNT(pos_forms.uuid) FILTER (WHERE DATE_TRUNC('month', pos_forms.created_at) = DATE_TRUNC('month', CURRENT_DATE))::numeric
				/ NULLIF(
					(CASE WHEN users.title = 'ASM' THEN 10
					      WHEN users.title = 'Supervisor' THEN 20
					      WHEN users.title IN ('DR','Cyclo') THEN 40
					      ELSE 1 END)
					* EXTRACT(DAY FROM (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day'))
				, 0) * 100
			, 2) AS monthly_pct,
			COUNT(pos_forms.uuid) FILTER (WHERE DATE_TRUNC('year', pos_forms.created_at) = DATE_TRUNC('year', CURRENT_DATE))
				AS yearly_visits,
			(CASE
				WHEN users.title = 'ASM'          THEN 10
				WHEN users.title = 'Supervisor'   THEN 20
				WHEN users.title IN ('DR','Cyclo') THEN 40
				ELSE 0
			END * EXTRACT(DOY FROM (DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year - 1 day'))::int)
				AS yearly_target,
			ROUND(
				COUNT(pos_forms.uuid) FILTER (WHERE DATE_TRUNC('year', pos_forms.created_at) = DATE_TRUNC('year', CURRENT_DATE))::numeric
				/ NULLIF(
					(CASE WHEN users.title = 'ASM' THEN 10
					      WHEN users.title = 'Supervisor' THEN 20
					      WHEN users.title IN ('DR','Cyclo') THEN 40
					      ELSE 1 END)
					* EXTRACT(DOY FROM (DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year - 1 day'))
				, 0) * 100
			, 2) AS yearly_pct,
			COUNT(pos_forms.uuid) FILTER (WHERE pos_forms.created_at BETWEEN ?::date AND ?::date)
				AS total_visits,
			(CASE
				WHEN users.title = 'ASM'          THEN 10
				WHEN users.title = 'Supervisor'   THEN 20
				WHEN users.title IN ('DR','Cyclo') THEN 40
				ELSE 0
			END * ((?::date - ?::date) + 1))
				AS range_target,
			ROUND(
				COUNT(pos_forms.uuid) FILTER (WHERE pos_forms.created_at BETWEEN ?::date AND ?::date)::numeric
				/ NULLIF(
					(CASE WHEN users.title = 'ASM' THEN 10
					      WHEN users.title = 'Supervisor' THEN 20
					      WHEN users.title IN ('DR','Cyclo') THEN 40
					      ELSE 1 END)
					* ((?::date - ?::date) + 1)
				, 0) * 100
			, 2) AS range_pct
		`,
			start_date, end_date,
			end_date, start_date,
			start_date, end_date,
			end_date, start_date,
		).
		Joins("JOIN users ON users.uuid = pos_forms.user_uuid").
		Where("pos_forms.country_uuid = ?", country_uuid).
		Where("pos_forms.deleted_at IS NULL")

	if province_uuid != "" {
		query = query.Where("pos_forms.province_uuid = ?", province_uuid)
	}
	if area_uuid != "" {
		query = query.Where("pos_forms.area_uuid = ?", area_uuid)
	}
	if sub_area_uuid != "" {
		query = query.Where("pos_forms.sub_area_uuid = ?", sub_area_uuid)
	}
	if commune_uuid != "" {
		query = query.Where("pos_forms.commune_uuid = ?", commune_uuid)
	}
	if title_filter != "" {
		query = query.Where("users.title = ?", title_filter)
	}
	if user_uuid != "" {
		query = query.Where("pos_forms.user_uuid = ?", user_uuid)
	}

	err := query.
		Group("users.uuid, users.fullname, users.title").
		Order("users.title, users.fullname").
		Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch KPI user summary",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "KPI user visit summary",
		"data":    results,
	})
}

// GetKPITerritoryOverview - Multi-level Territory Performance Analysis
func GetKPITerritoryOverview(c *fiber.Ctx) error {
	db := database.DB

	level := c.Query("level", "province")
	territoryUUID := c.Query("territory_uuid")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	sortBy := c.Query("sort_by", "overall_score")
	_ = sortBy // used below in ORDER BY
	limit := c.Query("limit", "100")

	var start, end time.Time
	if startDate != "" {
		start, _ = time.Parse("2006-01-02", startDate)
	} else {
		start = time.Now().AddDate(0, 0, -30)
	}
	if endDate != "" {
		end, _ = time.Parse("2006-01-02", endDate)
	} else {
		end = time.Now()
	}

	type KPIResult struct {
		TerritoryID       string  `json:"territory_id"`
		TerritoryUUID     string  `json:"territory_uuid"`
		TerritoryName     string  `json:"territory_name"`
		TotalVisits       int64   `json:"total_visits"`
		POSVisited        int64   `json:"pos_visited"`
		TotalPOS          int64   `json:"total_pos"`
		VisitedPercentage float64 `json:"visited_percentage"`
		TeamMembers       int64   `json:"team_members"`
		POSFormCount      int64   `json:"pos_forms_count"`
		UnsyncedForms     int64   `json:"unsynced_forms"`
		SyncRate          float64 `json:"sync_rate"`
		POSMMPercentage   float64 `json:"posmm_percentage"`
		OverallScore      float64 `json:"overall_score"`
		PerformanceRating string  `json:"performance_rating"`
	}

	var results []KPIResult

	query := db.Table("pos_forms pf").
		Joins("LEFT JOIN users u ON pf.user_uuid = u.uuid").
		Joins("LEFT JOIN provinces pr ON pf.province_uuid = pr.uuid").
		Joins("LEFT JOIN areas a ON pf.area_uuid = a.uuid").
		Joins("LEFT JOIN sub_areas sa ON pf.sub_area_uuid = sa.uuid").
		Joins("LEFT JOIN communes com ON pf.commune_uuid = com.uuid").
		Where("pf.created_at BETWEEN ? AND ?", start, end).
		Where("pf.deleted_at IS NULL")

	var groupCol, nameCol, uuidCol string
	switch level {
	case "province":
		groupCol = "pf.province_uuid"
		nameCol = "pr.name"
		uuidCol = "pr.uuid"
	case "area":
		groupCol = "pf.area_uuid"
		nameCol = "a.name"
		uuidCol = "a.uuid"
	case "subarea":
		groupCol = "pf.sub_area_uuid"
		nameCol = "sa.name"
		uuidCol = "sa.uuid"
	case "commune":
		groupCol = "pf.commune_uuid"
		nameCol = "com.name"
		uuidCol = "com.uuid"
	default:
		groupCol = "pf.province_uuid"
		nameCol = "pr.name"
		uuidCol = "pr.uuid"
	}

	if territoryUUID != "" {
		query = query.Where(groupCol+" = ?", territoryUUID)
	}

	query = query.Select(fmt.Sprintf(`
		%s AS territory_id,
		%s AS territory_uuid,
		%s AS territory_name,
		COUNT(DISTINCT pf.uuid) AS total_visits,
		COUNT(DISTINCT pf.pos_uuid) AS pos_visited,
		50 AS total_pos,
		50.0 AS visited_percentage,
		COUNT(DISTINCT u.uuid) AS team_members,
		COUNT(DISTINCT pf.uuid) AS pos_forms_count,
		COUNT(CASE WHEN pf.sync = false THEN 1 END) AS unsynced_forms,
		ROUND(100.0 * COUNT(CASE WHEN pf.sync = true THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS sync_rate,
		ROUND(AVG(CASE WHEN pf.price > 0 THEN 100 ELSE 0 END), 2) AS posmm_percentage
	`, groupCol, uuidCol, nameCol)).
		Group(groupCol + ", " + uuidCol + ", " + nameCol)

	if err := query.Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Error: %v", err),
		})
	}

	// Calculate composite scores (overall_score is Go-computed, not a SQL column)
	for i := range results {
		score := (results[i].VisitedPercentage * 0.4) + (results[i].POSMMPercentage * 0.3) + (results[i].SyncRate * 0.3)
		results[i].OverallScore = math.Round(score*100) / 100

		if score >= 85 {
			results[i].PerformanceRating = "⭐ EXCELLENT (A)"
		} else if score >= 70 {
			results[i].PerformanceRating = "✓ GOOD (B)"
		} else if score >= 50 {
			results[i].PerformanceRating = "△ FAIR (C)"
		} else {
			results[i].PerformanceRating = "✗ POOR (D)"
		}
	}

	// Sort in Go after score computation (overall_score is not a DB column)
	sort.Slice(results, func(i, j int) bool {
		switch sortBy {
		case "total_visits":
			return results[i].TotalVisits > results[j].TotalVisits
		case "pos_visited":
			return results[i].POSVisited > results[j].POSVisited
		case "team_members":
			return results[i].TeamMembers > results[j].TeamMembers
		case "sync_rate":
			return results[i].SyncRate > results[j].SyncRate
		case "visited_percentage":
			return results[i].VisitedPercentage > results[j].VisitedPercentage
		default: // overall_score
			return results[i].OverallScore > results[j].OverallScore
		}
	})

	limitInt, _ := strconv.Atoi(limit)
	if len(results) > limitInt {
		results = results[:limitInt]
	}

	return c.JSON(fiber.Map{
		"status":    "success",
		"count":     len(results),
		"level":     level,
		"period":    fiber.Map{"start": start.Format("2006-01-02"), "end": end.Format("2006-01-02")},
		"data":      results,
		"timestamp": time.Now(),
	})
}

// GetAgentPerformanceDetails - Individual Agent Performance
func GetAgentPerformanceDetails(c *fiber.Ctx) error {
	db := database.DB

	agentUUID := c.Query("agent_uuid")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	includeDaily := c.Query("include_daily", "true") == "true"

	if agentUUID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "agent_uuid is required",
		})
	}

	var start, end time.Time
	if startDate != "" {
		start, _ = time.Parse("2006-01-02", startDate)
	} else {
		start = time.Now().AddDate(0, 0, -7)
	}
	if endDate != "" {
		end, _ = time.Parse("2006-01-02", endDate)
	} else {
		end = time.Now()
	}

	type AgentSummary struct {
		AgentUUID        string  `json:"agent_uuid"`
		AgentName        string  `json:"agent_name"`
		AgentTitle       string  `json:"agent_title"`
		TotalVisits      int64   `json:"total_visits"`
		UniquePOSVisited int64   `json:"unique_pos_visited"`
		ActiveDays       int64   `json:"active_days"`
		AvgVisitsPerDay  float64 `json:"avg_visits_per_day"`
		UnsyncedForms    int64   `json:"unsynced_forms"`
		SyncRate         float64 `json:"sync_rate"`
		CommunesCovered  int64   `json:"communes_covered"`
		PerformanceScore float64 `json:"performance_score"`
	}

	var summary AgentSummary

	db.Table("pos_forms pf").
		Joins("LEFT JOIN users u ON pf.user_uuid = u.uuid").
		Where("pf.user_uuid = ? AND pf.created_at BETWEEN ? AND ?", agentUUID, start, end).
		Select(`
			u.uuid,
			u.fullname,
			u.title,
			COUNT(DISTINCT pf.uuid),
			COUNT(DISTINCT pf.pos_uuid),
			COUNT(DISTINCT DATE(pf.created_at)),
			ROUND(COUNT(DISTINCT pf.uuid)::FLOAT / NULLIF(COUNT(DISTINCT DATE(pf.created_at)), 0), 2),
			COUNT(CASE WHEN pf.sync = false THEN 1 END),
			ROUND(100.0 * COUNT(CASE WHEN pf.sync = true THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2),
			COUNT(DISTINCT pf.commune_uuid)
		`).
		Scan(&summary)

	summary.PerformanceScore = (summary.AvgVisitsPerDay/10)*40 + (summary.SyncRate * 0.3) + (float64(summary.CommunesCovered) / 10)

	response := fiber.Map{
		"agent_summary": summary,
		"period": fiber.Map{
			"start": start.Format("2006-01-02"),
			"end":   end.Format("2006-01-02"),
		},
	}

	if includeDaily {
		type DailyBreakdown struct {
			DateKey         string  `json:"date_key"`
			Visits          int64   `json:"visits"`
			POSCount        int64   `json:"pos_count"`
			Synced          int64   `json:"synced"`
			POSMMPercentage float64 `json:"posmm_percentage"`
		}

		var dailyData []DailyBreakdown
		db.Table("pos_forms pf").
			Where("pf.user_uuid = ? AND pf.created_at BETWEEN ? AND ?", agentUUID, start, end).
			Select(`
				DATE(pf.created_at),
				COUNT(DISTINCT pf.uuid),
				COUNT(DISTINCT pf.pos_uuid),
				COUNT(DISTINCT CASE WHEN pf.sync = true THEN pf.uuid END),
				ROUND(AVG(CASE WHEN pf.price > 0 THEN 100 ELSE 0 END), 2)
			`).
			Group("DATE(pf.created_at)").
			Order("DATE(pf.created_at) DESC").
			Scan(&dailyData)

		response["daily_breakdown"] = dailyData
	}

	return c.JSON(response)
}

// GetPOSLevelInsights - POS Visit Analysis
func GetPOSLevelInsights(c *fiber.Ctx) error {
	db := database.DB

	communeUUID := c.Query("commune_uuid")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	minVisits := c.Query("min_visits", "0")

	if communeUUID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "commune_uuid is required",
		})
	}

	var start, end time.Time
	if startDate != "" {
		start, _ = time.Parse("2006-01-02", startDate)
	} else {
		start = time.Now().AddDate(0, -1, 0)
	}
	if endDate != "" {
		end, _ = time.Parse("2006-01-02", endDate)
	} else {
		end = time.Now()
	}

	type POSInsight struct {
		POSUUID            string  `json:"pos_uuid"`
		POSCode            string  `json:"pos_code"`
		POSName            string  `json:"pos_name"`
		POSType            string  `json:"pos_type"`
		CommuneName        string  `json:"commune_name"`
		VisitsCount        int64   `json:"visits_count"`
		DaysSinceLastVisit int64   `json:"days_since_last_visit"`
		UniqueAgents       int64   `json:"unique_agents"`
		POSMMPercentage    float64 `json:"posmm_percentage"`
		CoverageStatus     string  `json:"coverage_status"`
	}

	var results []POSInsight

	minVisitsInt, _ := strconv.Atoi(minVisits)

	query := db.Table("pos p").
		Joins("LEFT JOIN communes c ON p.commune_uuid = c.uuid").
		Joins("LEFT JOIN pos_forms pf ON p.uuid = pf.pos_uuid AND pf.created_at BETWEEN ? AND ?", start, end).
		Where("p.commune_uuid = ?", communeUUID).
		Select(`
			p.uuid,
			p.code,
			p.name,
			p.type,
			c.name,
			COUNT(DISTINCT pf.uuid),
			ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(pf.created_at))) / 86400)::BIGINT,
			COUNT(DISTINCT pf.user_uuid),
			ROUND(100.0 * COUNT(DISTINCT CASE WHEN pf.price > 0 THEN pf.uuid END) / NULLIF(COUNT(DISTINCT pf.uuid), 0), 2)
		`).
		Group("p.uuid, p.code, p.name, p.type, c.name")

	if minVisitsInt > 0 {
		query = query.Having("COUNT(DISTINCT pf.uuid) >= ?", minVisitsInt)
	}

	if err := query.Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Error: %v", err),
		})
	}

	for i := range results {
		if results[i].VisitsCount == 0 {
			results[i].CoverageStatus = "🔴 NOT_VISITED"
		} else if results[i].DaysSinceLastVisit > 21 {
			results[i].CoverageStatus = "🟠 NEEDS_ATTENTION"
		} else if results[i].DaysSinceLastVisit > 14 {
			results[i].CoverageStatus = "🟡 WARNING"
		} else {
			results[i].CoverageStatus = "🟢 GOOD"
		}
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"commune": communeUUID,
		"count":   len(results),
		"period": fiber.Map{
			"start": start.Format("2006-01-02"),
			"end":   end.Format("2006-01-02"),
		},
		"data": results,
	})
}

// GetKPITargetVsActual - Performance Against Targets
func GetKPITargetVsActual(c *fiber.Ctx) error {
	db := database.DB

	level := c.Query("level", "area")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	var start, end time.Time
	if startDate != "" {
		start, _ = time.Parse("2006-01-02", startDate)
	} else {
		start = time.Now().AddDate(0, 0, -30)
	}
	if endDate != "" {
		end, _ = time.Parse("2006-01-02", endDate)
	} else {
		end = time.Now()
	}

	type TargetAnalysis struct {
		Territory             string  `json:"territory"`
		ActualVisits          int64   `json:"actual_visits"`
		TargetVisits          int64   `json:"target_visits"`
		AchievementPercentage float64 `json:"achievement_percentage"`
		Status                string  `json:"status"`
		RiskLevel             string  `json:"risk_level"`
	}

	var results []TargetAnalysis

	var groupCol, nameCol string
	switch level {
	case "province":
		groupCol = "pf.province_uuid"
		nameCol = "pr.name"
	case "area":
		groupCol = "pf.area_uuid"
		nameCol = "a.name"
	default:
		groupCol = "pf.area_uuid"
		nameCol = "a.name"
	}

	query := db.Table("pos_forms pf").
		Joins("LEFT JOIN users u ON pf.user_uuid = u.uuid").
		Joins("LEFT JOIN areas a ON pf.area_uuid = a.uuid").
		Joins("LEFT JOIN provinces pr ON pf.province_uuid = pr.uuid").
		Where("pf.created_at BETWEEN ? AND ?", start, end).
		Where("pf.deleted_at IS NULL").
		Select(fmt.Sprintf(`
			%s,
			COUNT(DISTINCT pf.uuid),
			CAST(10 * (EXTRACT(DAY FROM ?::timestamp - ?::timestamp) + 1) AS BIGINT),
			ROUND(100.0 * COUNT(DISTINCT pf.uuid) / NULLIF(10 * (EXTRACT(DAY FROM ?::timestamp - ?::timestamp) + 1), 0), 2)
		`, nameCol),
			end, start, end, start).
		Group(groupCol + ", " + nameCol)

	if err := query.Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Error: %v", err),
		})
	}

	for i := range results {
		if results[i].AchievementPercentage >= 100 {
			results[i].Status = "✅ ON_TRACK"
			results[i].RiskLevel = "LOW"
		} else if results[i].AchievementPercentage >= 80 {
			results[i].Status = "⚠️ AT_RISK"
			results[i].RiskLevel = "MEDIUM"
		} else {
			results[i].Status = "❌ OFF_TRACK"
			results[i].RiskLevel = "HIGH"
		}
	}

	return c.JSON(fiber.Map{
		"status": "success",
		"count":  len(results),
		"period": fiber.Map{
			"start": start.Format("2006-01-02"),
			"end":   end.Format("2006-01-02"),
		},
		"data": results,
	})
}

// GetTeamAbsenceAnalysis - Inactive Agents
func GetTeamAbsenceAnalysis(c *fiber.Ctx) error {
	db := database.DB

	daysInactive := c.Query("days_inactive", "7")
	daysInt, _ := strconv.Atoi(daysInactive)
	inactiveThreshold := time.Now().AddDate(0, 0, -daysInt)

	type AbsenceAlert struct {
		AgentUUID    string `json:"agent_uuid"`
		AgentName    string `json:"agent_name"`
		AgentTitle   string `json:"agent_title"`
		DaysInactive int64  `json:"days_inactive"`
		AlertLevel   string `json:"alert_level"`
	}

	var results []AbsenceAlert

	query := db.Table("users u").
		Joins("LEFT JOIN pos_forms pf ON u.uuid = pf.user_uuid").
		Where("u.title IN ?", []string{"ASM", "Supervisor", "DR", "Cyclo", "Agent"}).
		Where("u.status = true").
		Having("MAX(pf.created_at) < ? OR MAX(pf.created_at) IS NULL", inactiveThreshold).
		Select(`
			u.uuid        AS agent_uuid,
			u.fullname    AS agent_name,
			u.title       AS agent_title,
			COALESCE(ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(pf.created_at))) / 86400)::BIGINT, ?) AS days_inactive,
			CASE
				WHEN MAX(pf.created_at) IS NULL THEN '🔴 CRITICAL'
				WHEN ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(pf.created_at))) / 86400) > 14 THEN '🔴 CRITICAL'
				WHEN ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(pf.created_at))) / 86400) > 7  THEN '🟡 WARNING'
				ELSE '🟢 OK'
			END AS alert_level
		`, daysInt).
		Group("u.uuid, u.fullname, u.title").
		Order("days_inactive DESC")

	if err := query.Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Error: %v", err),
		})
	}

	criticalCount := 0
	warningCount := 0
	for _, r := range results {
		if r.AlertLevel == "🔴 CRITICAL" {
			criticalCount++
		} else if r.AlertLevel == "🟡 WARNING" {
			warningCount++
		}
	}

	return c.JSON(fiber.Map{
		"status":         "success",
		"total_agents":   len(results),
		"critical_count": criticalCount,
		"warning_count":  warningCount,
		"data":           results,
	})
}

// GetPeriodComparison - Trend Analysis
func GetPeriodComparison(c *fiber.Ctx) error {
	db := database.DB

	period := c.Query("period", "monthly")
	lastPeriods := c.Query("periods", "3")
	periodsInt, _ := strconv.Atoi(lastPeriods)

	type PeriodData struct {
		PeriodLabel     string  `json:"period_label"`
		Visits          int64   `json:"visits"`
		POSVisited      int64   `json:"pos_visited"`
		SyncRate        float64 `json:"sync_rate"`
		POSMMPercentage float64 `json:"posmm_percentage"`
	}

	var results []PeriodData
	now := time.Now()

	for i := periodsInt - 1; i >= 0; i-- {
		var start, end time.Time
		var periodLabel string

		if period == "weekly" {
			end = now.AddDate(0, 0, -7*i)
			start = end.AddDate(0, 0, -6)
			periodLabel = fmt.Sprintf("W%d", start.YearDay()/7)
		} else {
			endDate := now.AddDate(0, -i, 0)
			start = time.Date(endDate.Year(), endDate.Month(), 1, 0, 0, 0, 0, endDate.Location())
			end = start.AddDate(0, 1, -1)
			periodLabel = start.Format("Jan 2006")
		}

		var data PeriodData
		if err := db.Table("pos_forms pf").
			Where("pf.created_at BETWEEN ? AND ?", start, end).
			Where("pf.deleted_at IS NULL").
			Select(`
				COUNT(DISTINCT pf.uuid),
				COUNT(DISTINCT pf.pos_uuid),
				ROUND(100.0 * COUNT(CASE WHEN pf.sync = true THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2),
				ROUND(AVG(CASE WHEN pf.price > 0 THEN 100 ELSE 0 END), 2)
			`).
			Scan(&data).Error; err != nil {
			continue
		}

		data.PeriodLabel = periodLabel
		results = append(results, data)
	}

	return c.JSON(fiber.Map{
		"status": "success",
		"period": period,
		"count":  len(results),
		"data":   results,
	})
}

// GetNDAnalysisByTerritory - Numeric Distribution Analysis
func GetNDAnalysisByTerritory(c *fiber.Ctx) error {
	db := database.DB

	level := c.Query("level", "commune")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	var start, end time.Time
	if startDate != "" {
		start, _ = time.Parse("2006-01-02", startDate)
	} else {
		start = time.Now().AddDate(0, -1, 0)
	}
	if endDate != "" {
		end, _ = time.Parse("2006-01-02", endDate)
	} else {
		end = time.Now()
	}

	type NDAnalysis struct {
		Territory        string  `json:"territory"`
		TotalPOSVisited  int64   `json:"total_pos_visited"`
		NDPercentage     float64 `json:"nd_percentage"`
		OOSPercentage    float64 `json:"oos_percentage"`
		POSMMIntegration float64 `json:"posmm_integration"`
		DensityScore     float64 `json:"density_score"`
	}

	var results []NDAnalysis

	var groupCol, nameCol string
	switch level {
	case "province":
		groupCol = "pf.province_uuid"
		nameCol = "pr.name"
	case "area":
		groupCol = "pf.area_uuid"
		nameCol = "a.name"
	default:
		groupCol = "pf.commune_uuid"
		nameCol = "com.name"
	}

	query := db.Table("pos_forms pf").
		Joins("LEFT JOIN areas a ON pf.area_uuid = a.uuid").
		Joins("LEFT JOIN communes com ON pf.commune_uuid = com.uuid").
		Joins("LEFT JOIN provinces pr ON pf.province_uuid = pr.uuid").
		Where("pf.created_at BETWEEN ? AND ?", start, end).
		Where("pf.deleted_at IS NULL").
		Select(fmt.Sprintf(`
			%s,
			COUNT(DISTINCT pf.pos_uuid),
			ROUND(100.0 * COUNT(DISTINCT CASE WHEN pf.price > 0 THEN pf.pos_uuid END) / NULLIF(COUNT(DISTINCT pf.pos_uuid), 0), 2),
			ROUND(100.0 * COUNT(DISTINCT CASE WHEN pf.price = 0 THEN pf.pos_uuid END) / NULLIF(COUNT(DISTINCT pf.pos_uuid), 0), 2),
			ROUND(AVG(CASE WHEN pf.price > 0 THEN 100 ELSE 0 END), 2)
		`, nameCol)).
		Group(groupCol + ", " + nameCol)

	if err := query.Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Error: %v", err),
		})
	}

	for i := range results {
		results[i].DensityScore = (results[i].NDPercentage + results[i].POSMMIntegration) / 2
	}

	return c.JSON(fiber.Map{
		"status": "success",
		"level":  level,
		"count":  len(results),
		"period": fiber.Map{
			"start": start.Format("2006-01-02"),
			"end":   end.Format("2006-01-02"),
		},
		"data": results,
	})
}

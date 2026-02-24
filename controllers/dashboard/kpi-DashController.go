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

// KpiUserVisitSummary returns a per-user summary table:
// Name | Daily | Monthly | Yearly | Total visits (selected range)
// Targets are computed as: role_rate × number_of_days
//
//	Daily   → rate × 1
//	Monthly → rate × days_in_current_month
//	Yearly  → rate × days_in_current_year
//	Range   → rate × (end_date − start_date + 1)   ← selected by the user
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
		UserUUID string `json:"user_uuid"`
		Name     string `json:"name"`
		Title    string `json:"title"`
		// Fixed windows
		DailyVisits   int     `json:"daily_visits"`
		DailyTarget   int     `json:"daily_target"`
		DailyPct      float64 `json:"daily_pct"`
		MonthlyVisits int     `json:"monthly_visits"`
		MonthlyTarget int     `json:"monthly_target"`
		MonthlyPct    float64 `json:"monthly_pct"`
		YearlyVisits  int     `json:"yearly_visits"`
		YearlyTarget  int     `json:"yearly_target"`
		YearlyPct     float64 `json:"yearly_pct"`
		// Selected date-range (what the user picked on the frontend)
		TotalVisits int     `json:"total_visits"`
		RangeTarget int     `json:"range_target"`
		RangePct    float64 `json:"range_pct"`
	}

	// num_selected_days = end_date - start_date + 1
	query := db.Table("pos_forms").
		Select(`
			users.uuid     AS user_uuid,
			users.fullname AS name,
			users.title    AS title,

			-- ── DAILY (today) ────────────────────────────────────────────
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

			-- ── MONTHLY (current calendar month) ─────────────────────────
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

			-- ── YEARLY (current calendar year) ────────────────────────────
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

			-- ── SELECTED DATE RANGE  (rate × selected_days) ──────────────
			-- total_visits  = visits within [start_date, end_date]
			COUNT(pos_forms.uuid) FILTER (WHERE pos_forms.created_at BETWEEN ?::date AND ?::date)
				AS total_visits,
			-- range_target  = rate × (end_date − start_date + 1)
			(CASE
				WHEN users.title = 'ASM'          THEN 10
				WHEN users.title = 'Supervisor'   THEN 20
				WHEN users.title IN ('DR','Cyclo') THEN 40
				ELSE 0
			END * ((?::date - ?::date) + 1))
				AS range_target,
			-- range_pct     = total_visits / range_target × 100
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
			// FILTER BETWEEN binds (total_visits)
			start_date, end_date,
			// range_target binds
			end_date, start_date,
			// range_pct FILTER BETWEEN binds
			start_date, end_date,
			// range_pct divisor binds
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

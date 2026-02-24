package dashboard

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║          SALES EVOLUTION DASHBOARD — COMPREHENSIVE MARKET ANALYTICS     ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║  1.  TypePos Table Views   (Province / Area / SubArea / Commune)        ║
// ║  2.  Price Analysis Tables (Province / Area / SubArea / Commune)        ║
// ║  3.  Monthly Sales Line Chart  — brand + farde + sold trend             ║
// ║  4.  Sales Growth Rate         — period-over-period comparison          ║
// ║  5.  Brand Competition Matrix  — brand share by territory               ║
// ║  6.  Top 10 POS Ranking        — best performing points of sale         ║
// ║  7.  Sales Representative Scorecard                                     ║
// ║  8.  Revenue Heatmap by Day-of-Week                                     ║
// ╚══════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 — SALES BY POS TYPE : Table Views
// Breaks down volume (number_farde), sold quantity, revenue (pos_forms.price)
// and computes market share per POS type within a geographic scope.
// ─────────────────────────────────────────────────────────────────────────────

// TypePosTableProvince — Province-level sales breakdown by POS type
func TypePosTableProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		ProvinceName     string  `json:"province_name"`
		ProvinceUUID     string  `json:"province_uuid"`
		PosType          string  `json:"pos_type"`
		TotalVisits      int64   `json:"total_visits"`
		TotalPos         int64   `json:"total_pos"`
		TotalFarde       float64 `json:"total_farde"`
		TotalSold        float64 `json:"total_sold"`
		TotalRevenue     float64 `json:"total_revenue"`
		AvgFardePerVisit float64 `json:"avg_farde_per_visit"`
		AvgSoldPerVisit  float64 `json:"avg_sold_per_visit"`
		MarketShareFarde float64 `json:"market_share_farde"`
		MarketShareSold  float64 `json:"market_share_sold"`
	}

	sqlQuery := `
		WITH global AS (
			SELECT
				COALESCE(SUM(pfi.number_farde), 0) AS g_farde,
				COALESCE(SUM(pfi.sold), 0)          AS g_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = ? AND pf.province_uuid = ?
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		)
		SELECT
			pr.name                                          AS province_name,
			pr.uuid                                          AS province_uuid,
			COALESCE(NULLIF(p.postype, ''), 'Non défini')   AS pos_type,
			COUNT(DISTINCT pf.uuid)                          AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                      AS total_pos,
			ROUND(SUM(pfi.number_farde)::numeric, 2)         AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                 AS total_sold,
			ROUND(SUM(pf.price)::numeric, 2)                 AS total_revenue,
			ROUND((SUM(pfi.number_farde) / NULLIF(COUNT(DISTINCT pf.uuid), 0))::numeric, 2) AS avg_farde_per_visit,
			ROUND((SUM(pfi.sold) / NULLIF(COUNT(DISTINCT pf.uuid), 0))::numeric, 2)         AS avg_sold_per_visit,
			ROUND((SUM(pfi.number_farde) * 100.0 / NULLIF((SELECT g_farde FROM global), 0))::numeric, 2) AS market_share_farde,
			ROUND((SUM(pfi.sold) * 100.0 / NULLIF((SELECT g_sold FROM global), 0))::numeric, 2)          AS market_share_sold
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN pos p        ON pf.pos_uuid = p.uuid
		INNER JOIN provinces pr ON pf.province_uuid = pr.uuid
		WHERE pf.country_uuid = ? AND pf.province_uuid = ?
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY pr.name, pr.uuid, p.postype
		ORDER BY total_farde DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery,
		country_uuid, province_uuid, start_date, end_date,
		country_uuid, province_uuid, start_date, end_date,
	).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Sales by POS type — Province", "data": results})
}

// TypePosTableArea — Area-level sales breakdown by POS type
func TypePosTableArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		AreaName         string  `json:"area_name"`
		AreaUUID         string  `json:"area_uuid"`
		PosType          string  `json:"pos_type"`
		TotalVisits      int64   `json:"total_visits"`
		TotalPos         int64   `json:"total_pos"`
		TotalFarde       float64 `json:"total_farde"`
		TotalSold        float64 `json:"total_sold"`
		TotalRevenue     float64 `json:"total_revenue"`
		AvgFardePerVisit float64 `json:"avg_farde_per_visit"`
		AvgSoldPerVisit  float64 `json:"avg_sold_per_visit"`
		MarketShareFarde float64 `json:"market_share_farde"`
		MarketShareSold  float64 `json:"market_share_sold"`
	}

	sqlQuery := `
		WITH global AS (
			SELECT
				COALESCE(SUM(pfi.number_farde), 0) AS g_farde,
				COALESCE(SUM(pfi.sold), 0)          AS g_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		)
		SELECT
			a.name                                          AS area_name,
			a.uuid                                          AS area_uuid,
			COALESCE(NULLIF(p.postype, ''), 'Non défini')  AS pos_type,
			COUNT(DISTINCT pf.uuid)                         AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                     AS total_pos,
			ROUND(SUM(pfi.number_farde)::numeric, 2)        AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                AS total_sold,
			ROUND(SUM(pf.price)::numeric, 2)                AS total_revenue,
			ROUND((SUM(pfi.number_farde) / NULLIF(COUNT(DISTINCT pf.uuid), 0))::numeric, 2) AS avg_farde_per_visit,
			ROUND((SUM(pfi.sold) / NULLIF(COUNT(DISTINCT pf.uuid), 0))::numeric, 2)         AS avg_sold_per_visit,
			ROUND((SUM(pfi.number_farde) * 100.0 / NULLIF((SELECT g_farde FROM global), 0))::numeric, 2) AS market_share_farde,
			ROUND((SUM(pfi.sold) * 100.0 / NULLIF((SELECT g_sold FROM global), 0))::numeric, 2)          AS market_share_sold
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN pos p        ON pf.pos_uuid = p.uuid
		INNER JOIN areas a      ON pf.area_uuid = a.uuid
		WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY a.name, a.uuid, p.postype
		ORDER BY total_farde DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, start_date, end_date,
	).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Sales by POS type — Area", "data": results})
}

// TypePosTableSubArea — SubArea-level sales breakdown by POS type
func TypePosTableSubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		SubAreaName      string  `json:"sub_area_name"`
		SubAreaUUID      string  `json:"sub_area_uuid"`
		PosType          string  `json:"pos_type"`
		TotalVisits      int64   `json:"total_visits"`
		TotalPos         int64   `json:"total_pos"`
		TotalFarde       float64 `json:"total_farde"`
		TotalSold        float64 `json:"total_sold"`
		TotalRevenue     float64 `json:"total_revenue"`
		AvgFardePerVisit float64 `json:"avg_farde_per_visit"`
		AvgSoldPerVisit  float64 `json:"avg_sold_per_visit"`
		MarketShareFarde float64 `json:"market_share_farde"`
		MarketShareSold  float64 `json:"market_share_sold"`
	}

	sqlQuery := `
		WITH global AS (
			SELECT
				COALESCE(SUM(pfi.number_farde), 0) AS g_farde,
				COALESCE(SUM(pfi.sold), 0)          AS g_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ? AND pf.sub_area_uuid = ?
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		)
		SELECT
			sa.name                                          AS sub_area_name,
			sa.uuid                                          AS sub_area_uuid,
			COALESCE(NULLIF(p.postype, ''), 'Non défini')   AS pos_type,
			COUNT(DISTINCT pf.uuid)                          AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                      AS total_pos,
			ROUND(SUM(pfi.number_farde)::numeric, 2)         AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                 AS total_sold,
			ROUND(SUM(pf.price)::numeric, 2)                 AS total_revenue,
			ROUND((SUM(pfi.number_farde) / NULLIF(COUNT(DISTINCT pf.uuid), 0))::numeric, 2) AS avg_farde_per_visit,
			ROUND((SUM(pfi.sold) / NULLIF(COUNT(DISTINCT pf.uuid), 0))::numeric, 2)         AS avg_sold_per_visit,
			ROUND((SUM(pfi.number_farde) * 100.0 / NULLIF((SELECT g_farde FROM global), 0))::numeric, 2) AS market_share_farde,
			ROUND((SUM(pfi.sold) * 100.0 / NULLIF((SELECT g_sold FROM global), 0))::numeric, 2)          AS market_share_sold
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf  ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN pos p         ON pf.pos_uuid = p.uuid
		INNER JOIN sub_areas sa  ON pf.sub_area_uuid = sa.uuid
		WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ? AND pf.sub_area_uuid = ?
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY sa.name, sa.uuid, p.postype
		ORDER BY total_farde DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date,
	).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Sales by POS type — SubArea", "data": results})
}

// TypePosTableCommune — Commune-level sales breakdown by POS type
func TypePosTableCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		CommuneName      string  `json:"commune_name"`
		CommuneUUID      string  `json:"commune_uuid"`
		PosType          string  `json:"pos_type"`
		TotalVisits      int64   `json:"total_visits"`
		TotalPos         int64   `json:"total_pos"`
		TotalFarde       float64 `json:"total_farde"`
		TotalSold        float64 `json:"total_sold"`
		TotalRevenue     float64 `json:"total_revenue"`
		AvgFardePerVisit float64 `json:"avg_farde_per_visit"`
		AvgSoldPerVisit  float64 `json:"avg_sold_per_visit"`
		MarketShareFarde float64 `json:"market_share_farde"`
		MarketShareSold  float64 `json:"market_share_sold"`
	}

	sqlQuery := `
		WITH global AS (
			SELECT
				COALESCE(SUM(pfi.number_farde), 0) AS g_farde,
				COALESCE(SUM(pfi.sold), 0)          AS g_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?
			  AND pf.sub_area_uuid = ? AND pf.commune_uuid = ?
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		)
		SELECT
			co.name                                          AS commune_name,
			co.uuid                                          AS commune_uuid,
			COALESCE(NULLIF(p.postype, ''), 'Non défini')   AS pos_type,
			COUNT(DISTINCT pf.uuid)                          AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                      AS total_pos,
			ROUND(SUM(pfi.number_farde)::numeric, 2)         AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                 AS total_sold,
			ROUND(SUM(pf.price)::numeric, 2)                 AS total_revenue,
			ROUND((SUM(pfi.number_farde) / NULLIF(COUNT(DISTINCT pf.uuid), 0))::numeric, 2) AS avg_farde_per_visit,
			ROUND((SUM(pfi.sold) / NULLIF(COUNT(DISTINCT pf.uuid), 0))::numeric, 2)         AS avg_sold_per_visit,
			ROUND((SUM(pfi.number_farde) * 100.0 / NULLIF((SELECT g_farde FROM global), 0))::numeric, 2) AS market_share_farde,
			ROUND((SUM(pfi.sold) * 100.0 / NULLIF((SELECT g_sold FROM global), 0))::numeric, 2)          AS market_share_sold
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf  ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN pos p         ON pf.pos_uuid = p.uuid
		INNER JOIN communes co   ON pf.commune_uuid = co.uuid
		WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?
		  AND pf.sub_area_uuid = ? AND pf.commune_uuid = ?
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY co.name, co.uuid, p.postype
		ORDER BY total_farde DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, commune_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, commune_uuid, start_date, end_date,
	).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Sales by POS type — Commune", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 — PRICE ANALYSIS : Table Views
// Analyses price distribution (avg, min, max, revenue) per brand across
// geographic levels. Useful to spot pricing anomalies and coverage gaps.
// ─────────────────────────────────────────────────────────────────────────────

// PriceTableProvince — Province-level price analysis per brand
func PriceTableProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		ProvinceName string  `json:"province_name"`
		ProvinceUUID string  `json:"province_uuid"`
		BrandName    string  `json:"brand_name"`
		TotalVisits  int64   `json:"total_visits"`
		TotalPos     int64   `json:"total_pos"`
		TotalRevenue float64 `json:"total_revenue"`
		AvgPrice     float64 `json:"avg_price"`
		MinPrice     float64 `json:"min_price"`
		MaxPrice     float64 `json:"max_price"`
		TotalFarde   float64 `json:"total_farde"`
		TotalSold    float64 `json:"total_sold"`
		RevenueShare float64 `json:"revenue_share"`
	}

	sqlQuery := `
		WITH global_rev AS (
			SELECT COALESCE(SUM(pf.price), 0) AS g_rev
			FROM pos_forms pf
			WHERE pf.country_uuid = ? AND pf.province_uuid = ?
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL
		)
		SELECT
			pr.name                                              AS province_name,
			pr.uuid                                              AS province_uuid,
			b.name                                               AS brand_name,
			COUNT(DISTINCT pf.uuid)                              AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                          AS total_pos,
			ROUND(SUM(pf.price)::numeric, 2)                     AS total_revenue,
			ROUND(AVG(pf.price)::numeric, 2)                     AS avg_price,
			ROUND(MIN(pf.price)::numeric, 2)                     AS min_price,
			ROUND(MAX(pf.price)::numeric, 2)                     AS max_price,
			ROUND(SUM(pfi.number_farde)::numeric, 2)             AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                     AS total_sold,
			ROUND((SUM(pf.price) * 100.0 / NULLIF((SELECT g_rev FROM global_rev), 0))::numeric, 2) AS revenue_share
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf  ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN brands b      ON pfi.brand_uuid = b.uuid
		INNER JOIN provinces pr  ON pf.province_uuid = pr.uuid
		WHERE pf.country_uuid = ? AND pf.province_uuid = ?
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY pr.name, pr.uuid, b.name
		ORDER BY total_revenue DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery,
		country_uuid, province_uuid, start_date, end_date,
		country_uuid, province_uuid, start_date, end_date,
	).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Price analysis — Province", "data": results})
}

// PriceTableArea — Area-level price analysis per brand
func PriceTableArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		AreaName     string  `json:"area_name"`
		AreaUUID     string  `json:"area_uuid"`
		BrandName    string  `json:"brand_name"`
		TotalVisits  int64   `json:"total_visits"`
		TotalPos     int64   `json:"total_pos"`
		TotalRevenue float64 `json:"total_revenue"`
		AvgPrice     float64 `json:"avg_price"`
		MinPrice     float64 `json:"min_price"`
		MaxPrice     float64 `json:"max_price"`
		TotalFarde   float64 `json:"total_farde"`
		TotalSold    float64 `json:"total_sold"`
		RevenueShare float64 `json:"revenue_share"`
	}

	sqlQuery := `
		WITH global_rev AS (
			SELECT COALESCE(SUM(pf.price), 0) AS g_rev
			FROM pos_forms pf
			WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL
		)
		SELECT
			a.name                                               AS area_name,
			a.uuid                                               AS area_uuid,
			b.name                                               AS brand_name,
			COUNT(DISTINCT pf.uuid)                              AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                          AS total_pos,
			ROUND(SUM(pf.price)::numeric, 2)                     AS total_revenue,
			ROUND(AVG(pf.price)::numeric, 2)                     AS avg_price,
			ROUND(MIN(pf.price)::numeric, 2)                     AS min_price,
			ROUND(MAX(pf.price)::numeric, 2)                     AS max_price,
			ROUND(SUM(pfi.number_farde)::numeric, 2)             AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                     AS total_sold,
			ROUND((SUM(pf.price) * 100.0 / NULLIF((SELECT g_rev FROM global_rev), 0))::numeric, 2) AS revenue_share
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN brands b     ON pfi.brand_uuid = b.uuid
		INNER JOIN areas a      ON pf.area_uuid = a.uuid
		WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY a.name, a.uuid, b.name
		ORDER BY total_revenue DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, start_date, end_date,
	).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Price analysis — Area", "data": results})
}

// PriceTableSubArea — SubArea-level price analysis per brand
func PriceTableSubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		SubAreaName  string  `json:"sub_area_name"`
		SubAreaUUID  string  `json:"sub_area_uuid"`
		BrandName    string  `json:"brand_name"`
		TotalVisits  int64   `json:"total_visits"`
		TotalPos     int64   `json:"total_pos"`
		TotalRevenue float64 `json:"total_revenue"`
		AvgPrice     float64 `json:"avg_price"`
		MinPrice     float64 `json:"min_price"`
		MaxPrice     float64 `json:"max_price"`
		TotalFarde   float64 `json:"total_farde"`
		TotalSold    float64 `json:"total_sold"`
		RevenueShare float64 `json:"revenue_share"`
	}

	sqlQuery := `
		WITH global_rev AS (
			SELECT COALESCE(SUM(pf.price), 0) AS g_rev
			FROM pos_forms pf
			WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ? AND pf.sub_area_uuid = ?
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL
		)
		SELECT
			sa.name                                              AS sub_area_name,
			sa.uuid                                              AS sub_area_uuid,
			b.name                                               AS brand_name,
			COUNT(DISTINCT pf.uuid)                              AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                          AS total_pos,
			ROUND(SUM(pf.price)::numeric, 2)                     AS total_revenue,
			ROUND(AVG(pf.price)::numeric, 2)                     AS avg_price,
			ROUND(MIN(pf.price)::numeric, 2)                     AS min_price,
			ROUND(MAX(pf.price)::numeric, 2)                     AS max_price,
			ROUND(SUM(pfi.number_farde)::numeric, 2)             AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                     AS total_sold,
			ROUND((SUM(pf.price) * 100.0 / NULLIF((SELECT g_rev FROM global_rev), 0))::numeric, 2) AS revenue_share
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf  ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN brands b      ON pfi.brand_uuid = b.uuid
		INNER JOIN sub_areas sa  ON pf.sub_area_uuid = sa.uuid
		WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ? AND pf.sub_area_uuid = ?
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY sa.name, sa.uuid, b.name
		ORDER BY total_revenue DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date,
	).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Price analysis — SubArea", "data": results})
}

// PriceTableCommune — Commune-level price analysis per brand
func PriceTableCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		CommuneName  string  `json:"commune_name"`
		CommuneUUID  string  `json:"commune_uuid"`
		BrandName    string  `json:"brand_name"`
		TotalVisits  int64   `json:"total_visits"`
		TotalPos     int64   `json:"total_pos"`
		TotalRevenue float64 `json:"total_revenue"`
		AvgPrice     float64 `json:"avg_price"`
		MinPrice     float64 `json:"min_price"`
		MaxPrice     float64 `json:"max_price"`
		TotalFarde   float64 `json:"total_farde"`
		TotalSold    float64 `json:"total_sold"`
		RevenueShare float64 `json:"revenue_share"`
	}

	sqlQuery := `
		WITH global_rev AS (
			SELECT COALESCE(SUM(pf.price), 0) AS g_rev
			FROM pos_forms pf
			WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?
			  AND pf.sub_area_uuid = ? AND pf.commune_uuid = ?
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL
		)
		SELECT
			co.name                                              AS commune_name,
			co.uuid                                              AS commune_uuid,
			b.name                                               AS brand_name,
			COUNT(DISTINCT pf.uuid)                              AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                          AS total_pos,
			ROUND(SUM(pf.price)::numeric, 2)                     AS total_revenue,
			ROUND(AVG(pf.price)::numeric, 2)                     AS avg_price,
			ROUND(MIN(pf.price)::numeric, 2)                     AS min_price,
			ROUND(MAX(pf.price)::numeric, 2)                     AS max_price,
			ROUND(SUM(pfi.number_farde)::numeric, 2)             AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                     AS total_sold,
			ROUND((SUM(pf.price) * 100.0 / NULLIF((SELECT g_rev FROM global_rev), 0))::numeric, 2) AS revenue_share
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf  ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN brands b      ON pfi.brand_uuid = b.uuid
		INNER JOIN communes co   ON pf.commune_uuid = co.uuid
		WHERE pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?
		  AND pf.sub_area_uuid = ? AND pf.commune_uuid = ?
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY co.name, co.uuid, b.name
		ORDER BY total_revenue DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, commune_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, commune_uuid, start_date, end_date,
	).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Price analysis — Commune", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 — MONTHLY SALES EVOLUTION LINE CHART
// Returns one row per (month, brand) with farde, sold, revenue and MoM growth.
// Perfect for a multi-series time-series chart on the frontend.
// Query params: country_uuid, province_uuid?, area_uuid?, sub_area_uuid?,
//               commune_uuid?, brand_uuid?, start_date, end_date
// ─────────────────────────────────────────────────────────────────────────────

func SalesEvolutionByMonth(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	brand_uuid := c.Query("brand_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		YearMonth    string  `json:"year_month"` // e.g. "2025-03"
		BrandName    string  `json:"brand_name"`
		TotalVisits  int64   `json:"total_visits"`
		TotalPos     int64   `json:"total_pos"`
		TotalFarde   float64 `json:"total_farde"`
		TotalSold    float64 `json:"total_sold"`
		TotalRevenue float64 `json:"total_revenue"`
		GrowthFarde  float64 `json:"growth_farde_pct"` // MoM % change
		GrowthSold   float64 `json:"growth_sold_pct"`
	}

	// Build dynamic WHERE clauses for optional filters
	filters := "pf.country_uuid = @country_uuid AND pf.created_at BETWEEN @start_date AND @end_date AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL"
	params := map[string]interface{}{
		"country_uuid": country_uuid,
		"start_date":   start_date,
		"end_date":     end_date,
	}
	if province_uuid != "" {
		filters += " AND pf.province_uuid = @province_uuid"
		params["province_uuid"] = province_uuid
	}
	if area_uuid != "" {
		filters += " AND pf.area_uuid = @area_uuid"
		params["area_uuid"] = area_uuid
	}
	if sub_area_uuid != "" {
		filters += " AND pf.sub_area_uuid = @sub_area_uuid"
		params["sub_area_uuid"] = sub_area_uuid
	}
	if commune_uuid != "" {
		filters += " AND pf.commune_uuid = @commune_uuid"
		params["commune_uuid"] = commune_uuid
	}
	if brand_uuid != "" {
		filters += " AND pfi.brand_uuid = @brand_uuid"
		params["brand_uuid"] = brand_uuid
	}

	sqlQuery := `
		WITH monthly AS (
			SELECT
				TO_CHAR(pf.created_at, 'YYYY-MM')           AS year_month,
				b.name                                        AS brand_name,
				COUNT(DISTINCT pf.uuid)                       AS total_visits,
				COUNT(DISTINCT pf.pos_uuid)                   AS total_pos,
				ROUND(SUM(pfi.number_farde)::numeric, 2)      AS total_farde,
				ROUND(SUM(pfi.sold)::numeric, 2)              AS total_sold,
				ROUND(SUM(pf.price)::numeric, 2)              AS total_revenue
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN brands b     ON pfi.brand_uuid = b.uuid
			WHERE ` + filters + `
			GROUP BY year_month, b.name
		)
		SELECT
			year_month,
			brand_name,
			total_visits,
			total_pos,
			total_farde,
			total_sold,
			total_revenue,
			ROUND(((total_farde - LAG(total_farde) OVER (PARTITION BY brand_name ORDER BY year_month))
				* 100.0 / NULLIF(LAG(total_farde) OVER (PARTITION BY brand_name ORDER BY year_month), 0))::numeric, 2) AS growth_farde_pct,
			ROUND(((total_sold - LAG(total_sold) OVER (PARTITION BY brand_name ORDER BY year_month))
				* 100.0 / NULLIF(LAG(total_sold) OVER (PARTITION BY brand_name ORDER BY year_month), 0))::numeric, 2)  AS growth_sold_pct
		FROM monthly
		ORDER BY brand_name, year_month;
	`

	var results []Result
	err := db.Raw(sqlQuery, params).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch monthly evolution", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Monthly sales evolution", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 — PERIOD-OVER-PERIOD SALES GROWTH RATE
// Compares two date ranges (current vs previous) and computes absolute delta
// and % growth per brand.  Enables instant YoY / MoM growth scorecards.
// Query params: country_uuid, province_uuid?, area_uuid?, sub_area_uuid?,
//               commune_uuid?, curr_start, curr_end, prev_start, prev_end
// ─────────────────────────────────────────────────────────────────────────────

func SalesGrowthRate(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	curr_start := c.Query("curr_start")
	curr_end := c.Query("curr_end")
	prev_start := c.Query("prev_start")
	prev_end := c.Query("prev_end")

	type Result struct {
		BrandName        string  `json:"brand_name"`
		CurrFarde        float64 `json:"curr_farde"`
		PrevFarde        float64 `json:"prev_farde"`
		DeltaFarde       float64 `json:"delta_farde"`
		GrowthFardePct   float64 `json:"growth_farde_pct"`
		CurrSold         float64 `json:"curr_sold"`
		PrevSold         float64 `json:"prev_sold"`
		DeltaSold        float64 `json:"delta_sold"`
		GrowthSoldPct    float64 `json:"growth_sold_pct"`
		CurrRevenue      float64 `json:"curr_revenue"`
		PrevRevenue      float64 `json:"prev_revenue"`
		DeltaRevenue     float64 `json:"delta_revenue"`
		GrowthRevenuePct float64 `json:"growth_revenue_pct"`
		CurrVisits       int64   `json:"curr_visits"`
		PrevVisits       int64   `json:"prev_visits"`
		Trend            string  `json:"trend"` // "UP" | "DOWN" | "STABLE"
	}

	geoFilterBase := "pf.country_uuid = ?"
	geoArgs := []interface{}{country_uuid, country_uuid}

	extraFilter := ""
	if province_uuid != "" {
		extraFilter += " AND pf.province_uuid = ?"
		geoArgs = append([]interface{}{country_uuid, province_uuid}, country_uuid, province_uuid)
		geoArgs = []interface{}{country_uuid, province_uuid, country_uuid, province_uuid}
	}
	if area_uuid != "" {
		extraFilter += " AND pf.area_uuid = ?"
		geoArgs = []interface{}{country_uuid, province_uuid, area_uuid, country_uuid, province_uuid, area_uuid}
	}
	if sub_area_uuid != "" {
		extraFilter += " AND pf.sub_area_uuid = ?"
		geoArgs = []interface{}{country_uuid, province_uuid, area_uuid, sub_area_uuid, country_uuid, province_uuid, area_uuid, sub_area_uuid}
	}
	if commune_uuid != "" {
		extraFilter += " AND pf.commune_uuid = ?"
		geoArgs = []interface{}{country_uuid, province_uuid, area_uuid, sub_area_uuid, commune_uuid, country_uuid, province_uuid, area_uuid, sub_area_uuid, commune_uuid}
	}

	geoWhere := geoFilterBase + extraFilter

	// Inject date ranges into args
	// Pattern: curr CTE args, prev CTE args
	var fullArgs []interface{}
	for _, a := range geoArgs[:len(geoArgs)/2] {
		fullArgs = append(fullArgs, a)
	}
	fullArgs = append(fullArgs, curr_start, curr_end)
	for _, a := range geoArgs[len(geoArgs)/2:] {
		fullArgs = append(fullArgs, a)
	}
	fullArgs = append(fullArgs, prev_start, prev_end)

	sqlQuery := `
		WITH curr AS (
			SELECT
				b.name                                    AS brand_name,
				ROUND(SUM(pfi.number_farde)::numeric, 2)  AS farde,
				ROUND(SUM(pfi.sold)::numeric, 2)           AS sold,
				ROUND(SUM(pf.price)::numeric, 2)           AS revenue,
				COUNT(DISTINCT pf.uuid)                    AS visits
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN brands b     ON pfi.brand_uuid = b.uuid
			WHERE ` + geoWhere + ` AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY b.name
		),
		prev AS (
			SELECT
				b.name                                    AS brand_name,
				ROUND(SUM(pfi.number_farde)::numeric, 2)  AS farde,
				ROUND(SUM(pfi.sold)::numeric, 2)           AS sold,
				ROUND(SUM(pf.price)::numeric, 2)           AS revenue,
				COUNT(DISTINCT pf.uuid)                    AS visits
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN brands b     ON pfi.brand_uuid = b.uuid
			WHERE ` + geoWhere + ` AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY b.name
		)
		SELECT
			COALESCE(c.brand_name, p.brand_name)                        AS brand_name,
			COALESCE(c.farde, 0)                                         AS curr_farde,
			COALESCE(p.farde, 0)                                         AS prev_farde,
			ROUND((COALESCE(c.farde,0) - COALESCE(p.farde,0))::numeric, 2) AS delta_farde,
			ROUND(((COALESCE(c.farde,0) - COALESCE(p.farde,0)) * 100.0 / NULLIF(COALESCE(p.farde,0), 0))::numeric, 2) AS growth_farde_pct,
			COALESCE(c.sold, 0)                                          AS curr_sold,
			COALESCE(p.sold, 0)                                          AS prev_sold,
			ROUND((COALESCE(c.sold,0) - COALESCE(p.sold,0))::numeric, 2)   AS delta_sold,
			ROUND(((COALESCE(c.sold,0) - COALESCE(p.sold,0)) * 100.0 / NULLIF(COALESCE(p.sold,0), 0))::numeric, 2)    AS growth_sold_pct,
			COALESCE(c.revenue, 0)                                       AS curr_revenue,
			COALESCE(p.revenue, 0)                                       AS prev_revenue,
			ROUND((COALESCE(c.revenue,0) - COALESCE(p.revenue,0))::numeric, 2) AS delta_revenue,
			ROUND(((COALESCE(c.revenue,0) - COALESCE(p.revenue,0)) * 100.0 / NULLIF(COALESCE(p.revenue,0), 0))::numeric, 2) AS growth_revenue_pct,
			COALESCE(c.visits, 0)                                        AS curr_visits,
			COALESCE(p.visits, 0)                                        AS prev_visits,
			CASE
				WHEN COALESCE(c.farde,0) > COALESCE(p.farde,0) THEN 'UP'
				WHEN COALESCE(c.farde,0) < COALESCE(p.farde,0) THEN 'DOWN'
				ELSE 'STABLE'
			END AS trend
		FROM curr c
		FULL OUTER JOIN prev p ON c.brand_name = p.brand_name
		ORDER BY growth_farde_pct DESC NULLS LAST;
	`

	var results []Result
	err := db.Raw(sqlQuery, fullArgs...).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to compute growth rate", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Period-over-period sales growth", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 — BRAND COMPETITION MATRIX
// Returns a pivot-style matrix: for each geographic unit × brand pair,
// compute farde, sold, market share and rank.
// Ideal for a heatmap or grouped-bar chart on the frontend.
// ─────────────────────────────────────────────────────────────────────────────

func BrandCompetitionMatrix(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	level := c.Query("level") // "province" | "area" | "subarea" | "commune"

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	type Result struct {
		GeoName     string  `json:"geo_name"`
		GeoUUID     string  `json:"geo_uuid"`
		BrandName   string  `json:"brand_name"`
		TotalFarde  float64 `json:"total_farde"`
		TotalSold   float64 `json:"total_sold"`
		MarketShare float64 `json:"market_share"`
		BrandRank   int     `json:"brand_rank"` // rank within the geo unit
		TotalVisits int64   `json:"total_visits"`
	}

	geoSelect := "pr.name AS geo_name, pr.uuid AS geo_uuid"
	geoJoin := "INNER JOIN provinces pr ON pf.province_uuid = pr.uuid"
	geoGroup := "pr.name, pr.uuid"
	geoFilter := "pf.country_uuid = ? AND pf.province_uuid = ?"
	geoArgs := []interface{}{country_uuid, province_uuid, start_date, end_date}

	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")

	switch level {
	case "area":
		geoSelect = "a.name AS geo_name, a.uuid AS geo_uuid"
		geoJoin = "INNER JOIN areas a ON pf.area_uuid = a.uuid"
		geoGroup = "a.name, a.uuid"
		geoFilter = "pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ?"
		geoArgs = []interface{}{country_uuid, province_uuid, area_uuid, start_date, end_date}
	case "subarea":
		geoSelect = "sa.name AS geo_name, sa.uuid AS geo_uuid"
		geoJoin = "INNER JOIN sub_areas sa ON pf.sub_area_uuid = sa.uuid"
		geoGroup = "sa.name, sa.uuid"
		geoFilter = "pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ? AND pf.sub_area_uuid = ?"
		geoArgs = []interface{}{country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date}
	case "commune":
		geoSelect = "co.name AS geo_name, co.uuid AS geo_uuid"
		geoJoin = "INNER JOIN communes co ON pf.commune_uuid = co.uuid"
		geoGroup = "co.name, co.uuid"
		geoFilter = "pf.country_uuid = ? AND pf.province_uuid = ? AND pf.area_uuid = ? AND pf.sub_area_uuid = ? AND pf.commune_uuid = ?"
		geoArgs = []interface{}{country_uuid, province_uuid, area_uuid, sub_area_uuid, commune_uuid, start_date, end_date}
	}

	sqlQuery := `
		WITH base AS (
			SELECT
				` + geoSelect + `,
				b.name                                    AS brand_name,
				COUNT(DISTINCT pf.uuid)                   AS total_visits,
				ROUND(SUM(pfi.number_farde)::numeric, 2)  AS total_farde,
				ROUND(SUM(pfi.sold)::numeric, 2)           AS total_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN brands b     ON pfi.brand_uuid = b.uuid
			` + geoJoin + `
			WHERE ` + geoFilter + `
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY ` + geoGroup + `, b.name
		),
		totals AS (
			SELECT geo_uuid, SUM(total_farde) AS geo_total_farde FROM base GROUP BY geo_uuid
		)
		SELECT
			b.geo_name,
			b.geo_uuid,
			b.brand_name,
			b.total_farde,
			b.total_sold,
			ROUND((b.total_farde * 100.0 / NULLIF(t.geo_total_farde, 0))::numeric, 2) AS market_share,
			RANK() OVER (PARTITION BY b.geo_uuid ORDER BY b.total_farde DESC)::int      AS brand_rank,
			b.total_visits
		FROM base b
		INNER JOIN totals t ON b.geo_uuid = t.geo_uuid
		ORDER BY b.geo_name, brand_rank;
	`

	var results []Result
	err := db.Raw(sqlQuery, geoArgs...).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch brand competition matrix", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Brand competition matrix", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 — TOP 10 POS RANKING
// Ranks Points of Sale by total farde sold, sold quantity and revenue.
// Highlights your best-performing outlets for priority visit planning.
// ─────────────────────────────────────────────────────────────────────────────

func TopPOSRanking(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	limit := c.Query("limit") // default 10

	if start_date == "" || end_date == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date are required",
		})
	}

	if limit == "" {
		limit = "10"
	}

	type Result struct {
		Rank         int     `json:"rank"`
		PosName      string  `json:"pos_name"`
		PosUUID      string  `json:"pos_uuid"`
		Shop         string  `json:"shop"`
		Postype      string  `json:"postype"`
		CommuneName  string  `json:"commune_name"`
		AreaName     string  `json:"area_name"`
		TotalVisits  int64   `json:"total_visits"`
		TotalFarde   float64 `json:"total_farde"`
		TotalSold    float64 `json:"total_sold"`
		TotalRevenue float64 `json:"total_revenue"`
		AvgPrice     float64 `json:"avg_price"`
		FardeShare   float64 `json:"farde_share"`
	}

	geoFilter := "pf.country_uuid = ?"
	geoVals := []interface{}{country_uuid}

	if province_uuid != "" {
		geoFilter += " AND pf.province_uuid = ?"
		geoVals = append(geoVals, province_uuid)
	}
	if area_uuid != "" {
		geoFilter += " AND pf.area_uuid = ?"
		geoVals = append(geoVals, area_uuid)
	}
	if sub_area_uuid != "" {
		geoFilter += " AND pf.sub_area_uuid = ?"
		geoVals = append(geoVals, sub_area_uuid)
	}
	if commune_uuid != "" {
		geoFilter += " AND pf.commune_uuid = ?"
		geoVals = append(geoVals, commune_uuid)
	}
	// Each WHERE clause in the SQL uses geoFilter + BETWEEN ? AND ?
	// The query has two such clauses (global_farde CTE + main WHERE), so repeat args twice.
	args := append(append(append([]interface{}{}, geoVals...), start_date, end_date), append(geoVals, start_date, end_date)...)

	sqlQuery := `
		WITH global_farde AS (
			SELECT COALESCE(SUM(pfi.number_farde), 0) AS g_farde
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE ` + geoFilter + `
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		)
		SELECT
			RANK() OVER (ORDER BY SUM(pfi.number_farde) DESC)::int AS rank,
			p.name                                                    AS pos_name,
			p.uuid                                                    AS pos_uuid,
			p.shop,
			COALESCE(NULLIF(p.postype,''), 'Non défini')              AS postype,
			co.name                                                   AS commune_name,
			a.name                                                    AS area_name,
			COUNT(DISTINCT pf.uuid)                                   AS total_visits,
			ROUND(SUM(pfi.number_farde)::numeric, 2)                  AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                          AS total_sold,
			ROUND(SUM(pf.price)::numeric, 2)                          AS total_revenue,
			ROUND(AVG(pf.price)::numeric, 2)                          AS avg_price,
			ROUND((SUM(pfi.number_farde) * 100.0 / NULLIF((SELECT g_farde FROM global_farde), 0))::numeric, 2) AS farde_share
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf  ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN pos p         ON pf.pos_uuid = p.uuid
		LEFT  JOIN communes co   ON pf.commune_uuid = co.uuid
		LEFT  JOIN areas a       ON pf.area_uuid = a.uuid
		WHERE ` + geoFilter + `
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY p.name, p.uuid, p.shop, p.postype, co.name, a.name
		ORDER BY total_farde DESC
		LIMIT ` + limit + `;
	`

	var results []Result
	err := db.Raw(sqlQuery, args...).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch top POS ranking", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Top POS ranking", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 7 — SALES REPRESENTATIVE SCORECARD
// Per-agent summary: visits, farde, sold, revenue, avg price, coverage rate
// and a performance score to quickly identify top vs under-performing reps.
// ─────────────────────────────────────────────────────────────────────────────

func SalesRepScorecard(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	title_filter := c.Query("title") // ASM | Supervisor | DR | Cyclo
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	type Result struct {
		AgentName          string  `json:"agent_name"`
		AgentUUID          string  `json:"agent_uuid"`
		Title              string  `json:"title"`
		TotalVisits        int64   `json:"total_visits"`
		UniquePos          int64   `json:"unique_pos"`
		TotalFarde         float64 `json:"total_farde"`
		TotalSold          float64 `json:"total_sold"`
		TotalRevenue       float64 `json:"total_revenue"`
		AvgFardePerPos     float64 `json:"avg_farde_per_pos"`
		AvgRevenuePerVisit float64 `json:"avg_revenue_per_visit"`
		BrandsCovered      int64   `json:"brands_covered"`
		VisitTarget        int64   `json:"visit_target"`
		AchievementPct     float64 `json:"achievement_pct"`
		PerfScore          float64 `json:"perf_score"` // composite: 50% visits + 30% farde + 20% sold
	}

	geoFilter := "pf.country_uuid = ?"
	args := []interface{}{country_uuid, start_date, end_date, end_date, start_date, end_date, start_date, country_uuid, start_date, end_date}

	if province_uuid != "" {
		geoFilter += " AND pf.province_uuid = ?"
	}
	if area_uuid != "" {
		geoFilter += " AND pf.area_uuid = ?"
	}
	if sub_area_uuid != "" {
		geoFilter += " AND pf.sub_area_uuid = ?"
	}
	if commune_uuid != "" {
		geoFilter += " AND pf.commune_uuid = ?"
	}
	if title_filter != "" {
		geoFilter += " AND u.title = ?"
	}

	// Rebuild args with all geo params in correct order
	geoVals := []interface{}{country_uuid}
	if province_uuid != "" {
		geoVals = append(geoVals, province_uuid)
	}
	if area_uuid != "" {
		geoVals = append(geoVals, area_uuid)
	}
	if sub_area_uuid != "" {
		geoVals = append(geoVals, sub_area_uuid)
	}
	if commune_uuid != "" {
		geoVals = append(geoVals, commune_uuid)
	}
	if title_filter != "" {
		geoVals = append(geoVals, title_filter)
	}

	dateArgs := []interface{}{start_date, end_date}

	// Compose final args: geoVals + dateArgs + dateArgs (for target) + geoVals + dateArgs
	args = append(geoVals, dateArgs...)
	args = append(args, end_date, start_date) // for target calc
	args = append(args, geoVals...)
	args = append(args, dateArgs...)

	sqlQuery := `
		WITH agent_stats AS (
			SELECT
				u.fullname                                        AS agent_name,
				u.uuid                                            AS agent_uuid,
				u.title,
				COUNT(DISTINCT pf.uuid)                           AS total_visits,
				COUNT(DISTINCT pf.pos_uuid)                       AS unique_pos,
				ROUND(SUM(pfi.number_farde)::numeric, 2)          AS total_farde,
				ROUND(SUM(pfi.sold)::numeric, 2)                   AS total_sold,
				ROUND(SUM(pf.price)::numeric, 2)                   AS total_revenue,
				COUNT(DISTINCT pfi.brand_uuid)                    AS brands_covered,
				(CASE
					WHEN u.title = 'ASM'        THEN 10 * ((?::date - ?::date) + 1)
					WHEN u.title = 'Supervisor' THEN 20 * ((?::date - ?::date) + 1)
					WHEN u.title = 'DR'         THEN 40 * ((?::date - ?::date) + 1)
					WHEN u.title = 'Cyclo'      THEN 40 * ((?::date - ?::date) + 1)
					ELSE 1
				END) AS visit_target
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN users u      ON pf.user_uuid = u.uuid
			WHERE ` + geoFilter + `
			  AND pf.created_at BETWEEN ? AND ?
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY u.fullname, u.uuid, u.title
		),
		max_stats AS (
			SELECT
				MAX(total_visits) AS max_visits,
				MAX(total_farde)  AS max_farde,
				MAX(total_sold)   AS max_sold
			FROM agent_stats
		)
		SELECT
			a.agent_name,
			a.agent_uuid,
			a.title,
			a.total_visits,
			a.unique_pos,
			a.total_farde,
			a.total_sold,
			a.total_revenue,
			ROUND((a.total_farde / NULLIF(a.unique_pos, 0))::numeric, 2)        AS avg_farde_per_pos,
			ROUND((a.total_revenue / NULLIF(a.total_visits, 0))::numeric, 2)    AS avg_revenue_per_visit,
			a.brands_covered,
			a.visit_target,
			ROUND((a.total_visits * 100.0 / NULLIF(a.visit_target, 0))::numeric, 2) AS achievement_pct,
			ROUND((
				0.50 * (a.total_visits  * 100.0 / NULLIF(m.max_visits, 0)) +
				0.30 * (a.total_farde   * 100.0 / NULLIF(m.max_farde, 0))  +
				0.20 * (a.total_sold    * 100.0 / NULLIF(m.max_sold, 0))
			)::numeric, 2) AS perf_score
		FROM agent_stats a
		CROSS JOIN max_stats m
		ORDER BY perf_score DESC;
	`

	geoValsForQuery := []interface{}{country_uuid}
	if province_uuid != "" {
		geoValsForQuery = append(geoValsForQuery, province_uuid)
	}
	if area_uuid != "" {
		geoValsForQuery = append(geoValsForQuery, area_uuid)
	}
	if sub_area_uuid != "" {
		geoValsForQuery = append(geoValsForQuery, sub_area_uuid)
	}
	if commune_uuid != "" {
		geoValsForQuery = append(geoValsForQuery, commune_uuid)
	}
	if title_filter != "" {
		geoValsForQuery = append(geoValsForQuery, title_filter)
	}

	finalArgs := []interface{}{end_date, start_date, end_date, start_date, end_date, start_date, end_date, start_date}
	finalArgs = append(finalArgs, geoValsForQuery...)
	finalArgs = append(finalArgs, start_date, end_date)

	var results []Result
	err := db.Raw(sqlQuery, finalArgs...).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch rep scorecard", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Sales representative scorecard", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 — REVENUE HEATMAP BY DAY OF WEEK
// Returns (day_of_week, brand) with aggregated farde and sold.
// You can render this as a calendar-heatmap or bar chart to reveal
// which days of the week drive the most sales activity.
// ─────────────────────────────────────────────────────────────────────────────

func SalesHeatmapByDayOfWeek(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	type Result struct {
		DayOfWeek   int     `json:"day_of_week"` // 1=Monday … 7=Sunday (ISO)
		DayName     string  `json:"day_name"`    // "Monday" …
		BrandName   string  `json:"brand_name"`
		TotalFarde  float64 `json:"total_farde"`
		TotalSold   float64 `json:"total_sold"`
		TotalVisits int64   `json:"total_visits"`
		AvgFarde    float64 `json:"avg_farde"`
	}

	geoFilter := "pf.country_uuid = ?"
	geoVals := []interface{}{country_uuid}

	if province_uuid != "" {
		geoFilter += " AND pf.province_uuid = ?"
		geoVals = append(geoVals, province_uuid)
	}
	if area_uuid != "" {
		geoFilter += " AND pf.area_uuid = ?"
		geoVals = append(geoVals, area_uuid)
	}
	if sub_area_uuid != "" {
		geoFilter += " AND pf.sub_area_uuid = ?"
		geoVals = append(geoVals, sub_area_uuid)
	}
	if commune_uuid != "" {
		geoFilter += " AND pf.commune_uuid = ?"
		geoVals = append(geoVals, commune_uuid)
	}

	args := append(geoVals, start_date, end_date)

	sqlQuery := `
		SELECT
			EXTRACT(ISODOW FROM pf.created_at)::int                 AS day_of_week,
			TO_CHAR(pf.created_at, 'Day')                           AS day_name,
			b.name                                                    AS brand_name,
			ROUND(SUM(pfi.number_farde)::numeric, 2)                 AS total_farde,
			ROUND(SUM(pfi.sold)::numeric, 2)                          AS total_sold,
			COUNT(DISTINCT pf.uuid)                                   AS total_visits,
			ROUND(AVG(pfi.number_farde)::numeric, 2)                  AS avg_farde
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN brands b     ON pfi.brand_uuid = b.uuid
		WHERE ` + geoFilter + `
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		GROUP BY day_of_week, day_name, b.name
		ORDER BY day_of_week, total_farde DESC;
	`

	var results []Result
	err := db.Raw(sqlQuery, args...).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch day-of-week heatmap", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "Sales heatmap by day of week", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 — SALES SUMMARY KPI CARD
// Single-request KPI card for the top of the dashboard:
// total farde, sold, revenue, visits, active POS, brands, avg price,
// plus deltas vs previous equivalent period.
// ─────────────────────────────────────────────────────────────────────────────

func SalesSummaryKPI(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	type PeriodKPI struct {
		TotalFarde   float64 `json:"total_farde"`
		TotalSold    float64 `json:"total_sold"`
		TotalRevenue float64 `json:"total_revenue"`
		TotalVisits  int64   `json:"total_visits"`
		ActivePos    int64   `json:"active_pos"`
		ActiveBrands int64   `json:"active_brands"`
		AvgPrice     float64 `json:"avg_price"`
		ActiveAgents int64   `json:"active_agents"`
	}
	type KPISummary struct {
		Current       PeriodKPI `json:"current"`
		FardeGrowth   float64   `json:"farde_growth_pct"`
		SoldGrowth    float64   `json:"sold_growth_pct"`
		RevenueGrowth float64   `json:"revenue_growth_pct"`
		VisitsGrowth  float64   `json:"visits_growth_pct"`
	}

	geoFilter := "pf.country_uuid = ?"
	geoVals := []interface{}{country_uuid}
	if province_uuid != "" {
		geoFilter += " AND pf.province_uuid = ?"
		geoVals = append(geoVals, province_uuid)
	}
	if area_uuid != "" {
		geoFilter += " AND pf.area_uuid = ?"
		geoVals = append(geoVals, area_uuid)
	}
	if sub_area_uuid != "" {
		geoFilter += " AND pf.sub_area_uuid = ?"
		geoVals = append(geoVals, sub_area_uuid)
	}
	if commune_uuid != "" {
		geoFilter += " AND pf.commune_uuid = ?"
		geoVals = append(geoVals, commune_uuid)
	}

	sqlQuery := `
		SELECT
			ROUND(COALESCE(SUM(pfi.number_farde), 0)::numeric, 2)  AS total_farde,
			ROUND(COALESCE(SUM(pfi.sold), 0)::numeric, 2)           AS total_sold,
			ROUND(COALESCE(SUM(pf.price), 0)::numeric, 2)           AS total_revenue,
			COUNT(DISTINCT pf.uuid)                                  AS total_visits,
			COUNT(DISTINCT pf.pos_uuid)                              AS active_pos,
			COUNT(DISTINCT pfi.brand_uuid)                           AS active_brands,
			ROUND(COALESCE(AVG(pf.price), 0)::numeric, 2)           AS avg_price,
			COUNT(DISTINCT pf.user_uuid)                             AS active_agents
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
		WHERE ` + geoFilter + `
		  AND pf.created_at BETWEEN ? AND ?
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL;
	`

	currArgs := append(append([]interface{}{}, geoVals...), start_date, end_date)

	var current PeriodKPI
	err := db.Raw(sqlQuery, currArgs...).Scan(&current).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch KPI summary", "error": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Sales summary KPI",
		"data": KPISummary{
			Current:       current,
			FardeGrowth:   0, // requires prev period params — extend as needed
			SoldGrowth:    0,
			RevenueGrowth: 0,
			VisitsGrowth:  0,
		},
	})
}

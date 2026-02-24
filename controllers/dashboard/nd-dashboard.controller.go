package dashboard

import (
	"math"
	"time"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════════╗
// ║           NUMERIC DISTRIBUTION (ND) DASHBOARD — HIGH-LEVEL ANALYTICS       ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  Numeric Distribution = (POS where brand counter > 0)                       ║
// ║                        ──────────────────────────────  × 100               ║
// ║                            Total distinct POS visited                        ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  SECTION 1 — TABLE VIEWS    : Province / Area / SubArea / Commune           ║
// ║  SECTION 2 — BAR CHARTS     : Province / Area / SubArea / Commune           ║
// ║  SECTION 3 — TREND CHART    : ND% by Brand per Month                        ║
// ║  SECTION 4 — POWER ANALYTICS: Summary KPI / Brand Ranking / Gap Analysis    ║
// ║  SECTION 5 — ADVANCED       : Brand×Territory Heatmap / Period Evolution     ║
// ╚══════════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 — TABLE VIEWS
// Each row = (territory × brand) with:
//   nd_pos       — distinct POS where the brand counter > 0
//   total_pos    — total distinct POS visited (any brand)
//   nd_percent   — nd_pos / total_pos × 100
//   universe_pos — total registered POS in the territory
//   reach_rate   — total_pos / universe_pos × 100
// ─────────────────────────────────────────────────────────────────────────────

// NDTableViewProvince — ND breakdown per brand at Province level
func NDTableViewProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH visited AS (
			SELECT
				pf.province_uuid,
				COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY pf.province_uuid
		),
		universe AS (
			SELECT p.province_uuid, COUNT(p.uuid) AS universe_pos
			FROM pos p
			WHERE p.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR p.province_uuid = @province_uuid)
			  AND p.deleted_at IS NULL
			GROUP BY p.province_uuid
		),
		nd_counts AS (
			SELECT
				pf.province_uuid,
				pfi.brand_uuid,
				COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pf.province_uuid, pfi.brand_uuid
		)
		SELECT
			pr.name                                                          AS territory_name,
			pr.uuid                                                          AS territory_uuid,
			'province'                                                       AS territory_level,
			b.name                                                           AS brand_name,
			b.uuid                                                           AS brand_uuid,
			COALESCE(nd.nd_pos, 0)                                           AS nd_pos,
			COALESCE(v.total_pos, 0)                                         AS total_pos,
			COALESCE(u.universe_pos, 0)                                      AS universe_pos,
			ROUND((COALESCE(nd.nd_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)        AS nd_percent,
			ROUND((COALESCE(v.total_pos, 0) * 100.0 /
			       NULLIF(COALESCE(u.universe_pos, 0), 0))::numeric, 2)     AS reach_rate
		FROM nd_counts nd
		INNER JOIN brands b   ON b.uuid  = nd.brand_uuid
		INNER JOIN provinces pr ON pr.uuid = nd.province_uuid
		LEFT  JOIN visited v  ON v.province_uuid  = nd.province_uuid
		LEFT  JOIN universe u ON u.province_uuid  = nd.province_uuid
		ORDER BY pr.name, nd_percent DESC
	`

	type NDRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		UniversePos    int64   `json:"universe_pos"`
		NdPercent      float64 `json:"nd_percent"`
		ReachRate      float64 `json:"reach_rate"`
	}

	var results []NDRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND province data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "ND Province Table", "data": results})
}

// NDTableViewArea — ND breakdown per brand at Area level
func NDTableViewArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH visited AS (
			SELECT pf.area_uuid, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY pf.area_uuid
		),
		universe AS (
			SELECT p.area_uuid, COUNT(p.uuid) AS universe_pos
			FROM pos p
			WHERE p.country_uuid = @country_uuid
			  AND (@area_uuid = '' OR p.area_uuid = @area_uuid)
			  AND p.deleted_at IS NULL
			GROUP BY p.area_uuid
		),
		nd_counts AS (
			SELECT pf.area_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pf.area_uuid, pfi.brand_uuid
		)
		SELECT
			a.name                                                            AS territory_name,
			a.uuid                                                            AS territory_uuid,
			'area'                                                            AS territory_level,
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			COALESCE(nd.nd_pos, 0)                                            AS nd_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			COALESCE(u.universe_pos, 0)                                       AS universe_pos,
			ROUND((COALESCE(nd.nd_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS nd_percent,
			ROUND((COALESCE(v.total_pos, 0) * 100.0 /
			       NULLIF(COALESCE(u.universe_pos, 0), 0))::numeric, 2)      AS reach_rate
		FROM nd_counts nd
		INNER JOIN brands b ON b.uuid = nd.brand_uuid
		INNER JOIN areas  a ON a.uuid = nd.area_uuid
		LEFT  JOIN visited v  ON v.area_uuid = nd.area_uuid
		LEFT  JOIN universe u ON u.area_uuid = nd.area_uuid
		ORDER BY a.name, nd_percent DESC
	`

	type NDRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		UniversePos    int64   `json:"universe_pos"`
		NdPercent      float64 `json:"nd_percent"`
		ReachRate      float64 `json:"reach_rate"`
	}

	var results []NDRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND area data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "ND Area Table", "data": results})
}

// NDTableViewSubArea — ND breakdown per brand at SubArea level
func NDTableViewSubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH visited AS (
			SELECT pf.sub_area_uuid, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY pf.sub_area_uuid
		),
		universe AS (
			SELECT p.sub_area_uuid, COUNT(p.uuid) AS universe_pos
			FROM pos p
			WHERE p.country_uuid = @country_uuid
			  AND (@sub_area_uuid = '' OR p.sub_area_uuid = @sub_area_uuid)
			  AND p.deleted_at IS NULL
			GROUP BY p.sub_area_uuid
		),
		nd_counts AS (
			SELECT pf.sub_area_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pf.sub_area_uuid, pfi.brand_uuid
		)
		SELECT
			sa.name                                                           AS territory_name,
			sa.uuid                                                           AS territory_uuid,
			'subarea'                                                         AS territory_level,
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			COALESCE(nd.nd_pos, 0)                                            AS nd_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			COALESCE(u.universe_pos, 0)                                       AS universe_pos,
			ROUND((COALESCE(nd.nd_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS nd_percent,
			ROUND((COALESCE(v.total_pos, 0) * 100.0 /
			       NULLIF(COALESCE(u.universe_pos, 0), 0))::numeric, 2)      AS reach_rate
		FROM nd_counts nd
		INNER JOIN brands   b  ON b.uuid  = nd.brand_uuid
		INNER JOIN sub_areas sa ON sa.uuid = nd.sub_area_uuid
		LEFT  JOIN visited v  ON v.sub_area_uuid = nd.sub_area_uuid
		LEFT  JOIN universe u ON u.sub_area_uuid = nd.sub_area_uuid
		ORDER BY sa.name, nd_percent DESC
	`

	type NDRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		UniversePos    int64   `json:"universe_pos"`
		NdPercent      float64 `json:"nd_percent"`
		ReachRate      float64 `json:"reach_rate"`
	}

	var results []NDRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND subarea data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "ND SubArea Table", "data": results})
}

// NDTableViewCommune — ND breakdown per brand at Commune level
func NDTableViewCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH visited AS (
			SELECT pf.commune_uuid, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY pf.commune_uuid
		),
		universe AS (
			SELECT p.commune_uuid, COUNT(p.uuid) AS universe_pos
			FROM pos p
			WHERE p.country_uuid = @country_uuid
			  AND (@commune_uuid = '' OR p.commune_uuid = @commune_uuid)
			  AND p.deleted_at IS NULL
			GROUP BY p.commune_uuid
		),
		nd_counts AS (
			SELECT pf.commune_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pf.commune_uuid, pfi.brand_uuid
		)
		SELECT
			cm.name                                                           AS territory_name,
			cm.uuid                                                           AS territory_uuid,
			'commune'                                                         AS territory_level,
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			COALESCE(nd.nd_pos, 0)                                            AS nd_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			COALESCE(u.universe_pos, 0)                                       AS universe_pos,
			ROUND((COALESCE(nd.nd_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS nd_percent,
			ROUND((COALESCE(v.total_pos, 0) * 100.0 /
			       NULLIF(COALESCE(u.universe_pos, 0), 0))::numeric, 2)      AS reach_rate
		FROM nd_counts nd
		INNER JOIN brands   b  ON b.uuid  = nd.brand_uuid
		INNER JOIN communes cm ON cm.uuid = nd.commune_uuid
		LEFT  JOIN visited v  ON v.commune_uuid = nd.commune_uuid
		LEFT  JOIN universe u ON u.commune_uuid = nd.commune_uuid
		ORDER BY cm.name, nd_percent DESC
	`

	type NDRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		UniversePos    int64   `json:"universe_pos"`
		NdPercent      float64 `json:"nd_percent"`
		ReachRate      float64 `json:"reach_rate"`
	}

	var results []NDRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND commune data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "ND Commune Table", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 — BAR CHARTS
// Returns data grouped per territory, each territory contains an array of
// brands with nd_pos, total_pos and nd_percent. Ready for grouped bar charts.
// ─────────────────────────────────────────────────────────────────────────────

type ndBrandItem struct {
	BrandName string  `json:"brand_name"`
	BrandUUID string  `json:"brand_uuid"`
	NdPos     int64   `json:"nd_pos"`
	TotalPos  int64   `json:"total_pos"`
	NdPercent float64 `json:"nd_percent"`
}

type ndBarGroup struct {
	TerritoryName  string        `json:"territory_name"`
	TerritoryUUID  string        `json:"territory_uuid"`
	TerritoryLevel string        `json:"territory_level"`
	TotalPos       int64         `json:"total_pos"`
	UniversePos    int64         `json:"universe_pos"`
	ReachRate      float64       `json:"reach_rate"`
	Brands         []ndBrandItem `json:"brands"`
}

// ndBarChartBuilder — shared logic for all bar-chart endpoints.
func ndBarChartBuilder(c *fiber.Ctx, geoCol, joinTable, level string) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	type rawRow struct {
		TerritoryName string  `json:"territory_name"`
		TerritoryUUID string  `json:"territory_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		NdPos         int64   `json:"nd_pos"`
		TotalPos      int64   `json:"total_pos"`
		UniversePos   int64   `json:"universe_pos"`
		NdPercent     float64 `json:"nd_percent"`
		ReachRate     float64 `json:"reach_rate"`
	}

	sqlQuery := `
		WITH visited AS (
			SELECT pf.` + geoCol + `, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY pf.` + geoCol + `
		),
		universe AS (
			SELECT p.` + geoCol + `, COUNT(p.uuid) AS universe_pos
			FROM pos p
			WHERE p.country_uuid = @country_uuid AND p.deleted_at IS NULL
			GROUP BY p.` + geoCol + `
		),
		nd_counts AS (
			SELECT pf.` + geoCol + `, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pf.` + geoCol + `, pfi.brand_uuid
		)
		SELECT
			t.name                                                            AS territory_name,
			t.uuid                                                            AS territory_uuid,
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			COALESCE(nd.nd_pos, 0)                                            AS nd_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			COALESCE(u.universe_pos, 0)                                       AS universe_pos,
			ROUND((COALESCE(nd.nd_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS nd_percent,
			ROUND((COALESCE(v.total_pos, 0) * 100.0 /
			       NULLIF(COALESCE(u.universe_pos, 0), 0))::numeric, 2)      AS reach_rate
		FROM nd_counts nd
		INNER JOIN brands       b ON b.uuid = nd.brand_uuid
		INNER JOIN ` + joinTable + ` t ON t.uuid = nd.` + geoCol + `
		LEFT  JOIN visited v  ON v.` + geoCol + ` = nd.` + geoCol + `
		LEFT  JOIN universe u ON u.` + geoCol + ` = nd.` + geoCol + `
		ORDER BY t.name, nd_percent DESC
	`

	var rawResults []rawRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&rawResults).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND bar chart data", "error": err.Error(),
		})
	}

	// Aggregate into grouped structure
	groupMap := make(map[string]*ndBarGroup)
	order := []string{}

	for _, r := range rawResults {
		if _, exists := groupMap[r.TerritoryUUID]; !exists {
			reachRate := math.Round(float64(r.TotalPos)*100.0/math.Max(float64(r.UniversePos), 1)*100) / 100
			groupMap[r.TerritoryUUID] = &ndBarGroup{
				TerritoryName:  r.TerritoryName,
				TerritoryUUID:  r.TerritoryUUID,
				TerritoryLevel: level,
				TotalPos:       r.TotalPos,
				UniversePos:    r.UniversePos,
				ReachRate:      reachRate,
				Brands:         []ndBrandItem{},
			}
			order = append(order, r.TerritoryUUID)
		}
		groupMap[r.TerritoryUUID].Brands = append(groupMap[r.TerritoryUUID].Brands, ndBrandItem{
			BrandName: r.BrandName,
			BrandUUID: r.BrandUUID,
			NdPos:     r.NdPos,
			TotalPos:  r.TotalPos,
			NdPercent: r.NdPercent,
		})
	}

	grouped := make([]ndBarGroup, 0, len(order))
	for _, uuid := range order {
		grouped = append(grouped, *groupMap[uuid])
	}

	return c.JSON(fiber.Map{
		"status": "success", "message": "ND Bar Chart — " + level, "data": grouped,
	})
}

func NDBarChartProvince(c *fiber.Ctx) error {
	return ndBarChartBuilder(c, "province_uuid", "provinces", "province")
}
func NDBarChartArea(c *fiber.Ctx) error { return ndBarChartBuilder(c, "area_uuid", "areas", "area") }
func NDBarChartSubArea(c *fiber.Ctx) error {
	return ndBarChartBuilder(c, "sub_area_uuid", "sub_areas", "subarea")
}
func NDBarChartCommune(c *fiber.Ctx) error {
	return ndBarChartBuilder(c, "commune_uuid", "communes", "commune")
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 — MONTHLY TREND LINE CHART
// Returns ND% per brand per calendar month. Great for seasonal analysis.
// ─────────────────────────────────────────────────────────────────────────────

// NDLineChartByMonth — ND% per brand per calendar month
func NDLineChartByMonth(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	type MonthRow struct {
		BrandName string  `json:"brand_name"`
		BrandUUID string  `json:"brand_uuid"`
		Month     string  `json:"month"`
		NdPos     int64   `json:"nd_pos"`
		TotalPos  int64   `json:"total_pos"`
		NdPercent float64 `json:"nd_percent"`
	}

	sqlQuery := `
		WITH monthly_visited AS (
			SELECT
				TO_CHAR(pf.created_at, 'YYYY-MM') AS month,
				COUNT(DISTINCT pf.pos_uuid)        AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY month
		),
		nd_monthly AS (
			SELECT
				TO_CHAR(pf.created_at, 'YYYY-MM') AS month,
				pfi.brand_uuid,
				COUNT(DISTINCT pf.pos_uuid)        AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY month, pfi.brand_uuid
		)
		SELECT
			b.name                                                              AS brand_name,
			b.uuid                                                              AS brand_uuid,
			nm.month,
			nm.nd_pos,
			COALESCE(mv.total_pos, 0)                                           AS total_pos,
			ROUND((nm.nd_pos * 100.0 /
			       NULLIF(COALESCE(mv.total_pos, 0), 0))::numeric, 2)          AS nd_percent
		FROM nd_monthly nm
		INNER JOIN brands b ON b.uuid = nm.brand_uuid
		LEFT  JOIN monthly_visited mv ON mv.month = nm.month
		ORDER BY b.name, nm.month
	`

	var rawRows []MonthRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&rawRows).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND monthly trend", "error": err.Error(),
		})
	}

	// Group per brand → series of monthly points
	type BrandSeries struct {
		BrandName string     `json:"brand_name"`
		BrandUUID string     `json:"brand_uuid"`
		Points    []MonthRow `json:"points"`
	}
	brandMap := make(map[string]*BrandSeries)
	order := []string{}
	for _, r := range rawRows {
		if _, ok := brandMap[r.BrandUUID]; !ok {
			brandMap[r.BrandUUID] = &BrandSeries{BrandName: r.BrandName, BrandUUID: r.BrandUUID, Points: []MonthRow{}}
			order = append(order, r.BrandUUID)
		}
		brandMap[r.BrandUUID].Points = append(brandMap[r.BrandUUID].Points, r)
	}
	series := make([]BrandSeries, 0, len(order))
	for _, uuid := range order {
		series = append(series, *brandMap[uuid])
	}

	return c.JSON(fiber.Map{"status": "success", "message": "ND Monthly Trend by Brand", "data": series})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 — POWER ANALYTICS
// ─────────────────────────────────────────────────────────────────────────────

// NDSummaryKPI — Executive KPI card for the ND dashboard.
//
//	total_universe_pos  — total registered POS
//	total_visited_pos   — distinct POS visited in period
//	total_nd_pos        — POS where at least one brand counter > 0
//	avg_nd_percent      — average ND% across all brands
//	total_brands        — number of distinct brands measured
//	reach_rate          — visited / universe × 100
//	coverage_index      — nd_pos / universe × 100
func NDSummaryKPI(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	type KPI struct {
		TotalUniversePos int64   `json:"total_universe_pos"`
		TotalVisitedPos  int64   `json:"total_visited_pos"`
		TotalNdPos       int64   `json:"total_nd_pos"`
		AvgNdPercent     float64 `json:"avg_nd_percent"`
		TotalBrands      int64   `json:"total_brands"`
		ReachRate        float64 `json:"reach_rate"`
		CoverageIndex    float64 `json:"coverage_index"`
	}

	sqlQuery := `
		WITH universe AS (
			SELECT COUNT(p.uuid) AS cnt
			FROM pos p
			WHERE p.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR p.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR p.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR p.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR p.commune_uuid  = @commune_uuid)
			  AND p.deleted_at IS NULL
		),
		visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS cnt
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		),
		nd_pos AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS cnt
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
		),
		brand_nd AS (
			SELECT
				pfi.brand_uuid,
				ROUND((COUNT(DISTINCT pf.pos_uuid) * 100.0 /
				       NULLIF((SELECT cnt FROM visited), 0))::numeric, 2) AS nd_pct
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			(SELECT cnt FROM universe)                                         AS total_universe_pos,
			(SELECT cnt FROM visited)                                          AS total_visited_pos,
			(SELECT cnt FROM nd_pos)                                           AS total_nd_pos,
			ROUND(COALESCE((SELECT AVG(nd_pct) FROM brand_nd), 0)::numeric, 2) AS avg_nd_percent,
			(SELECT COUNT(*) FROM brand_nd)                                    AS total_brands,
			ROUND(((SELECT cnt FROM visited) * 100.0 /
			       NULLIF((SELECT cnt FROM universe), 0))::numeric, 2)        AS reach_rate,
			ROUND(((SELECT cnt FROM nd_pos) * 100.0 /
			       NULLIF((SELECT cnt FROM universe), 0))::numeric, 2)        AS coverage_index
	`

	var kpi KPI
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&kpi).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND summary KPI", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "ND Summary KPI", "data": kpi})
}

// NDBrandRanking — Brands ranked by ND% descending.
// Also returns total_farde and avg_counter for deeper sales insight.
func NDBrandRanking(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	type RankRow struct {
		Rank       int     `json:"rank"`
		BrandName  string  `json:"brand_name"`
		BrandUUID  string  `json:"brand_uuid"`
		NdPos      int64   `json:"nd_pos"`
		TotalPos   int64   `json:"total_pos"`
		NdPercent  float64 `json:"nd_percent"`
		TotalFarde float64 `json:"total_farde"`
		AvgCounter float64 `json:"avg_counter"`
	}

	sqlQuery := `
		WITH visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		),
		brand_stats AS (
			SELECT
				pfi.brand_uuid,
				COUNT(DISTINCT pf.pos_uuid)             AS nd_pos,
				ROUND(SUM(pfi.number_farde)::numeric, 2) AS total_farde,
				ROUND(AVG(pfi.counter)::numeric, 2)      AS avg_counter
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			ROW_NUMBER() OVER (ORDER BY nd_pos DESC)                           AS rank,
			b.name                                                             AS brand_name,
			b.uuid                                                             AS brand_uuid,
			bs.nd_pos,
			(SELECT total_pos FROM visited)                                    AS total_pos,
			ROUND((bs.nd_pos * 100.0 /
			       NULLIF((SELECT total_pos FROM visited), 0))::numeric, 2)   AS nd_percent,
			bs.total_farde,
			bs.avg_counter
		FROM brand_stats bs
		INNER JOIN brands b ON b.uuid = bs.brand_uuid
		ORDER BY nd_pos DESC
	`

	var results []RankRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND brand ranking", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "ND Brand Ranking", "data": results})
}

// NDGapAnalysis — 3-zone opportunity funnel per brand:
//
//	Zone A (ND Zone)       — POS where brand counter > 0
//	Zone B (Visited Gap)   — POS visited but brand NOT present
//	Zone C (Universe Gap)  — Registered POS never visited
//
// opportunity_pct = (B + C) / universe × 100
func NDGapAnalysis(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	type GapRow struct {
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		NdPos          int64   `json:"nd_pos"`
		VisitedGapPos  int64   `json:"visited_gap_pos"`
		UniverseGapPos int64   `json:"universe_gap_pos"`
		TotalVisited   int64   `json:"total_visited"`
		TotalUniverse  int64   `json:"total_universe"`
		NdPercent      float64 `json:"nd_percent"`
		ReachRate      float64 `json:"reach_rate"`
		OpportunityPct float64 `json:"opportunity_pct"`
	}

	sqlQuery := `
		WITH universe AS (
			SELECT COUNT(p.uuid) AS cnt
			FROM pos p
			WHERE p.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR p.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR p.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR p.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR p.commune_uuid  = @commune_uuid)
			  AND p.deleted_at IS NULL
		),
		visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS cnt
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		),
		nd_per_brand AS (
			SELECT pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.name                                                                AS brand_name,
			b.uuid                                                                AS brand_uuid,
			nb.nd_pos,
			GREATEST((SELECT cnt FROM visited) - nb.nd_pos, 0)                   AS visited_gap_pos,
			GREATEST((SELECT cnt FROM universe) - (SELECT cnt FROM visited), 0)  AS universe_gap_pos,
			(SELECT cnt FROM visited)                                             AS total_visited,
			(SELECT cnt FROM universe)                                            AS total_universe,
			ROUND((nb.nd_pos * 100.0 /
			       NULLIF((SELECT cnt FROM visited), 0))::numeric, 2)            AS nd_percent,
			ROUND(((SELECT cnt FROM visited) * 100.0 /
			       NULLIF((SELECT cnt FROM universe), 0))::numeric, 2)           AS reach_rate,
			ROUND(((GREATEST((SELECT cnt FROM visited) - nb.nd_pos, 0) +
			        GREATEST((SELECT cnt FROM universe) - (SELECT cnt FROM visited), 0))
			       * 100.0 /
			       NULLIF((SELECT cnt FROM universe), 0))::numeric, 2)           AS opportunity_pct
		FROM nd_per_brand nb
		INNER JOIN brands b ON b.uuid = nb.brand_uuid
		ORDER BY nd_percent DESC
	`

	var results []GapRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND gap analysis", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "ND Gap Analysis (3-Zone Funnel)", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 — ADVANCED ANALYTICS
// ─────────────────────────────────────────────────────────────────────────────

// NDHeatmap — Brand × Territory ND% matrix.
//
// Response shape:
//
//	{
//	  brands:      [{uuid, name}, ...],
//	  territories: [{uuid, name}, ...],
//	  matrix:      [[nd_percent, ...], ...]   // matrix[brandIndex][territoryIndex]
//	}
//
// Query param ?level=province|area|subarea|commune  (default: province)
func NDHeatmap(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	level := c.Query("level")
	if level == "" {
		level = "province"
	}

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	geoCol, joinTable := "province_uuid", "provinces"
	switch level {
	case "area":
		geoCol, joinTable = "area_uuid", "areas"
	case "subarea":
		geoCol, joinTable = "sub_area_uuid", "sub_areas"
	case "commune":
		geoCol, joinTable = "commune_uuid", "communes"
	}

	type CellRow struct {
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		TerritoryName string  `json:"territory_name"`
		TerritoryUUID string  `json:"territory_uuid"`
		NdPos         int64   `json:"nd_pos"`
		TotalPos      int64   `json:"total_pos"`
		NdPercent     float64 `json:"nd_percent"`
	}

	sqlQuery := `
		WITH visited AS (
			SELECT pf.` + geoCol + `, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY pf.` + geoCol + `
		),
		nd_cells AS (
			SELECT pf.` + geoCol + `, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pf.` + geoCol + `, pfi.brand_uuid
		)
		SELECT
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			t.name                                                            AS territory_name,
			t.uuid                                                            AS territory_uuid,
			COALESCE(nc.nd_pos, 0)                                            AS nd_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			ROUND((COALESCE(nc.nd_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS nd_percent
		FROM nd_cells nc
		INNER JOIN brands       b ON b.uuid = nc.brand_uuid
		INNER JOIN ` + joinTable + ` t ON t.uuid = nc.` + geoCol + `
		LEFT  JOIN visited v ON v.` + geoCol + ` = nc.` + geoCol + `
		ORDER BY b.name, t.name
	`

	var raw []CellRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&raw).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND heatmap", "error": err.Error(),
		})
	}

	// Pivot into brands × territories matrix
	brandIndex := map[string]int{}
	terrIndex := map[string]int{}
	var brands []map[string]string
	var territories []map[string]string

	for _, r := range raw {
		if _, ok := brandIndex[r.BrandUUID]; !ok {
			brandIndex[r.BrandUUID] = len(brands)
			brands = append(brands, map[string]string{"uuid": r.BrandUUID, "name": r.BrandName})
		}
		if _, ok := terrIndex[r.TerritoryUUID]; !ok {
			terrIndex[r.TerritoryUUID] = len(territories)
			territories = append(territories, map[string]string{"uuid": r.TerritoryUUID, "name": r.TerritoryName})
		}
	}

	matrix := make([][]float64, len(brands))
	for i := range matrix {
		matrix[i] = make([]float64, len(territories))
	}
	for _, r := range raw {
		matrix[brandIndex[r.BrandUUID]][terrIndex[r.TerritoryUUID]] = r.NdPercent
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "ND Heatmap — Brand × Territory",
		"level":   level,
		"data": fiber.Map{
			"brands":      brands,
			"territories": territories,
			"matrix":      matrix,
		},
	})
}

// NDEvolution — Period-over-Period (PoP) ND% comparison.
// Compares current window (start_date → end_date) with the preceding window
// of equal length. Per brand:
//
//	current_nd_percent  — ND% in selected period
//	previous_nd_percent — ND% in the prior equal-length period
//	delta               — current - previous (pp points)
//	trend               — "up" | "down" | "stable"
func NDEvolution(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	// Compute previous period in Go to avoid @param::date cast issues with GORM
	const dateFmt = "2006-01-02"
	parsedStart, err1 := time.Parse(dateFmt, start_date)
	parsedEnd, err2 := time.Parse(dateFmt, end_date)
	if err1 != nil || err2 != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date must be YYYY-MM-DD",
		})
	}
	window := parsedEnd.Sub(parsedStart) + 24*time.Hour
	prevEnd := parsedStart.AddDate(0, 0, -1)
	prevStart := prevEnd.Add(-window + 24*time.Hour)
	prev_start_date := prevStart.Format(dateFmt)
	prev_end_date := prevEnd.Format(dateFmt)

	type EvoRow struct {
		BrandName         string  `json:"brand_name"`
		BrandUUID         string  `json:"brand_uuid"`
		CurrentNdPos      int64   `json:"current_nd_pos"`
		PreviousNdPos     int64   `json:"previous_nd_pos"`
		CurrentTotalPos   int64   `json:"current_total_pos"`
		PreviousTotalPos  int64   `json:"previous_total_pos"`
		CurrentNdPercent  float64 `json:"current_nd_percent"`
		PreviousNdPercent float64 `json:"previous_nd_percent"`
		Delta             float64 `json:"delta"`
		Trend             string  `json:"trend"`
	}

	sqlQuery := `
		WITH curr_visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS cnt
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		),
		prev_visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS cnt
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @prev_start_date AND @prev_end_date
			  AND pf.deleted_at IS NULL
		),
		curr_nd AS (
			SELECT pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		),
		prev_nd AS (
			SELECT pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @prev_start_date AND @prev_end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.name                                                                 AS brand_name,
			b.uuid                                                                 AS brand_uuid,
			COALESCE(cn.nd_pos, 0)                                                 AS current_nd_pos,
			COALESCE(pn.nd_pos, 0)                                                 AS previous_nd_pos,
			(SELECT cnt FROM curr_visited)                                         AS current_total_pos,
			(SELECT cnt FROM prev_visited)                                         AS previous_total_pos,
			ROUND((COALESCE(cn.nd_pos, 0) * 100.0 /
			       NULLIF((SELECT cnt FROM curr_visited), 0))::numeric, 2)        AS current_nd_percent,
			ROUND((COALESCE(pn.nd_pos, 0) * 100.0 /
			       NULLIF((SELECT cnt FROM prev_visited), 0))::numeric, 2)        AS previous_nd_percent,
			ROUND((COALESCE(cn.nd_pos, 0) * 100.0 /
			       NULLIF((SELECT cnt FROM curr_visited), 0) -
			       COALESCE(pn.nd_pos, 0) * 100.0 /
			       NULLIF((SELECT cnt FROM prev_visited), 0))::numeric, 2)        AS delta,
			CASE
				WHEN (COALESCE(cn.nd_pos, 0) * 1.0 /
				      NULLIF((SELECT cnt FROM curr_visited), 0)) >
				     (COALESCE(pn.nd_pos, 0) * 1.0 /
				      NULLIF((SELECT cnt FROM prev_visited), 0)) THEN 'up'
				WHEN (COALESCE(cn.nd_pos, 0) * 1.0 /
				      NULLIF((SELECT cnt FROM curr_visited), 0)) <
				     (COALESCE(pn.nd_pos, 0) * 1.0 /
				      NULLIF((SELECT cnt FROM prev_visited), 0)) THEN 'down'
				ELSE 'stable'
			END AS trend
		FROM (SELECT DISTINCT brand_uuid FROM curr_nd
		      UNION
		      SELECT DISTINCT brand_uuid FROM prev_nd) all_brands
		INNER JOIN brands b ON b.uuid = all_brands.brand_uuid
		LEFT  JOIN curr_nd cn ON cn.brand_uuid = all_brands.brand_uuid
		LEFT  JOIN prev_nd pn ON pn.brand_uuid = all_brands.brand_uuid
		ORDER BY current_nd_percent DESC
	`

	var results []EvoRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":    country_uuid,
		"province_uuid":   province_uuid,
		"area_uuid":       area_uuid,
		"sub_area_uuid":   sub_area_uuid,
		"commune_uuid":    commune_uuid,
		"start_date":      start_date,
		"end_date":        end_date,
		"prev_start_date": prev_start_date,
		"prev_end_date":   prev_end_date,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch ND evolution", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "ND Period-over-Period Evolution", "data": results})
}

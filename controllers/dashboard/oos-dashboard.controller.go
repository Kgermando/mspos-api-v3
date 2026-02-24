package dashboard

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════════╗
// ║         OUT-OF-STOCK (OOS) DASHBOARD — HIGH-LEVEL ANALYTICS                ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  Out-of-Stock Rate = (POS where brand counter = 0)                          ║
// ║                     ──────────────────────────────  × 100                  ║
// ║                          Total distinct POS visited                          ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  SECTION 1 — TABLE VIEWS    : Province / Area / SubArea / Commune           ║
// ║  SECTION 2 — BAR CHARTS     : Province / Area / SubArea / Commune           ║
// ║  SECTION 3 — TREND CHART    : OOS% by Brand per Month                       ║
// ║  SECTION 4 — POWER ANALYTICS: Summary KPI / Brand Ranking / Critical Alert  ║
// ║  SECTION 5 — ADVANCED       : Brand×Territory Heatmap / Period Evolution     ║
// ╚══════════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 — TABLE VIEWS
// Each row = (territory × brand) with:
//   oos_pos      — distinct POS where brand counter = 0
//   total_pos    — total distinct POS visited (any brand)
//   oos_percent  — oos_pos / total_pos × 100  (higher = worse)
//   coverage_pos — distinct POS where brand counter > 0 (in-stock)
//   coverage_pct — coverage_pos / total_pos × 100
// ─────────────────────────────────────────────────────────────────────────────

// OOSTableViewProvince — OOS breakdown per brand at Province level
func OOSTableViewProvince(c *fiber.Ctx) error {
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
		oos_counts AS (
			SELECT
				pf.province_uuid,
				pfi.brand_uuid,
				COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY pf.province_uuid, pfi.brand_uuid
		),
		coverage_counts AS (
			SELECT
				pf.province_uuid,
				pfi.brand_uuid,
				COUNT(DISTINCT pf.pos_uuid) AS coverage_pos
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
			pr.name                                                           AS territory_name,
			pr.uuid                                                           AS territory_uuid,
			'province'                                                        AS territory_level,
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			COALESCE(oos.oos_pos, 0)                                          AS oos_pos,
			COALESCE(cc.coverage_pos, 0)                                      AS coverage_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			ROUND((COALESCE(oos.oos_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS oos_percent,
			ROUND((COALESCE(cc.coverage_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS coverage_pct
		FROM oos_counts oos
		INNER JOIN brands b     ON b.uuid   = oos.brand_uuid
		INNER JOIN provinces pr ON pr.uuid  = oos.province_uuid
		LEFT  JOIN visited v    ON v.province_uuid   = oos.province_uuid
		LEFT  JOIN coverage_counts cc ON cc.province_uuid = oos.province_uuid
		                              AND cc.brand_uuid   = oos.brand_uuid
		ORDER BY pr.name, oos_percent DESC
	`

	type OOSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		OosPos         int64   `json:"oos_pos"`
		CoveragePos    int64   `json:"coverage_pos"`
		TotalPos       int64   `json:"total_pos"`
		OosPercent     float64 `json:"oos_percent"`
		CoveragePct    float64 `json:"coverage_pct"`
	}

	var results []OOSRow
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
			"status": "error", "message": "Failed to fetch OOS province data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "OOS Province Table", "data": results})
}

// OOSTableViewArea — OOS breakdown per brand at Area level
func OOSTableViewArea(c *fiber.Ctx) error {
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
		oos_counts AS (
			SELECT pf.area_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY pf.area_uuid, pfi.brand_uuid
		),
		coverage_counts AS (
			SELECT pf.area_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS coverage_pos
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
			COALESCE(oos.oos_pos, 0)                                          AS oos_pos,
			COALESCE(cc.coverage_pos, 0)                                      AS coverage_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			ROUND((COALESCE(oos.oos_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS oos_percent,
			ROUND((COALESCE(cc.coverage_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS coverage_pct
		FROM oos_counts oos
		INNER JOIN brands b  ON b.uuid  = oos.brand_uuid
		INNER JOIN areas  a  ON a.uuid  = oos.area_uuid
		LEFT  JOIN visited v ON v.area_uuid = oos.area_uuid
		LEFT  JOIN coverage_counts cc ON cc.area_uuid  = oos.area_uuid
		                              AND cc.brand_uuid = oos.brand_uuid
		ORDER BY a.name, oos_percent DESC
	`

	type OOSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		OosPos         int64   `json:"oos_pos"`
		CoveragePos    int64   `json:"coverage_pos"`
		TotalPos       int64   `json:"total_pos"`
		OosPercent     float64 `json:"oos_percent"`
		CoveragePct    float64 `json:"coverage_pct"`
	}

	var results []OOSRow
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
			"status": "error", "message": "Failed to fetch OOS area data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "OOS Area Table", "data": results})
}

// OOSTableViewSubArea — OOS breakdown per brand at SubArea level
func OOSTableViewSubArea(c *fiber.Ctx) error {
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
		oos_counts AS (
			SELECT pf.sub_area_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY pf.sub_area_uuid, pfi.brand_uuid
		),
		coverage_counts AS (
			SELECT pf.sub_area_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS coverage_pos
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
			COALESCE(oos.oos_pos, 0)                                          AS oos_pos,
			COALESCE(cc.coverage_pos, 0)                                      AS coverage_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			ROUND((COALESCE(oos.oos_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS oos_percent,
			ROUND((COALESCE(cc.coverage_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS coverage_pct
		FROM oos_counts oos
		INNER JOIN brands    b   ON b.uuid   = oos.brand_uuid
		INNER JOIN sub_areas sa  ON sa.uuid  = oos.sub_area_uuid
		LEFT  JOIN visited   v   ON v.sub_area_uuid = oos.sub_area_uuid
		LEFT  JOIN coverage_counts cc ON cc.sub_area_uuid = oos.sub_area_uuid
		                              AND cc.brand_uuid   = oos.brand_uuid
		ORDER BY sa.name, oos_percent DESC
	`

	type OOSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		OosPos         int64   `json:"oos_pos"`
		CoveragePos    int64   `json:"coverage_pos"`
		TotalPos       int64   `json:"total_pos"`
		OosPercent     float64 `json:"oos_percent"`
		CoveragePct    float64 `json:"coverage_pct"`
	}

	var results []OOSRow
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
			"status": "error", "message": "Failed to fetch OOS subarea data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "OOS SubArea Table", "data": results})
}

// OOSTableViewCommune — OOS breakdown per brand at Commune level
func OOSTableViewCommune(c *fiber.Ctx) error {
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
		oos_counts AS (
			SELECT pf.commune_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY pf.commune_uuid, pfi.brand_uuid
		),
		coverage_counts AS (
			SELECT pf.commune_uuid, pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS coverage_pos
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
			COALESCE(oos.oos_pos, 0)                                          AS oos_pos,
			COALESCE(cc.coverage_pos, 0)                                      AS coverage_pos,
			COALESCE(v.total_pos, 0)                                          AS total_pos,
			ROUND((COALESCE(oos.oos_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS oos_percent,
			ROUND((COALESCE(cc.coverage_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS coverage_pct
		FROM oos_counts oos
		INNER JOIN brands    b   ON b.uuid   = oos.brand_uuid
		INNER JOIN communes  cm  ON cm.uuid  = oos.commune_uuid
		LEFT  JOIN visited   v   ON v.commune_uuid = oos.commune_uuid
		LEFT  JOIN coverage_counts cc ON cc.commune_uuid = oos.commune_uuid
		                              AND cc.brand_uuid  = oos.brand_uuid
		ORDER BY cm.name, oos_percent DESC
	`

	type OOSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		OosPos         int64   `json:"oos_pos"`
		CoveragePos    int64   `json:"coverage_pos"`
		TotalPos       int64   `json:"total_pos"`
		OosPercent     float64 `json:"oos_percent"`
		CoveragePct    float64 `json:"coverage_pct"`
	}

	var results []OOSRow
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
			"status": "error", "message": "Failed to fetch OOS commune data", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "OOS Commune Table", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 — BAR CHARTS
// Returns chart-ready series: one series per brand, categories = territories.
//   series[i].brand_name, series[i].brand_uuid
//   series[i].data[] = [{territory_name, oos_percent}]
// ─────────────────────────────────────────────────────────────────────────────

func buildOOSBarChart(c *fiber.Ctx, level string) error {
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

	levelSQL := map[string]struct{ dim, join string }{
		"province": {
			"pf.province_uuid",
			"INNER JOIN provinces t ON t.uuid = oos.dim_uuid",
		},
		"area": {
			"pf.area_uuid",
			"INNER JOIN areas t ON t.uuid = oos.dim_uuid",
		},
		"subarea": {
			"pf.sub_area_uuid",
			"INNER JOIN sub_areas t ON t.uuid = oos.dim_uuid",
		},
		"commune": {
			"pf.commune_uuid",
			"INNER JOIN communes t ON t.uuid = oos.dim_uuid",
		},
	}

	ls, ok := levelSQL[level]
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "invalid level; use province|area|subarea|commune",
		})
	}

	sqlQuery := `
		WITH visited AS (
			SELECT ` + ls.dim + ` AS dim_uuid, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY ` + ls.dim + `
		),
		oos AS (
			SELECT ` + ls.dim + ` AS dim_uuid, pfi.brand_uuid,
			       COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY ` + ls.dim + `, pfi.brand_uuid
		)
		SELECT
			t.name                                                            AS territory_name,
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			ROUND((COALESCE(oos.oos_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS oos_percent
		FROM oos
		` + ls.join + `
		INNER JOIN brands b ON b.uuid = oos.brand_uuid
		LEFT  JOIN visited v ON v.dim_uuid = oos.dim_uuid
		ORDER BY t.name, oos_percent DESC
	`

	type RawRow struct {
		TerritoryName string  `json:"territory_name"`
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		OosPercent    float64 `json:"oos_percent"`
	}

	var rows []RawRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&rows).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch OOS bar chart data", "error": err.Error(),
		})
	}

	// Pivot: brand → []{ territory, oos_percent }
	type DataPoint struct {
		TerritoryName string  `json:"territory_name"`
		OosPercent    float64 `json:"oos_percent"`
	}
	type Series struct {
		BrandName string      `json:"brand_name"`
		BrandUUID string      `json:"brand_uuid"`
		Data      []DataPoint `json:"data"`
	}

	seriesMap := map[string]*Series{}
	var seriesOrder []string
	categories := map[string]struct{}{}

	for _, r := range rows {
		if _, ok := seriesMap[r.BrandUUID]; !ok {
			seriesMap[r.BrandUUID] = &Series{BrandName: r.BrandName, BrandUUID: r.BrandUUID}
			seriesOrder = append(seriesOrder, r.BrandUUID)
		}
		seriesMap[r.BrandUUID].Data = append(seriesMap[r.BrandUUID].Data, DataPoint{
			TerritoryName: r.TerritoryName,
			OosPercent:    r.OosPercent,
		})
		categories[r.TerritoryName] = struct{}{}
	}

	var series []Series
	for _, uuid := range seriesOrder {
		series = append(series, *seriesMap[uuid])
	}
	var cats []string
	for k := range categories {
		cats = append(cats, k)
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "OOS Bar Chart — " + level,
		"level":   level,
		"data": fiber.Map{
			"categories": cats,
			"series":     series,
		},
	})
}

func OOSBarChartProvince(c *fiber.Ctx) error { return buildOOSBarChart(c, "province") }
func OOSBarChartArea(c *fiber.Ctx) error     { return buildOOSBarChart(c, "area") }
func OOSBarChartSubArea(c *fiber.Ctx) error  { return buildOOSBarChart(c, "subarea") }
func OOSBarChartCommune(c *fiber.Ctx) error  { return buildOOSBarChart(c, "commune") }

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 — MONTHLY TREND LINE CHART
// OOS% for each brand, grouped by month.
// ─────────────────────────────────────────────────────────────────────────────

// OOSLineChartByMonth — monthly OOS% per brand (line/area chart ready)
func OOSLineChartByMonth(c *fiber.Ctx) error {
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
		monthly_oos AS (
			SELECT
				TO_CHAR(pf.created_at, 'YYYY-MM') AS month,
				pfi.brand_uuid,
				COUNT(DISTINCT pf.pos_uuid)        AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY month, pfi.brand_uuid
		)
		SELECT
			mo.month,
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			COALESCE(mo.oos_pos, 0)                                           AS oos_pos,
			COALESCE(mv.total_pos, 0)                                         AS total_pos,
			ROUND((COALESCE(mo.oos_pos, 0) * 100.0 /
			       NULLIF(COALESCE(mv.total_pos, 0), 0))::numeric, 2)        AS oos_percent
		FROM monthly_oos mo
		INNER JOIN brands         b  ON b.uuid  = mo.brand_uuid
		LEFT  JOIN monthly_visited mv ON mv.month = mo.month
		ORDER BY mo.month, brand_name
	`

	type MonthRow struct {
		Month      string  `json:"month"`
		BrandName  string  `json:"brand_name"`
		BrandUUID  string  `json:"brand_uuid"`
		OosPos     int64   `json:"oos_pos"`
		TotalPos   int64   `json:"total_pos"`
		OosPercent float64 `json:"oos_percent"`
	}

	var rows []MonthRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&rows).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch OOS monthly trend", "error": err.Error(),
		})
	}

	type DataPoint struct {
		Month      string  `json:"month"`
		OosPercent float64 `json:"oos_percent"`
	}
	type Series struct {
		BrandName string      `json:"brand_name"`
		BrandUUID string      `json:"brand_uuid"`
		Data      []DataPoint `json:"data"`
	}

	seriesMap := map[string]*Series{}
	var order []string
	months := map[string]struct{}{}

	for _, r := range rows {
		if _, ok := seriesMap[r.BrandUUID]; !ok {
			seriesMap[r.BrandUUID] = &Series{BrandName: r.BrandName, BrandUUID: r.BrandUUID}
			order = append(order, r.BrandUUID)
		}
		seriesMap[r.BrandUUID].Data = append(seriesMap[r.BrandUUID].Data, DataPoint{
			Month: r.Month, OosPercent: r.OosPercent,
		})
		months[r.Month] = struct{}{}
	}

	var series []Series
	for _, id := range order {
		series = append(series, *seriesMap[id])
	}
	var sortedMonths []string
	for m := range months {
		sortedMonths = append(sortedMonths, m)
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "OOS Monthly Trend by Brand",
		"data": fiber.Map{
			"months": sortedMonths,
			"series": series,
		},
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 — POWER ANALYTICS
// ─────────────────────────────────────────────────────────────────────────────

// OOSSummaryKPI — executive single-number cards for the dashboard header.
//
//	total_pos_visited    — distinct POS visited in the period
//	total_oos_events     — total OOS records (brand × POS) across all brands
//	avg_oos_percent      — weighted average OOS% across all brands
//	most_affected_brand  — brand with the highest OOS%
//	least_affected_brand — brand with the lowest OOS%
//	critical_threshold   — number of brands with OOS% > 30
func OOSSummaryKPI(c *fiber.Ctx) error {
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
			SELECT COUNT(DISTINCT pf.pos_uuid) AS total_visited
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		),
		brand_oos AS (
			SELECT pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY pfi.brand_uuid
		),
		brand_pct AS (
			SELECT
				bo.brand_uuid,
				b.name AS brand_name,
				bo.oos_pos,
				(SELECT total_visited FROM visited)                                    AS total_pos,
				ROUND((bo.oos_pos * 100.0 /
				       NULLIF((SELECT total_visited FROM visited), 0))::numeric, 2)   AS oos_percent
			FROM brand_oos bo
			INNER JOIN brands b ON b.uuid = bo.brand_uuid
		)
		SELECT
			(SELECT total_visited FROM visited)          AS total_pos_visited,
			(SELECT SUM(oos_pos) FROM brand_pct)         AS total_oos_events,
			ROUND(AVG(oos_percent)::numeric, 2)          AS avg_oos_percent,
			(SELECT brand_name FROM brand_pct ORDER BY oos_percent DESC LIMIT 1) AS most_affected_brand,
			(SELECT brand_name FROM brand_pct ORDER BY oos_percent ASC  LIMIT 1) AS least_affected_brand,
			(SELECT COUNT(*) FROM brand_pct WHERE oos_percent > 30)              AS critical_threshold
		FROM brand_pct
	`

	type KPIResult struct {
		TotalPosVisited    int64   `json:"total_pos_visited"`
		TotalOosEvents     int64   `json:"total_oos_events"`
		AvgOosPercent      float64 `json:"avg_oos_percent"`
		MostAffectedBrand  string  `json:"most_affected_brand"`
		LeastAffectedBrand string  `json:"least_affected_brand"`
		CriticalThreshold  int64   `json:"critical_threshold"`
	}

	var result KPIResult
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&result).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch OOS summary KPI", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "OOS Summary KPI", "data": result})
}

// OOSBrandRanking — brands ranked by OOS% (worst first).
// severity: "critical" (>50%), "high" (30-50%), "medium" (15-30%), "low" (<15%)
func OOSBrandRanking(c *fiber.Ctx) error {
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
		brand_oos AS (
			SELECT pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			ROW_NUMBER() OVER (ORDER BY oos_percent DESC)                    AS rank,
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			bo.oos_pos,
			(SELECT cnt FROM visited)                                         AS total_pos,
			ROUND((bo.oos_pos * 100.0 /
			       NULLIF((SELECT cnt FROM visited), 0))::numeric, 2)        AS oos_percent,
			CASE
				WHEN (bo.oos_pos * 100.0 / NULLIF((SELECT cnt FROM visited), 0)) > 50 THEN 'critical'
				WHEN (bo.oos_pos * 100.0 / NULLIF((SELECT cnt FROM visited), 0)) > 30 THEN 'high'
				WHEN (bo.oos_pos * 100.0 / NULLIF((SELECT cnt FROM visited), 0)) > 15 THEN 'medium'
				ELSE 'low'
			END AS severity
		FROM brand_oos bo
		INNER JOIN brands b ON b.uuid = bo.brand_uuid
		ORDER BY oos_percent DESC
	`

	type RankRow struct {
		Rank       int64   `json:"rank"`
		BrandName  string  `json:"brand_name"`
		BrandUUID  string  `json:"brand_uuid"`
		OosPos     int64   `json:"oos_pos"`
		TotalPos   int64   `json:"total_pos"`
		OosPercent float64 `json:"oos_percent"`
		Severity   string  `json:"severity"`
	}

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
			"status": "error", "message": "Failed to fetch OOS brand ranking", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "OOS Brand Ranking (worst first)", "data": results})
}

// OOSCriticalAlert — top 20 hotspot (brand × territory) pairs with OOS% > 15.
// ?level=province|area|subarea|commune
// severity: "critical" > 50%, "high" > 30%, "medium" > 15%
func OOSCriticalAlert(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	level := c.Query("level", "province")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	levelSQL := map[string]struct{ dim, join string }{
		"province": {"pf.province_uuid", "INNER JOIN provinces t ON t.uuid = oos.dim_uuid"},
		"area":     {"pf.area_uuid", "INNER JOIN areas t ON t.uuid = oos.dim_uuid"},
		"subarea":  {"pf.sub_area_uuid", "INNER JOIN sub_areas t ON t.uuid = oos.dim_uuid"},
		"commune":  {"pf.commune_uuid", "INNER JOIN communes t ON t.uuid = oos.dim_uuid"},
	}
	ls, ok := levelSQL[level]
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "invalid level; use province|area|subarea|commune",
		})
	}

	sqlQuery := `
		WITH visited AS (
			SELECT ` + ls.dim + ` AS dim_uuid, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY ` + ls.dim + `
		),
		oos AS (
			SELECT ` + ls.dim + ` AS dim_uuid, pfi.brand_uuid,
			       COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY ` + ls.dim + `, pfi.brand_uuid
		)
		SELECT
			b.name                                                            AS brand_name,
			b.uuid                                                            AS brand_uuid,
			t.name                                                            AS territory_name,
			ROUND((oos.oos_pos * 100.0 / NULLIF(v.total_pos, 0))::numeric, 2) AS oos_percent,
			CASE
				WHEN (oos.oos_pos * 100.0 / NULLIF(v.total_pos, 0)) > 50 THEN 'critical'
				WHEN (oos.oos_pos * 100.0 / NULLIF(v.total_pos, 0)) > 30 THEN 'high'
				WHEN (oos.oos_pos * 100.0 / NULLIF(v.total_pos, 0)) > 15 THEN 'medium'
				ELSE 'low'
			END AS severity
		FROM oos
		` + ls.join + `
		INNER JOIN brands b ON b.uuid = oos.brand_uuid
		LEFT  JOIN visited v ON v.dim_uuid = oos.dim_uuid
		WHERE (oos.oos_pos * 100.0 / NULLIF(v.total_pos, 0)) > 15
		ORDER BY oos_percent DESC
		LIMIT 20
	`

	type AlertRow struct {
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		TerritoryName string  `json:"territory_name"`
		OosPercent    float64 `json:"oos_percent"`
		Severity      string  `json:"severity"`
	}

	var results []AlertRow
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
			"status": "error", "message": "Failed to fetch OOS critical alerts", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "OOS Critical Alerts — top hotspots",
		"level":   level,
		"data":    results,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 — ADVANCED ANALYTICS
// ─────────────────────────────────────────────────────────────────────────────

// OOSHeatmap — Brand × Territory matrix of OOS%.
// ?level=province|area|subarea|commune
// Returns brands[], territories[], matrix[][] (brand-major order)
func OOSHeatmap(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	level := c.Query("level", "province")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	levelSQL := map[string]struct{ dim, join string }{
		"province": {"pf.province_uuid", "INNER JOIN provinces t ON t.uuid = oos.dim_uuid"},
		"area":     {"pf.area_uuid", "INNER JOIN areas t ON t.uuid = oos.dim_uuid"},
		"subarea":  {"pf.sub_area_uuid", "INNER JOIN sub_areas t ON t.uuid = oos.dim_uuid"},
		"commune":  {"pf.commune_uuid", "INNER JOIN communes t ON t.uuid = oos.dim_uuid"},
	}
	ls, ok := levelSQL[level]
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "invalid level; use province|area|subarea|commune",
		})
	}

	sqlQuery := `
		WITH visited AS (
			SELECT ` + ls.dim + ` AS dim_uuid, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			GROUP BY ` + ls.dim + `
		),
		oos AS (
			SELECT ` + ls.dim + ` AS dim_uuid, pfi.brand_uuid,
			       COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter = 0
			GROUP BY ` + ls.dim + `, pfi.brand_uuid
		)
		SELECT
			b.name  AS brand_name,
			b.uuid  AS brand_uuid,
			t.name  AS territory_name,
			t.uuid  AS territory_uuid,
			ROUND((oos.oos_pos * 100.0 / NULLIF(v.total_pos, 0))::numeric, 2) AS oos_percent
		FROM oos
		` + ls.join + `
		INNER JOIN brands b ON b.uuid = oos.brand_uuid
		LEFT  JOIN visited v ON v.dim_uuid = oos.dim_uuid
		ORDER BY b.name, t.name
	`

	type RawRow struct {
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		TerritoryName string  `json:"territory_name"`
		TerritoryUUID string  `json:"territory_uuid"`
		OosPercent    float64 `json:"oos_percent"`
	}

	var raw []RawRow
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
			"status": "error", "message": "Failed to fetch OOS heatmap", "error": err.Error(),
		})
	}

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
		matrix[brandIndex[r.BrandUUID]][terrIndex[r.TerritoryUUID]] = r.OosPercent
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "OOS Heatmap — Brand × Territory",
		"level":   level,
		"data": fiber.Map{
			"brands":      brands,
			"territories": territories,
			"matrix":      matrix,
		},
	})
}

// OOSEvolution — Period-over-Period (PoP) OOS% comparison.
// Compares current window (start_date → end_date) with the preceding window
// of equal length. Per brand:
//
//	current_oos_percent  — OOS% in selected period
//	previous_oos_percent — OOS% in the prior equal-length period
//	delta                — current - previous (pp points)
//	trend                — "worsening" | "improving" | "stable"
func OOSEvolution(c *fiber.Ctx) error {
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

	type EvoRow struct {
		BrandName          string  `json:"brand_name"`
		BrandUUID          string  `json:"brand_uuid"`
		CurrentOosPos      int64   `json:"current_oos_pos"`
		PreviousOosPos     int64   `json:"previous_oos_pos"`
		CurrentTotalPos    int64   `json:"current_total_pos"`
		PreviousTotalPos   int64   `json:"previous_total_pos"`
		CurrentOosPercent  float64 `json:"current_oos_percent"`
		PreviousOosPercent float64 `json:"previous_oos_percent"`
		Delta              float64 `json:"delta"`
		Trend              string  `json:"trend"`
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
			  AND pf.created_at BETWEEN
			        (@start_date::date - (@end_date::date - @start_date::date + 1))
			    AND (@start_date::date - 1)
			  AND pf.deleted_at IS NULL
		),
		curr_oos AS (
			SELECT pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter = 0
			GROUP BY pfi.brand_uuid
		),
		prev_oos AS (
			SELECT pfi.brand_uuid, COUNT(DISTINCT pf.pos_uuid) AS oos_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN
			        (@start_date::date - (@end_date::date - @start_date::date + 1))
			    AND (@start_date::date - 1)
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter = 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.name                                                                 AS brand_name,
			b.uuid                                                                 AS brand_uuid,
			COALESCE(co.oos_pos, 0)                                                AS current_oos_pos,
			COALESCE(po.oos_pos, 0)                                                AS previous_oos_pos,
			(SELECT cnt FROM curr_visited)                                         AS current_total_pos,
			(SELECT cnt FROM prev_visited)                                         AS previous_total_pos,
			ROUND((COALESCE(co.oos_pos, 0) * 100.0 /
			       NULLIF((SELECT cnt FROM curr_visited), 0))::numeric, 2)        AS current_oos_percent,
			ROUND((COALESCE(po.oos_pos, 0) * 100.0 /
			       NULLIF((SELECT cnt FROM prev_visited), 0))::numeric, 2)        AS previous_oos_percent,
			ROUND((COALESCE(co.oos_pos, 0) * 100.0 /
			       NULLIF((SELECT cnt FROM curr_visited), 0) -
			       COALESCE(po.oos_pos, 0) * 100.0 /
			       NULLIF((SELECT cnt FROM prev_visited), 0))::numeric, 2)        AS delta,
			CASE
				WHEN (COALESCE(co.oos_pos, 0) * 1.0 /
				      NULLIF((SELECT cnt FROM curr_visited), 0)) >
				     (COALESCE(po.oos_pos, 0) * 1.0 /
				      NULLIF((SELECT cnt FROM prev_visited), 0)) THEN 'worsening'
				WHEN (COALESCE(co.oos_pos, 0) * 1.0 /
				      NULLIF((SELECT cnt FROM curr_visited), 0)) <
				     (COALESCE(po.oos_pos, 0) * 1.0 /
				      NULLIF((SELECT cnt FROM prev_visited), 0)) THEN 'improving'
				ELSE 'stable'
			END AS trend
		FROM (SELECT DISTINCT brand_uuid FROM curr_oos
		      UNION
		      SELECT DISTINCT brand_uuid FROM prev_oos) all_brands
		INNER JOIN brands b  ON b.uuid = all_brands.brand_uuid
		LEFT  JOIN curr_oos co ON co.brand_uuid = all_brands.brand_uuid
		LEFT  JOIN prev_oos po ON po.brand_uuid = all_brands.brand_uuid
		ORDER BY current_oos_percent DESC
	`

	var results []EvoRow
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
			"status": "error", "message": "Failed to fetch OOS evolution", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "OOS Period-over-Period Evolution", "data": results})
}

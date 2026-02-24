package dashboard

import (
	"math"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════════╗
// ║         WEIGHTED SALES (WS) DASHBOARD — SALES-VOLUME WEIGHTED PRESENCE     ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  Weighted Sales % =  SUM(sold at POS where brand counter > 0)              ║
// ║                     ────────────────────────────────────────  × 100        ║
// ║                     SUM(total sold across ALL visited POS)                  ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  Unlike WD (weighted by stock fardes), WS weights each POS by units SOLD — ║
// ║  a brand present in high-turnover outlets scores higher here.               ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  SECTION 1 — TABLE VIEWS    : Province / Area / SubArea / Commune           ║
// ║  SECTION 2 — BAR CHARTS     : Province / Area / SubArea / Commune           ║
// ║  SECTION 3 — TREND CHART    : WS% by Brand per Month                        ║
// ║  SECTION 4 — POWER ANALYTICS: Summary KPI / Brand Ranking / Gap Analysis    ║
// ║  SECTION 5 — ADVANCED       : Heatmap / Evolution / WS vs ND Correlation    ║
// ╚══════════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 — TABLE VIEWS
//
//	Each row = (territory × brand) with:
//	  brand_sold    — total units sold at POS where brand counter > 0
//	  total_sold    — total units sold across ALL visited POS in the territory
//	  ws_percent    — brand_sold / total_sold × 100
//	  nd_pos        — distinct POS where brand counter > 0
//	  total_pos     — distinct POS visited
//	  nd_percent    — nd_pos / total_pos × 100  (for comparison)
// ─────────────────────────────────────────────────────────────────────────────

// WSTableViewProvince — WS% breakdown per brand at Province level
func WSTableViewProvince(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT
				pf.province_uuid,
				SUM(pfi.sold)                AS total_sold,
				COUNT(DISTINCT pf.pos_uuid)  AS total_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.province_uuid
		),
		brand_sales AS (
			SELECT
				pf.province_uuid,
				pfi.brand_uuid,
				SUM(pfi.sold)                AS brand_sold,
				COUNT(DISTINCT pf.pos_uuid)  AS nd_pos
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
			pr.name                                                              AS territory_name,
			pr.uuid                                                              AS territory_uuid,
			'province'                                                           AS territory_level,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			bs.brand_sold,
			COALESCE(ts.total_sold, 0)                                           AS total_sold,
			bs.nd_pos,
			COALESCE(ts.total_pos, 0)                                            AS total_pos,
			ROUND((bs.brand_sold * 100.0 /
			       NULLIF(ts.total_sold, 0))::numeric, 2)                        AS ws_percent,
			ROUND((bs.nd_pos * 100.0 /
			       NULLIF(ts.total_pos, 0))::numeric, 2)                         AS nd_percent
		FROM brand_sales bs
		INNER JOIN brands    b  ON b.uuid  = bs.brand_uuid
		INNER JOIN provinces pr ON pr.uuid = bs.province_uuid
		LEFT  JOIN total_sales ts ON ts.province_uuid = bs.province_uuid
		ORDER BY pr.name, ws_percent DESC
	`

	type WSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		WsPercent      float64 `json:"ws_percent"`
		NdPercent      float64 `json:"nd_percent"`
	}

	var results []WSRow
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
			"status": "error", "message": "Failed to fetch WS province table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Province Table", "data": results})
}

// WSTableViewArea — WS% breakdown per brand at Area level
func WSTableViewArea(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT
				pf.area_uuid,
				SUM(pfi.sold)                AS total_sold,
				COUNT(DISTINCT pf.pos_uuid)  AS total_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.area_uuid
		),
		brand_sales AS (
			SELECT
				pf.area_uuid,
				pfi.brand_uuid,
				SUM(pfi.sold)                AS brand_sold,
				COUNT(DISTINCT pf.pos_uuid)  AS nd_pos
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
			a.name                                                               AS territory_name,
			a.uuid                                                               AS territory_uuid,
			'area'                                                               AS territory_level,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			bs.brand_sold,
			COALESCE(ts.total_sold, 0)                                           AS total_sold,
			bs.nd_pos,
			COALESCE(ts.total_pos, 0)                                            AS total_pos,
			ROUND((bs.brand_sold * 100.0 /
			       NULLIF(ts.total_sold, 0))::numeric, 2)                        AS ws_percent,
			ROUND((bs.nd_pos * 100.0 /
			       NULLIF(ts.total_pos, 0))::numeric, 2)                         AS nd_percent
		FROM brand_sales bs
		INNER JOIN brands b ON b.uuid = bs.brand_uuid
		INNER JOIN areas  a ON a.uuid = bs.area_uuid
		LEFT  JOIN total_sales ts ON ts.area_uuid = bs.area_uuid
		ORDER BY a.name, ws_percent DESC
	`

	type WSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		WsPercent      float64 `json:"ws_percent"`
		NdPercent      float64 `json:"nd_percent"`
	}

	var results []WSRow
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
			"status": "error", "message": "Failed to fetch WS area table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Area Table", "data": results})
}

// WSTableViewSubArea — WS% breakdown per brand at SubArea level
func WSTableViewSubArea(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT
				pf.sub_area_uuid,
				SUM(pfi.sold)                AS total_sold,
				COUNT(DISTINCT pf.pos_uuid)  AS total_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.sub_area_uuid
		),
		brand_sales AS (
			SELECT
				pf.sub_area_uuid,
				pfi.brand_uuid,
				SUM(pfi.sold)                AS brand_sold,
				COUNT(DISTINCT pf.pos_uuid)  AS nd_pos
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
			sa.name                                                              AS territory_name,
			sa.uuid                                                              AS territory_uuid,
			'subarea'                                                            AS territory_level,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			bs.brand_sold,
			COALESCE(ts.total_sold, 0)                                           AS total_sold,
			bs.nd_pos,
			COALESCE(ts.total_pos, 0)                                            AS total_pos,
			ROUND((bs.brand_sold * 100.0 /
			       NULLIF(ts.total_sold, 0))::numeric, 2)                        AS ws_percent,
			ROUND((bs.nd_pos * 100.0 /
			       NULLIF(ts.total_pos, 0))::numeric, 2)                         AS nd_percent
		FROM brand_sales bs
		INNER JOIN brands    b  ON b.uuid  = bs.brand_uuid
		INNER JOIN sub_areas sa ON sa.uuid = bs.sub_area_uuid
		LEFT  JOIN total_sales ts ON ts.sub_area_uuid = bs.sub_area_uuid
		ORDER BY sa.name, ws_percent DESC
	`

	type WSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		WsPercent      float64 `json:"ws_percent"`
		NdPercent      float64 `json:"nd_percent"`
	}

	var results []WSRow
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
			"status": "error", "message": "Failed to fetch WS subarea table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS SubArea Table", "data": results})
}

// WSTableViewCommune — WS% breakdown per brand at Commune level
func WSTableViewCommune(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT
				pf.commune_uuid,
				SUM(pfi.sold)                AS total_sold,
				COUNT(DISTINCT pf.pos_uuid)  AS total_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.commune_uuid
		),
		brand_sales AS (
			SELECT
				pf.commune_uuid,
				pfi.brand_uuid,
				SUM(pfi.sold)                AS brand_sold,
				COUNT(DISTINCT pf.pos_uuid)  AS nd_pos
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
			co.name                                                              AS territory_name,
			co.uuid                                                              AS territory_uuid,
			'commune'                                                            AS territory_level,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			bs.brand_sold,
			COALESCE(ts.total_sold, 0)                                           AS total_sold,
			bs.nd_pos,
			COALESCE(ts.total_pos, 0)                                            AS total_pos,
			ROUND((bs.brand_sold * 100.0 /
			       NULLIF(ts.total_sold, 0))::numeric, 2)                        AS ws_percent,
			ROUND((bs.nd_pos * 100.0 /
			       NULLIF(ts.total_pos, 0))::numeric, 2)                         AS nd_percent
		FROM brand_sales bs
		INNER JOIN brands    b  ON b.uuid  = bs.brand_uuid
		INNER JOIN communes  co ON co.uuid = bs.commune_uuid
		LEFT  JOIN total_sales ts ON ts.commune_uuid = bs.commune_uuid
		ORDER BY co.name, ws_percent DESC
	`

	type WSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		WsPercent      float64 `json:"ws_percent"`
		NdPercent      float64 `json:"nd_percent"`
	}

	var results []WSRow
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
			"status": "error", "message": "Failed to fetch WS commune table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Commune Table", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 — BAR CHARTS
// ─────────────────────────────────────────────────────────────────────────────

// WSBarChartProvince — grouped bar chart data: WS% per brand per province
func WSBarChartProvince(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT pf.province_uuid, SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.province_uuid
		),
		brand_sales AS (
			SELECT pf.province_uuid, pfi.brand_uuid, SUM(pfi.sold) AS brand_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pf.province_uuid, pfi.brand_uuid
		)
		SELECT pr.name AS territory_name, pr.uuid AS territory_uuid,
			b.name AS brand_name, b.uuid AS brand_uuid,
			bs.brand_sold, COALESCE(ts.total_sold,0) AS total_sold,
			ROUND((bs.brand_sold*100.0/NULLIF(ts.total_sold,0))::numeric,2) AS ws_percent
		FROM brand_sales bs
		INNER JOIN brands b ON b.uuid=bs.brand_uuid
		INNER JOIN provinces pr ON pr.uuid=bs.province_uuid
		LEFT JOIN total_sales ts ON ts.province_uuid=bs.province_uuid
		ORDER BY pr.name, ws_percent DESC
	`

	type WSBarRow struct {
		TerritoryName string  `json:"territory_name"`
		TerritoryUUID string  `json:"territory_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		BrandSold     float64 `json:"brand_sold"`
		TotalSold     float64 `json:"total_sold"`
		WsPercent     float64 `json:"ws_percent"`
	}

	var results []WSBarRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS province bar chart", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Province Bar Chart", "data": results})
}

// WSBarChartArea — grouped bar chart data: WS% per brand per area
func WSBarChartArea(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT pf.area_uuid, SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.area_uuid
		),
		brand_sales AS (
			SELECT pf.area_uuid, pfi.brand_uuid, SUM(pfi.sold) AS brand_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pf.area_uuid, pfi.brand_uuid
		)
		SELECT a.name AS territory_name, a.uuid AS territory_uuid,
			b.name AS brand_name, b.uuid AS brand_uuid,
			bs.brand_sold, COALESCE(ts.total_sold,0) AS total_sold,
			ROUND((bs.brand_sold*100.0/NULLIF(ts.total_sold,0))::numeric,2) AS ws_percent
		FROM brand_sales bs
		INNER JOIN brands b ON b.uuid=bs.brand_uuid
		INNER JOIN areas a ON a.uuid=bs.area_uuid
		LEFT JOIN total_sales ts ON ts.area_uuid=bs.area_uuid
		ORDER BY a.name, ws_percent DESC
	`

	type WSBarRow struct {
		TerritoryName string  `json:"territory_name"`
		TerritoryUUID string  `json:"territory_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		BrandSold     float64 `json:"brand_sold"`
		TotalSold     float64 `json:"total_sold"`
		WsPercent     float64 `json:"ws_percent"`
	}

	var results []WSBarRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS area bar chart", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Area Bar Chart", "data": results})
}

// WSBarChartSubArea — grouped bar chart data: WS% per brand per subarea
func WSBarChartSubArea(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT pf.sub_area_uuid, SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.sub_area_uuid
		),
		brand_sales AS (
			SELECT pf.sub_area_uuid, pfi.brand_uuid, SUM(pfi.sold) AS brand_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pf.sub_area_uuid, pfi.brand_uuid
		)
		SELECT sa.name AS territory_name, sa.uuid AS territory_uuid,
			b.name AS brand_name, b.uuid AS brand_uuid,
			bs.brand_sold, COALESCE(ts.total_sold,0) AS total_sold,
			ROUND((bs.brand_sold*100.0/NULLIF(ts.total_sold,0))::numeric,2) AS ws_percent
		FROM brand_sales bs
		INNER JOIN brands b ON b.uuid=bs.brand_uuid
		INNER JOIN sub_areas sa ON sa.uuid=bs.sub_area_uuid
		LEFT JOIN total_sales ts ON ts.sub_area_uuid=bs.sub_area_uuid
		ORDER BY sa.name, ws_percent DESC
	`

	type WSBarRow struct {
		TerritoryName string  `json:"territory_name"`
		TerritoryUUID string  `json:"territory_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		BrandSold     float64 `json:"brand_sold"`
		TotalSold     float64 `json:"total_sold"`
		WsPercent     float64 `json:"ws_percent"`
	}

	var results []WSBarRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS subarea bar chart", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS SubArea Bar Chart", "data": results})
}

// WSBarChartCommune — grouped bar chart data: WS% per brand per commune
func WSBarChartCommune(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT pf.commune_uuid, SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.commune_uuid
		),
		brand_sales AS (
			SELECT pf.commune_uuid, pfi.brand_uuid, SUM(pfi.sold) AS brand_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pf.commune_uuid, pfi.brand_uuid
		)
		SELECT co.name AS territory_name, co.uuid AS territory_uuid,
			b.name AS brand_name, b.uuid AS brand_uuid,
			bs.brand_sold, COALESCE(ts.total_sold,0) AS total_sold,
			ROUND((bs.brand_sold*100.0/NULLIF(ts.total_sold,0))::numeric,2) AS ws_percent
		FROM brand_sales bs
		INNER JOIN brands b ON b.uuid=bs.brand_uuid
		INNER JOIN communes co ON co.uuid=bs.commune_uuid
		LEFT JOIN total_sales ts ON ts.commune_uuid=bs.commune_uuid
		ORDER BY co.name, ws_percent DESC
	`

	type WSBarRow struct {
		TerritoryName string  `json:"territory_name"`
		TerritoryUUID string  `json:"territory_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		BrandSold     float64 `json:"brand_sold"`
		TotalSold     float64 `json:"total_sold"`
		WsPercent     float64 `json:"ws_percent"`
	}

	var results []WSBarRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS commune bar chart", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Commune Bar Chart", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 — MONTHLY TREND LINE CHART
// ?brand_uuid= (optional) — filter to a single brand
// ─────────────────────────────────────────────────────────────────────────────

// WSLineChartByMonth — WS% per brand per calendar month
func WSLineChartByMonth(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	brand_uuid := c.Query("brand_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH monthly_total AS (
			SELECT
				TO_CHAR(DATE_TRUNC('month', pf.created_at), 'YYYY-MM') AS month,
				SUM(pfi.sold)                                           AS total_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY DATE_TRUNC('month', pf.created_at)
		),
		monthly_brand AS (
			SELECT
				TO_CHAR(DATE_TRUNC('month', pf.created_at), 'YYYY-MM') AS month,
				pfi.brand_uuid,
				SUM(pfi.sold)                                           AS brand_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND (@brand_uuid    = '' OR pfi.brand_uuid   = @brand_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY DATE_TRUNC('month', pf.created_at), pfi.brand_uuid
		)
		SELECT
			mb.month,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			mb.brand_sold,
			COALESCE(mt.total_sold, 0)                                           AS total_sold,
			ROUND((mb.brand_sold * 100.0 /
			       NULLIF(mt.total_sold, 0))::numeric, 2)                        AS ws_percent
		FROM monthly_brand mb
		INNER JOIN brands b ON b.uuid = mb.brand_uuid
		LEFT  JOIN monthly_total mt ON mt.month = mb.month
		ORDER BY mb.month, ws_percent DESC
	`

	type WSMonthRow struct {
		Month     string  `json:"month"`
		BrandName string  `json:"brand_name"`
		BrandUUID string  `json:"brand_uuid"`
		BrandSold float64 `json:"brand_sold"`
		TotalSold float64 `json:"total_sold"`
		WsPercent float64 `json:"ws_percent"`
	}

	var results []WSMonthRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "brand_uuid": brand_uuid,
		"start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS monthly trend", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Monthly Trend", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 — POWER ANALYTICS
// ─────────────────────────────────────────────────────────────────────────────

// WSSummaryKPI — single KPI card: overall WS% + brand count + total sold
func WSSummaryKPI(c *fiber.Ctx) error {
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
		WITH base AS (
			SELECT pfi.brand_uuid, pfi.sold, pfi.counter
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		)
		SELECT
			COUNT(DISTINCT brand_uuid)                                           AS total_brands,
			SUM(sold)                                                            AS grand_total_sold,
			SUM(CASE WHEN counter > 0 THEN sold ELSE 0 END)                     AS weighted_sold,
			ROUND((SUM(CASE WHEN counter > 0 THEN sold ELSE 0 END) * 100.0 /
			       NULLIF(SUM(sold), 0))::numeric, 2)                            AS overall_ws_percent,
			COUNT(DISTINCT CASE WHEN counter > 0 THEN brand_uuid END)           AS brands_with_ws
		FROM base
	`

	type WSKpi struct {
		TotalBrands      int64   `json:"total_brands"`
		GrandTotalSold   float64 `json:"grand_total_sold"`
		WeightedSold     float64 `json:"weighted_sold"`
		OverallWsPercent float64 `json:"overall_ws_percent"`
		BrandsWithWS     int64   `json:"brands_with_ws"`
	}

	var result WSKpi
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&result).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS KPI", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Summary KPI", "data": result})
}

// WSBrandRanking — brands ranked by WS%, with WS–ND gap
func WSBrandRanking(c *fiber.Ctx) error {
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
		WITH total AS (
			SELECT SUM(pfi.sold) AS total_sold, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		),
		brand_agg AS (
			SELECT pfi.brand_uuid, SUM(pfi.sold) AS brand_sold, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.name AS brand_name, b.uuid AS brand_uuid,
			ba.brand_sold, t.total_sold, ba.nd_pos, t.total_pos,
			ROUND((ba.brand_sold*100.0/NULLIF(t.total_sold,0))::numeric,2) AS ws_percent,
			ROUND((ba.nd_pos    *100.0/NULLIF(t.total_pos, 0))::numeric,2) AS nd_percent,
			ROUND(((ba.brand_sold*100.0/NULLIF(t.total_sold,0)) -
			       (ba.nd_pos    *100.0/NULLIF(t.total_pos, 0)))::numeric,2) AS ws_nd_gap,
			RANK() OVER (ORDER BY (ba.brand_sold*100.0/NULLIF(t.total_sold,0)) DESC) AS rank
		FROM brand_agg ba
		CROSS JOIN total t
		INNER JOIN brands b ON b.uuid = ba.brand_uuid
		ORDER BY ws_percent DESC
	`

	type WSRankRow struct {
		BrandName string  `json:"brand_name"`
		BrandUUID string  `json:"brand_uuid"`
		BrandSold float64 `json:"brand_sold"`
		TotalSold float64 `json:"total_sold"`
		NdPos     int64   `json:"nd_pos"`
		TotalPos  int64   `json:"total_pos"`
		WsPercent float64 `json:"ws_percent"`
		NdPercent float64 `json:"nd_percent"`
		WsNdGap   float64 `json:"ws_nd_gap"`
		Rank      int64   `json:"rank"`
	}

	var results []WSRankRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS brand ranking", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Brand Ranking", "data": results})
}

// WSGapAnalysis — bucket brands into 3 zones: Strong (≥66%), Mid (33-66%), Weak (<33%)
func WSGapAnalysis(c *fiber.Ctx) error {
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
		WITH total_sales AS (
			SELECT SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		),
		brand_ws AS (
			SELECT pfi.brand_uuid,
				ROUND((SUM(pfi.sold)*100.0/NULLIF((SELECT total_sold FROM total_sales),0))::numeric,2) AS ws_percent
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT b.name AS brand_name, b.uuid AS brand_uuid, bw.ws_percent,
			CASE WHEN bw.ws_percent >= 66 THEN 'strong'
			     WHEN bw.ws_percent >= 33 THEN 'mid'
			     ELSE 'weak' END AS zone
		FROM brand_ws bw INNER JOIN brands b ON b.uuid = bw.brand_uuid
		ORDER BY bw.ws_percent DESC
	`

	type WSGapRow struct {
		BrandName string  `json:"brand_name"`
		BrandUUID string  `json:"brand_uuid"`
		WsPercent float64 `json:"ws_percent"`
		Zone      string  `json:"zone"`
	}

	var rows []WSGapRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&rows).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS gap analysis", "error": err.Error()})
	}

	zoneSummary := map[string]int{"strong": 0, "mid": 0, "weak": 0}
	for _, r := range rows {
		zoneSummary[r.Zone]++
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Gap Analysis", "summary": zoneSummary, "data": rows})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 — ADVANCED ANALYTICS
// ─────────────────────────────────────────────────────────────────────────────

// WSHeatmap — brand × territory WS% matrix
// ?level=province|area|subarea|commune  (default: province)
func WSHeatmap(c *fiber.Ctx) error {
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

	var territoryJoin, territoryGroup, territorySel string
	switch level {
	case "area":
		territoryJoin = "INNER JOIN areas t ON t.uuid = pf.area_uuid"
		territoryGroup = "pf.area_uuid"
		territorySel = "pf.area_uuid AS territory_uuid, t.name AS territory_name"
	case "subarea":
		territoryJoin = "INNER JOIN sub_areas t ON t.uuid = pf.sub_area_uuid"
		territoryGroup = "pf.sub_area_uuid"
		territorySel = "pf.sub_area_uuid AS territory_uuid, t.name AS territory_name"
	case "commune":
		territoryJoin = "INNER JOIN communes t ON t.uuid = pf.commune_uuid"
		territoryGroup = "pf.commune_uuid"
		territorySel = "pf.commune_uuid AS territory_uuid, t.name AS territory_name"
	default:
		territoryJoin = "INNER JOIN provinces t ON t.uuid = pf.province_uuid"
		territoryGroup = "pf.province_uuid"
		territorySel = "pf.province_uuid AS territory_uuid, t.name AS territory_name"
	}

	sqlQuery := `
		WITH total_sales AS (
			SELECT ` + territoryGroup + ` AS territory_uuid, SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY ` + territoryGroup + `
		),
		brand_sales AS (
			SELECT ` + territorySel + `, pfi.brand_uuid, SUM(pfi.sold) AS brand_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			` + territoryJoin + `
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY ` + territoryGroup + `, t.name, pfi.brand_uuid
		)
		SELECT bs.territory_uuid, bs.territory_name,
			b.uuid AS brand_uuid, b.name AS brand_name,
			bs.brand_sold, COALESCE(ts.total_sold,0) AS total_sold,
			ROUND((bs.brand_sold*100.0/NULLIF(ts.total_sold,0))::numeric,2) AS ws_percent
		FROM brand_sales bs
		INNER JOIN brands b ON b.uuid = bs.brand_uuid
		LEFT  JOIN total_sales ts ON ts.territory_uuid = bs.territory_uuid
		ORDER BY bs.territory_name, ws_percent DESC
	`

	type WSHeatRow struct {
		TerritoryUUID string  `json:"territory_uuid"`
		TerritoryName string  `json:"territory_name"`
		BrandUUID     string  `json:"brand_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandSold     float64 `json:"brand_sold"`
		TotalSold     float64 `json:"total_sold"`
		WsPercent     float64 `json:"ws_percent"`
	}

	var results []WSHeatRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS heatmap", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Heatmap", "level": level, "data": results})
}

// WSEvolution — period-over-period WS% comparison (current vs previous window of same length)
func WSEvolution(c *fiber.Ctx) error {
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
		WITH curr_total AS (
			SELECT SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		),
		prev_total AS (
			SELECT SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN
			        (@start_date::date - (@end_date::date - @start_date::date + 1))
			    AND (@start_date::date - 1)
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		),
		curr_brand AS (
			SELECT pfi.brand_uuid, SUM(pfi.sold) AS brand_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		),
		prev_brand AS (
			SELECT pfi.brand_uuid, SUM(pfi.sold) AS brand_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN
			        (@start_date::date - (@end_date::date - @start_date::date + 1))
			    AND (@start_date::date - 1)
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT b.name AS brand_name, b.uuid AS brand_uuid,
			ROUND((COALESCE(cb.brand_sold,0)*100.0/NULLIF((SELECT total_sold FROM curr_total),0))::numeric,2) AS curr_ws_percent,
			ROUND((COALESCE(pb.brand_sold,0)*100.0/NULLIF((SELECT total_sold FROM prev_total),0))::numeric,2) AS prev_ws_percent,
			ROUND(((COALESCE(cb.brand_sold,0)*100.0/NULLIF((SELECT total_sold FROM curr_total),0)) -
			       (COALESCE(pb.brand_sold,0)*100.0/NULLIF((SELECT total_sold FROM prev_total),0)))::numeric,2) AS delta_ws
		FROM brands b
		LEFT JOIN curr_brand cb ON cb.brand_uuid = b.uuid
		LEFT JOIN prev_brand pb ON pb.brand_uuid = b.uuid
		WHERE cb.brand_uuid IS NOT NULL OR pb.brand_uuid IS NOT NULL
		ORDER BY curr_ws_percent DESC
	`

	type WSEvoRow struct {
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		CurrWsPercent float64 `json:"curr_ws_percent"`
		PrevWsPercent float64 `json:"prev_ws_percent"`
		DeltaWs       float64 `json:"delta_ws"`
	}

	var results []WSEvoRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS evolution", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS Evolution", "data": results})
}

// WSvsNDCorrelation — WS × ND quadrant matrix per brand
// ?threshold=50  (default 50)
func WSvsNDCorrelation(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	threshold := 50.0
	_ = math.IsNaN(threshold) // keep math import used

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH total AS (
			SELECT SUM(pfi.sold) AS total_sold, COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		),
		brand_agg AS (
			SELECT pfi.brand_uuid, SUM(pfi.sold) AS brand_sold, COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT b.name AS brand_name, b.uuid AS brand_uuid,
			ba.brand_sold, t.total_sold, ba.nd_pos, t.total_pos,
			ROUND((ba.brand_sold*100.0/NULLIF(t.total_sold,0))::numeric,2) AS ws_percent,
			ROUND((ba.nd_pos    *100.0/NULLIF(t.total_pos, 0))::numeric,2) AS nd_percent,
			CASE
				WHEN (ba.brand_sold*100.0/NULLIF(t.total_sold,0)) >= @threshold
				 AND (ba.nd_pos    *100.0/NULLIF(t.total_pos, 0)) >= @threshold THEN 'leader'
				WHEN (ba.brand_sold*100.0/NULLIF(t.total_sold,0)) >= @threshold
				 AND (ba.nd_pos    *100.0/NULLIF(t.total_pos, 0)) <  @threshold THEN 'niche'
				WHEN (ba.brand_sold*100.0/NULLIF(t.total_sold,0)) <  @threshold
				 AND (ba.nd_pos    *100.0/NULLIF(t.total_pos, 0)) >= @threshold THEN 'volume'
				ELSE 'laggard'
			END AS segment
		FROM brand_agg ba CROSS JOIN total t INNER JOIN brands b ON b.uuid = ba.brand_uuid
		ORDER BY ws_percent DESC
	`

	type WSCorrRow struct {
		BrandName string  `json:"brand_name"`
		BrandUUID string  `json:"brand_uuid"`
		BrandSold float64 `json:"brand_sold"`
		TotalSold float64 `json:"total_sold"`
		NdPos     int64   `json:"nd_pos"`
		TotalPos  int64   `json:"total_pos"`
		WsPercent float64 `json:"ws_percent"`
		NdPercent float64 `json:"nd_percent"`
		Segment   string  `json:"segment"`
	}

	var results []WSCorrRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "start_date": start_date, "end_date": end_date,
		"threshold": threshold,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS vs ND correlation", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS vs ND Correlation", "threshold": threshold, "data": results})
}

// WSPosDrillDown — POS-level WS deep-dive for a specific brand
// ?brand_uuid= (required)
func WSPosDrillDown(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	brand_uuid := c.Query("brand_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	if brand_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "brand_uuid is required for POS drill-down",
		})
	}

	sqlQuery := `
		WITH pos_total AS (
			SELECT pf.uuid AS pos_form_uuid, SUM(pfi.sold) AS pos_sold
			FROM pos_form_items pfi INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.uuid
		)
		SELECT
			p.name                                                               AS pos_name,
			p.uuid                                                               AS pos_uuid,
			pf.uuid                                                              AS pos_form_uuid,
			b.name                                                               AS brand_name,
			pfi.sold                                                             AS brand_sold,
			COALESCE(pt.pos_sold, 0)                                             AS pos_total_sold,
			pfi.counter,
			pfi.number_farde,
			ROUND((pfi.sold*100.0/NULLIF(pt.pos_sold,0))::numeric,2)            AS ws_contribution,
			pf.created_at::date                                                  AS visit_date
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN pos        p ON p.uuid = pf.pos_uuid
		INNER JOIN brands     b ON b.uuid = pfi.brand_uuid
		LEFT  JOIN pos_total pt ON pt.pos_form_uuid = pf.uuid
		WHERE pf.country_uuid = @country_uuid
		  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
		  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
		  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
		  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
		  AND pfi.brand_uuid = @brand_uuid
		  AND pf.created_at BETWEEN @start_date AND @end_date
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		ORDER BY ws_contribution DESC, visit_date DESC
	`

	type WSDrillRow struct {
		PosName        string  `json:"pos_name"`
		PosUUID        string  `json:"pos_uuid"`
		PosFormUUID    string  `json:"pos_form_uuid"`
		BrandName      string  `json:"brand_name"`
		BrandSold      float64 `json:"brand_sold"`
		PosTotalSold   float64 `json:"pos_total_sold"`
		Counter        int     `json:"counter"`
		NumberFarde    float64 `json:"number_farde"`
		WsContribution float64 `json:"ws_contribution"`
		VisitDate      string  `json:"visit_date"`
	}

	var results []WSDrillRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid, "province_uuid": province_uuid,
		"area_uuid": area_uuid, "sub_area_uuid": sub_area_uuid,
		"commune_uuid": commune_uuid, "brand_uuid": brand_uuid,
		"start_date": start_date, "end_date": end_date,
	}).Scan(&results).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"status": "error", "message": "Failed to fetch WS POS drill-down", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WS POS Drill-Down", "data": results})
}


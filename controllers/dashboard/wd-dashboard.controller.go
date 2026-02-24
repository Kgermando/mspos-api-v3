package dashboard

import (
	"math"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════════╗
// ║         WEIGHTED DISTRIBUTION (WD) DASHBOARD — VOLUME-WEIGHTED PRESENCE    ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  Weighted Distribution % =  SUM(fardes at POS where brand counter > 0)     ║
// ║                            ─────────────────────────────────────────────── ║
// ║                            SUM(total fardes at ALL visited POS) × 100      ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  Unlike Numeric Distribution (counts POS), WD weights each POS by its      ║
// ║  volume importance — a brand present in high-volume outlets scores higher.  ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  SECTION 1 — TABLE VIEWS    : Province / Area / SubArea / Commune           ║
// ║  SECTION 2 — BAR CHARTS     : Province / Area / SubArea / Commune           ║
// ║  SECTION 3 — TREND CHART    : WD% by Brand per Month                        ║
// ║  SECTION 4 — POWER ANALYTICS: Summary KPI / Brand Ranking / Gap Analysis    ║
// ║  SECTION 5 — ADVANCED       : Heatmap / Evolution / WD vs ND Correlation    ║
// ╚══════════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 — TABLE VIEWS
//
//	Each row = (territory × brand) with:
//	  brand_volume   — total fardes at POS where brand counter > 0
//	  total_volume   — total fardes across ALL visited POS in the territory
//	  wd_percent     — brand_volume / total_volume × 100
//	  nd_pos         — distinct POS where brand counter > 0
//	  total_pos      — distinct POS visited
//	  nd_percent     — nd_pos / total_pos × 100  (for comparison)
// ─────────────────────────────────────────────────────────────────────────────

// WDTableViewProvince — WD% breakdown per brand at Province level
func WDTableViewProvince(c *fiber.Ctx) error {
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
		WITH total_vol AS (
			-- Total fardes across ALL visited POS per province (the denominator)
			SELECT
				pf.province_uuid,
				SUM(pfi.number_farde)        AS total_volume,
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
		brand_vol AS (
			-- Fardes per brand at POS where that brand's counter > 0 (the numerator)
			SELECT
				pf.province_uuid,
				pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_volume,
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
			bv.brand_volume,
			COALESCE(tv.total_volume, 0)                                         AS total_volume,
			bv.nd_pos,
			COALESCE(tv.total_pos, 0)                                            AS total_pos,
			ROUND((bv.brand_volume * 100.0 /
			       NULLIF(tv.total_volume, 0))::numeric, 2)                      AS wd_percent,
			ROUND((bv.nd_pos * 100.0 /
			       NULLIF(tv.total_pos, 0))::numeric, 2)                         AS nd_percent
		FROM brand_vol bv
		INNER JOIN brands    b  ON b.uuid  = bv.brand_uuid
		INNER JOIN provinces pr ON pr.uuid = bv.province_uuid
		LEFT  JOIN total_vol tv ON tv.province_uuid = bv.province_uuid
		ORDER BY pr.name, wd_percent DESC
	`

	type WDRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandVolume    float64 `json:"brand_volume"`
		TotalVolume    float64 `json:"total_volume"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		WdPercent      float64 `json:"wd_percent"`
		NdPercent      float64 `json:"nd_percent"`
	}

	var results []WDRow
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
			"status": "error", "message": "Failed to fetch WD province table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD Province Table", "data": results})
}

// WDTableViewArea — WD% breakdown per brand at Area level
func WDTableViewArea(c *fiber.Ctx) error {
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
		WITH total_vol AS (
			SELECT
				pf.area_uuid,
				SUM(pfi.number_farde)        AS total_volume,
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
		brand_vol AS (
			SELECT
				pf.area_uuid,
				pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_volume,
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
			bv.brand_volume,
			COALESCE(tv.total_volume, 0)                                         AS total_volume,
			bv.nd_pos,
			COALESCE(tv.total_pos, 0)                                            AS total_pos,
			ROUND((bv.brand_volume * 100.0 /
			       NULLIF(tv.total_volume, 0))::numeric, 2)                      AS wd_percent,
			ROUND((bv.nd_pos * 100.0 /
			       NULLIF(tv.total_pos, 0))::numeric, 2)                         AS nd_percent
		FROM brand_vol bv
		INNER JOIN brands b ON b.uuid = bv.brand_uuid
		INNER JOIN areas  a ON a.uuid = bv.area_uuid
		LEFT  JOIN total_vol tv ON tv.area_uuid = bv.area_uuid
		ORDER BY a.name, wd_percent DESC
	`

	type WDRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandVolume    float64 `json:"brand_volume"`
		TotalVolume    float64 `json:"total_volume"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		WdPercent      float64 `json:"wd_percent"`
		NdPercent      float64 `json:"nd_percent"`
	}

	var results []WDRow
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
			"status": "error", "message": "Failed to fetch WD area table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD Area Table", "data": results})
}

// WDTableViewSubArea — WD% breakdown per brand at SubArea level
func WDTableViewSubArea(c *fiber.Ctx) error {
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
		WITH total_vol AS (
			SELECT
				pf.sub_area_uuid,
				SUM(pfi.number_farde)        AS total_volume,
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
		brand_vol AS (
			SELECT
				pf.sub_area_uuid,
				pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_volume,
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
			bv.brand_volume,
			COALESCE(tv.total_volume, 0)                                         AS total_volume,
			bv.nd_pos,
			COALESCE(tv.total_pos, 0)                                            AS total_pos,
			ROUND((bv.brand_volume * 100.0 /
			       NULLIF(tv.total_volume, 0))::numeric, 2)                      AS wd_percent,
			ROUND((bv.nd_pos * 100.0 /
			       NULLIF(tv.total_pos, 0))::numeric, 2)                         AS nd_percent
		FROM brand_vol bv
		INNER JOIN brands    b  ON b.uuid  = bv.brand_uuid
		INNER JOIN sub_areas sa ON sa.uuid = bv.sub_area_uuid
		LEFT  JOIN total_vol tv ON tv.sub_area_uuid = bv.sub_area_uuid
		ORDER BY sa.name, wd_percent DESC
	`

	type WDRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandVolume    float64 `json:"brand_volume"`
		TotalVolume    float64 `json:"total_volume"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		WdPercent      float64 `json:"wd_percent"`
		NdPercent      float64 `json:"nd_percent"`
	}

	var results []WDRow
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
			"status": "error", "message": "Failed to fetch WD subarea table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD SubArea Table", "data": results})
}

// WDTableViewCommune — WD% breakdown per brand at Commune level
func WDTableViewCommune(c *fiber.Ctx) error {
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
		WITH total_vol AS (
			SELECT
				pf.commune_uuid,
				SUM(pfi.number_farde)        AS total_volume,
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
		brand_vol AS (
			SELECT
				pf.commune_uuid,
				pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_volume,
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
			cm.name                                                              AS territory_name,
			cm.uuid                                                              AS territory_uuid,
			'commune'                                                            AS territory_level,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			bv.brand_volume,
			COALESCE(tv.total_volume, 0)                                         AS total_volume,
			bv.nd_pos,
			COALESCE(tv.total_pos, 0)                                            AS total_pos,
			ROUND((bv.brand_volume * 100.0 /
			       NULLIF(tv.total_volume, 0))::numeric, 2)                      AS wd_percent,
			ROUND((bv.nd_pos * 100.0 /
			       NULLIF(tv.total_pos, 0))::numeric, 2)                         AS nd_percent
		FROM brand_vol bv
		INNER JOIN brands   b  ON b.uuid  = bv.brand_uuid
		INNER JOIN communes cm ON cm.uuid = bv.commune_uuid
		LEFT  JOIN total_vol tv ON tv.commune_uuid = bv.commune_uuid
		ORDER BY cm.name, wd_percent DESC
	`

	type WDRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandVolume    float64 `json:"brand_volume"`
		TotalVolume    float64 `json:"total_volume"`
		NdPos          int64   `json:"nd_pos"`
		TotalPos       int64   `json:"total_pos"`
		WdPercent      float64 `json:"wd_percent"`
		NdPercent      float64 `json:"nd_percent"`
	}

	var results []WDRow
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
			"status": "error", "message": "Failed to fetch WD commune table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD Commune Table", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 — BAR CHARTS
// Each element = one brand per territory, with wd_percent for the bar height.
// ─────────────────────────────────────────────────────────────────────────────

func wdBarChartSQL(geoCol, joinTable, levelLabel string) string {
	return `
		WITH total_vol AS (
			SELECT pf.` + geoCol + `, SUM(pfi.number_farde) AS total_volume
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.` + geoCol + `
		),
		brand_vol AS (
			SELECT pf.` + geoCol + `, pfi.brand_uuid,
				SUM(pfi.number_farde) AS brand_volume
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
			t.name                                                               AS territory_name,
			t.uuid                                                               AS territory_uuid,
			'` + levelLabel + `'                                                 AS territory_level,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			bv.brand_volume,
			COALESCE(tv.total_volume, 0)                                         AS total_volume,
			ROUND((bv.brand_volume * 100.0 /
			       NULLIF(tv.total_volume, 0))::numeric, 2)                      AS wd_percent
		FROM brand_vol bv
		INNER JOIN brands        b ON b.uuid = bv.brand_uuid
		INNER JOIN ` + joinTable + ` t ON t.uuid = bv.` + geoCol + `
		LEFT  JOIN total_vol    tv ON tv.` + geoCol + ` = bv.` + geoCol + `
		ORDER BY t.name, wd_percent DESC
	`
}

type WDBarRow struct {
	TerritoryName  string  `json:"territory_name"`
	TerritoryUUID  string  `json:"territory_uuid"`
	TerritoryLevel string  `json:"territory_level"`
	BrandName      string  `json:"brand_name"`
	BrandUUID      string  `json:"brand_uuid"`
	BrandVolume    float64 `json:"brand_volume"`
	TotalVolume    float64 `json:"total_volume"`
	WdPercent      float64 `json:"wd_percent"`
}

func wdBarChartHandler(c *fiber.Ctx, geoCol, joinTable, levelLabel, msgLabel string) error {
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

	var results []WDBarRow
	err := db.Raw(wdBarChartSQL(geoCol, joinTable, levelLabel), map[string]interface{}{
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
			"status": "error", "message": "Failed to fetch WD bar chart", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": msgLabel, "data": results})
}

// WDBarChartProvince — grouped brand WD% bars at Province level
func WDBarChartProvince(c *fiber.Ctx) error {
	return wdBarChartHandler(c, "province_uuid", "provinces", "province", "WD Bar Chart — Province")
}

// WDBarChartArea — grouped brand WD% bars at Area level
func WDBarChartArea(c *fiber.Ctx) error {
	return wdBarChartHandler(c, "area_uuid", "areas", "area", "WD Bar Chart — Area")
}

// WDBarChartSubArea — grouped brand WD% bars at SubArea level
func WDBarChartSubArea(c *fiber.Ctx) error {
	return wdBarChartHandler(c, "sub_area_uuid", "sub_areas", "subarea", "WD Bar Chart — SubArea")
}

// WDBarChartCommune — grouped brand WD% bars at Commune level
func WDBarChartCommune(c *fiber.Ctx) error {
	return wdBarChartHandler(c, "commune_uuid", "communes", "commune", "WD Bar Chart — Commune")
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 — MONTHLY TREND LINE CHART
//
//	One data-point per (brand × month):
//	  month        — YYYY-MM
//	  wd_percent   — WD% for that brand in that month
// ─────────────────────────────────────────────────────────────────────────────

// WDLineChartByMonth — WD% trend per brand per month
func WDLineChartByMonth(c *fiber.Ctx) error {
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
		WITH monthly_total AS (
			SELECT
				TO_CHAR(pf.created_at, 'YYYY-MM') AS month,
				SUM(pfi.number_farde)              AS total_volume
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY month
		),
		monthly_brand AS (
			SELECT
				TO_CHAR(pf.created_at, 'YYYY-MM') AS month,
				pfi.brand_uuid,
				SUM(pfi.number_farde)              AS brand_volume
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
			mb.month,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			mb.brand_volume,
			COALESCE(mt.total_volume, 0)                                         AS total_volume,
			ROUND((mb.brand_volume * 100.0 /
			       NULLIF(mt.total_volume, 0))::numeric, 2)                      AS wd_percent
		FROM monthly_brand mb
		INNER JOIN brands        b  ON b.uuid  = mb.brand_uuid
		LEFT  JOIN monthly_total mt ON mt.month = mb.month
		ORDER BY mb.month, b.name
	`

	type TrendRow struct {
		Month       string  `json:"month"`
		BrandName   string  `json:"brand_name"`
		BrandUUID   string  `json:"brand_uuid"`
		BrandVolume float64 `json:"brand_volume"`
		TotalVolume float64 `json:"total_volume"`
		WdPercent   float64 `json:"wd_percent"`
	}

	var results []TrendRow
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
			"status": "error", "message": "Failed to fetch WD trend by month", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD Monthly Trend", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 — POWER ANALYTICS
// ─────────────────────────────────────────────────────────────────────────────

// WDSummaryKPI — executive-level KPI card
//
//	Returns:
//	  total_volume        — total fardes across all visited POS
//	  total_pos           — distinct POS visited
//	  avg_wd_percent      — average WD% across all brands
//	  best_brand_name     — brand with highest WD%
//	  best_brand_wd       — WD% of best brand
//	  worst_brand_name    — brand with lowest WD%
//	  worst_brand_wd      — WD% of worst brand
//	  brands_above_50pct  — count of brands with WD% ≥ 50
func WDSummaryKPI(c *fiber.Ctx) error {
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
		WITH total_vol AS (
			SELECT
				SUM(pfi.number_farde)        AS total_volume,
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
		),
		brand_wd AS (
			SELECT
				pfi.brand_uuid,
				SUM(pfi.number_farde) AS brand_volume
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
		),
		wd_pct AS (
			SELECT
				bw.brand_uuid,
				ROUND((bw.brand_volume * 100.0 /
				       NULLIF((SELECT total_volume FROM total_vol), 0))::numeric, 2) AS wd_percent
			FROM brand_wd bw
		)
		SELECT
			(SELECT total_volume FROM total_vol)                                  AS total_volume,
			(SELECT total_pos    FROM total_vol)                                  AS total_pos,
			ROUND(AVG(wp.wd_percent)::numeric, 2)                                 AS avg_wd_percent,
			(SELECT b.name FROM wd_pct wp2
			 INNER JOIN brands b ON b.uuid = wp2.brand_uuid
			 ORDER BY wp2.wd_percent DESC LIMIT 1)                                AS best_brand_name,
			(SELECT wp2.wd_percent FROM wd_pct wp2 ORDER BY wp2.wd_percent DESC LIMIT 1) AS best_brand_wd,
			(SELECT b.name FROM wd_pct wp3
			 INNER JOIN brands b ON b.uuid = wp3.brand_uuid
			 ORDER BY wp3.wd_percent ASC LIMIT 1)                                AS worst_brand_name,
			(SELECT wp3.wd_percent FROM wd_pct wp3 ORDER BY wp3.wd_percent ASC LIMIT 1)  AS worst_brand_wd,
			COUNT(CASE WHEN wp.wd_percent >= 50 THEN 1 END)                       AS brands_above_50pct
		FROM wd_pct wp
	`

	type KPIRow struct {
		TotalVolume      float64 `json:"total_volume"`
		TotalPos         int64   `json:"total_pos"`
		AvgWdPercent     float64 `json:"avg_wd_percent"`
		BestBrandName    string  `json:"best_brand_name"`
		BestBrandWd      float64 `json:"best_brand_wd"`
		WorstBrandName   string  `json:"worst_brand_name"`
		WorstBrandWd     float64 `json:"worst_brand_wd"`
		BrandsAbove50Pct int64   `json:"brands_above_50pct"`
	}

	var result KPIRow
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
			"status": "error", "message": "Failed to fetch WD summary KPI", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD Summary KPI", "data": result})
}

// WDBrandRanking — brands ranked by WD% (highest first)
//
//	Each row also carries nd_percent so the caller can compare WD vs ND.
//	A WD >> ND means the brand is concentrated in high-volume outlets.
//	A WD << ND means the brand is spread thin across low-volume outlets.
func WDBrandRanking(c *fiber.Ctx) error {
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
		Rank        int64   `json:"rank"`
		BrandName   string  `json:"brand_name"`
		BrandUUID   string  `json:"brand_uuid"`
		BrandVolume float64 `json:"brand_volume"`
		TotalVolume float64 `json:"total_volume"`
		WdPercent   float64 `json:"wd_percent"`
		NdPos       int64   `json:"nd_pos"`
		TotalPos    int64   `json:"total_pos"`
		NdPercent   float64 `json:"nd_percent"`
		WdNdGap     float64 `json:"wd_nd_gap"` // WD% - ND%: positive = quality outlets, negative = thin spread
	}

	sqlQuery := `
		WITH total_stats AS (
			SELECT
				SUM(pfi.number_farde)        AS total_volume,
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
		),
		brand_stats AS (
			SELECT
				pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_volume,
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
			GROUP BY pfi.brand_uuid
		)
		SELECT
			ROW_NUMBER() OVER (ORDER BY
				(bs.brand_volume * 100.0 / NULLIF((SELECT total_volume FROM total_stats), 0)) DESC
			)                                                                    AS rank,
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			bs.brand_volume,
			(SELECT total_volume FROM total_stats)                               AS total_volume,
			ROUND((bs.brand_volume * 100.0 /
			       NULLIF((SELECT total_volume FROM total_stats), 0))::numeric, 2) AS wd_percent,
			bs.nd_pos,
			(SELECT total_pos FROM total_stats)                                  AS total_pos,
			ROUND((bs.nd_pos * 100.0 /
			       NULLIF((SELECT total_pos FROM total_stats), 0))::numeric, 2)  AS nd_percent,
			ROUND((bs.brand_volume * 100.0 /
			       NULLIF((SELECT total_volume FROM total_stats), 0) -
			       bs.nd_pos * 100.0 /
			       NULLIF((SELECT total_pos FROM total_stats), 0))::numeric, 2)  AS wd_nd_gap
		FROM brand_stats bs
		INNER JOIN brands b ON b.uuid = bs.brand_uuid
		ORDER BY wd_percent DESC
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
			"status": "error", "message": "Failed to fetch WD brand ranking", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD Brand Ranking", "data": results})
}

// WDGapAnalysis — volume-weighted opportunity funnel per brand
//
//	Zone A — volume at POS where brand IS present  (brand_volume)
//	Zone B — volume at visited POS where brand is NOT present (visited_gap_volume)
//	Zone C — estimated unreached volume (universe_gap_volume) — not calculable
//	         without individual-POS volume data; set to 0 for now.
//
//	opportunity_pct = visited_gap_volume / total_volume × 100
func WDGapAnalysis(c *fiber.Ctx) error {
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
		BrandName        string  `json:"brand_name"`
		BrandUUID        string  `json:"brand_uuid"`
		BrandVolume      float64 `json:"brand_volume"`
		VisitedGapVolume float64 `json:"visited_gap_volume"`
		TotalVolume      float64 `json:"total_volume"`
		WdPercent        float64 `json:"wd_percent"`
		OpportunityPct   float64 `json:"opportunity_pct"`
	}

	sqlQuery := `
		WITH total_vol AS (
			SELECT SUM(pfi.number_farde) AS total_volume
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		),
		brand_vol AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_volume
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
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			bv.brand_volume,
			GREATEST((SELECT total_volume FROM total_vol) - bv.brand_volume, 0) AS visited_gap_volume,
			(SELECT total_volume FROM total_vol)                                 AS total_volume,
			ROUND((bv.brand_volume * 100.0 /
			       NULLIF((SELECT total_volume FROM total_vol), 0))::numeric, 2) AS wd_percent,
			ROUND((GREATEST((SELECT total_volume FROM total_vol) - bv.brand_volume, 0) * 100.0 /
			       NULLIF((SELECT total_volume FROM total_vol), 0))::numeric, 2) AS opportunity_pct
		FROM brand_vol bv
		INNER JOIN brands b ON b.uuid = bv.brand_uuid
		ORDER BY wd_percent DESC
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
			"status": "error", "message": "Failed to fetch WD gap analysis", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD Gap Analysis", "data": results})
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 — ADVANCED ANALYTICS
// ─────────────────────────────────────────────────────────────────────────────

// WDHeatmap — Brand × Territory WD% matrix
//
// Response shape:
//
//	{
//	  brands:      [{uuid, name}, ...],
//	  territories: [{uuid, name}, ...],
//	  matrix:      [[wd_percent, ...], ...]   // matrix[brandIndex][territoryIndex]
//	}
//
// Query param ?level=province|area|subarea|commune  (default: province)
func WDHeatmap(c *fiber.Ctx) error {
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
		BrandVolume   float64 `json:"brand_volume"`
		TotalVolume   float64 `json:"total_volume"`
		WdPercent     float64 `json:"wd_percent"`
	}

	sqlQuery := `
		WITH total_vol AS (
			SELECT pf.` + geoCol + `, SUM(pfi.number_farde) AS total_volume
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.` + geoCol + `
		),
		brand_vol AS (
			SELECT pf.` + geoCol + `, pfi.brand_uuid, SUM(pfi.number_farde) AS brand_volume
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
			b.name                                                               AS brand_name,
			b.uuid                                                               AS brand_uuid,
			t.name                                                               AS territory_name,
			t.uuid                                                               AS territory_uuid,
			COALESCE(bv.brand_volume, 0)                                         AS brand_volume,
			COALESCE(tv.total_volume, 0)                                         AS total_volume,
			ROUND((COALESCE(bv.brand_volume, 0) * 100.0 /
			       NULLIF(COALESCE(tv.total_volume, 0), 0))::numeric, 2)         AS wd_percent
		FROM brand_vol bv
		INNER JOIN brands        b ON b.uuid = bv.brand_uuid
		INNER JOIN ` + joinTable + ` t ON t.uuid = bv.` + geoCol + `
		LEFT  JOIN total_vol    tv ON tv.` + geoCol + ` = bv.` + geoCol + `
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
			"status": "error", "message": "Failed to fetch WD heatmap", "error": err.Error(),
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
		matrix[brandIndex[r.BrandUUID]][terrIndex[r.TerritoryUUID]] = r.WdPercent
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "WD Heatmap — Brand × Territory",
		"level":   level,
		"data": fiber.Map{
			"brands":      brands,
			"territories": territories,
			"matrix":      matrix,
		},
	})
}

// WDEvolution — Period-over-Period (PoP) WD% comparison.
// Compares current window (start_date → end_date) with the preceding equal-length window.
//
//	current_wd_percent  — WD% in selected period
//	previous_wd_percent — WD% in prior equal-length period
//	delta               — current - previous (pp points)
//	trend               — "up" | "down" | "stable"
func WDEvolution(c *fiber.Ctx) error {
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
		BrandName           string  `json:"brand_name"`
		BrandUUID           string  `json:"brand_uuid"`
		CurrentVolume       float64 `json:"current_volume"`
		PreviousVolume      float64 `json:"previous_volume"`
		CurrentTotalVolume  float64 `json:"current_total_volume"`
		PreviousTotalVolume float64 `json:"previous_total_volume"`
		CurrentWdPercent    float64 `json:"current_wd_percent"`
		PreviousWdPercent   float64 `json:"previous_wd_percent"`
		Delta               float64 `json:"delta"`
		Trend               string  `json:"trend"`
	}

	sqlQuery := `
		WITH curr_total AS (
			SELECT SUM(pfi.number_farde) AS total_volume
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		),
		prev_total AS (
			SELECT SUM(pfi.number_farde) AS total_volume
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
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		),
		curr_brand AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_volume
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
		prev_brand AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_volume
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
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.name                                                                 AS brand_name,
			b.uuid                                                                 AS brand_uuid,
			COALESCE(cb.brand_volume, 0)                                           AS current_volume,
			COALESCE(pb.brand_volume, 0)                                           AS previous_volume,
			(SELECT total_volume FROM curr_total)                                   AS current_total_volume,
			(SELECT total_volume FROM prev_total)                                   AS previous_total_volume,
			ROUND((COALESCE(cb.brand_volume, 0) * 100.0 /
			       NULLIF((SELECT total_volume FROM curr_total), 0))::numeric, 2)  AS current_wd_percent,
			ROUND((COALESCE(pb.brand_volume, 0) * 100.0 /
			       NULLIF((SELECT total_volume FROM prev_total), 0))::numeric, 2)  AS previous_wd_percent,
			ROUND((COALESCE(cb.brand_volume, 0) * 100.0 /
			       NULLIF((SELECT total_volume FROM curr_total), 0) -
			       COALESCE(pb.brand_volume, 0) * 100.0 /
			       NULLIF((SELECT total_volume FROM prev_total), 0))::numeric, 2)  AS delta,
			CASE
				WHEN (COALESCE(cb.brand_volume, 0) /
				      NULLIF((SELECT total_volume FROM curr_total), 0)) >
				     (COALESCE(pb.brand_volume, 0) /
				      NULLIF((SELECT total_volume FROM prev_total), 0)) THEN 'up'
				WHEN (COALESCE(cb.brand_volume, 0) /
				      NULLIF((SELECT total_volume FROM curr_total), 0)) <
				     (COALESCE(pb.brand_volume, 0) /
				      NULLIF((SELECT total_volume FROM prev_total), 0)) THEN 'down'
				ELSE 'stable'
			END AS trend
		FROM (SELECT DISTINCT brand_uuid FROM curr_brand
		      UNION
		      SELECT DISTINCT brand_uuid FROM prev_brand) all_brands
		INNER JOIN brands b ON b.uuid = all_brands.brand_uuid
		LEFT  JOIN curr_brand cb ON cb.brand_uuid = all_brands.brand_uuid
		LEFT  JOIN prev_brand pb ON pb.brand_uuid = all_brands.brand_uuid
		ORDER BY current_wd_percent DESC
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
			"status": "error", "message": "Failed to fetch WD evolution", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD Period-over-Period Evolution", "data": results})
}

// WDvsNDCorrelation — WD% × ND% quadrant matrix per brand.
//
//	Quadrants (threshold = 50% for both axes by default, override with ?threshold=N):
//	  "leader"       — WD ≥ T  AND ND ≥ T  : broad presence in high-volume outlets
//	  "volume_focus" — WD ≥ T  AND ND < T  : few but high-volume outlets
//	  "spread"       — WD < T  AND ND ≥ T  : many but low-volume outlets
//	  "laggard"      — WD < T  AND ND < T  : weak on both dimensions
func WDvsNDCorrelation(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	commune_uuid := c.Query("commune_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	threshold := 50.0
	if t := c.QueryFloat("threshold"); t > 0 {
		threshold = t
	}

	if start_date == "" || end_date == "" || country_uuid == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	type QuadRow struct {
		BrandName   string  `json:"brand_name"`
		BrandUUID   string  `json:"brand_uuid"`
		BrandVolume float64 `json:"brand_volume"`
		TotalVolume float64 `json:"total_volume"`
		WdPercent   float64 `json:"wd_percent"`
		NdPos       int64   `json:"nd_pos"`
		TotalPos    int64   `json:"total_pos"`
		NdPercent   float64 `json:"nd_percent"`
		Quadrant    string  `json:"quadrant"`
	}

	sqlQuery := `
		WITH total_stats AS (
			SELECT
				SUM(pfi.number_farde)        AS total_volume,
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
		),
		brand_stats AS (
			SELECT
				pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_volume,
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
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.name                                                                AS brand_name,
			b.uuid                                                                AS brand_uuid,
			bs.brand_volume,
			(SELECT total_volume FROM total_stats)                                AS total_volume,
			ROUND((bs.brand_volume * 100.0 /
			       NULLIF((SELECT total_volume FROM total_stats), 0))::numeric, 2) AS wd_percent,
			bs.nd_pos,
			(SELECT total_pos FROM total_stats)                                   AS total_pos,
			ROUND((bs.nd_pos * 100.0 /
			       NULLIF((SELECT total_pos FROM total_stats), 0))::numeric, 2)   AS nd_percent,
			CASE
				WHEN (bs.brand_volume * 100.0 / NULLIF((SELECT total_volume FROM total_stats), 0)) >= @threshold
				 AND (bs.nd_pos * 100.0 / NULLIF((SELECT total_pos FROM total_stats), 0)) >= @threshold
				THEN 'leader'
				WHEN (bs.brand_volume * 100.0 / NULLIF((SELECT total_volume FROM total_stats), 0)) >= @threshold
				 AND (bs.nd_pos * 100.0 / NULLIF((SELECT total_pos FROM total_stats), 0)) < @threshold
				THEN 'volume_focus'
				WHEN (bs.brand_volume * 100.0 / NULLIF((SELECT total_volume FROM total_stats), 0)) < @threshold
				 AND (bs.nd_pos * 100.0 / NULLIF((SELECT total_pos FROM total_stats), 0)) >= @threshold
				THEN 'spread'
				ELSE 'laggard'
			END AS quadrant
		FROM brand_stats bs
		INNER JOIN brands b ON b.uuid = bs.brand_uuid
		ORDER BY wd_percent DESC
	`

	var results []QuadRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
		"threshold":     threshold,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch WD vs ND correlation", "error": err.Error(),
		})
	}

	// Summarize quadrant counts
	quadrantSummary := map[string]int{
		"leader": 0, "volume_focus": 0, "spread": 0, "laggard": 0,
	}
	for _, r := range results {
		quadrantSummary[r.Quadrant]++
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "WD vs ND Correlation — Quadrant Matrix",
		"meta": fiber.Map{
			"threshold":        threshold,
			"quadrant_summary": quadrantSummary,
		},
		"data": results,
	})
}

// WDPosDrillDown — POS-level WD deep-dive for a specific brand.
//
//	?brand_uuid=<uuid> (required)
//	Returns each POS where the brand was visited, with:
//	  pos_volume     — total fardes of this brand at this POS (counter > 0)
//	  total_volume   — total ALL-brand fardes at this POS
//	  pos_wd_percent — pos_volume / total_volume × 100 (outlet-level weight)
func WDPosDrillDown(c *fiber.Ctx) error {
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
			"status": "error", "message": "brand_uuid is required",
		})
	}

	type DrillRow struct {
		PosName      string  `json:"pos_name"`
		PosUUID      string  `json:"pos_uuid"`
		PosType      string  `json:"pos_type"`
		PosVolume    float64 `json:"pos_volume"`
		TotalVolume  float64 `json:"total_volume"`
		PosWdPercent float64 `json:"pos_wd_percent"`
		VisitCount   int64   `json:"visit_count"`
	}

	sqlQuery := `
		WITH pos_total AS (
			-- All-brand total fardes per POS (i.e. each POS's "weight")
			SELECT pf.pos_uuid, SUM(pfi.number_farde) AS total_volume
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.pos_uuid
		),
		brand_pos AS (
			-- This brand's fardes + visit count per POS
			SELECT
				pf.pos_uuid,
				SUM(pfi.number_farde)    AS pos_volume,
				COUNT(pf.uuid)           AS visit_count
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.brand_uuid = @brand_uuid
			  AND pfi.counter > 0
			GROUP BY pf.pos_uuid
		)
		SELECT
			p.name                                                               AS pos_name,
			p.uuid                                                               AS pos_uuid,
			p.postype                                                            AS pos_type,
			bp.pos_volume,
			COALESCE(pt.total_volume, 0)                                         AS total_volume,
			ROUND((bp.pos_volume * 100.0 /
			       NULLIF(pt.total_volume, 0))::numeric, 2)                      AS pos_wd_percent,
			bp.visit_count
		FROM brand_pos bp
		INNER JOIN pos   p  ON p.uuid  = bp.pos_uuid
		LEFT  JOIN pos_total pt ON pt.pos_uuid = bp.pos_uuid
		ORDER BY pos_wd_percent DESC
	`

	var results []DrillRow
	err := db.Raw(sqlQuery, map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": province_uuid,
		"area_uuid":     area_uuid,
		"sub_area_uuid": sub_area_uuid,
		"commune_uuid":  commune_uuid,
		"brand_uuid":    brand_uuid,
		"start_date":    start_date,
		"end_date":      end_date,
	}).Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch WD POS drill-down", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "WD POS Drill-Down", "data": results})
}

// keep math import used (avoids unused-import error)
var _ = math.NaN

package dashboard

import (
	"fmt"
	"math"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════════╗
// ║          SHARE IN SHOP (SISH) DASHBOARD — SALES MARKET SHARE ANALYTICS     ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  SISH%  (global) =   SUM(brand units sold)                                  ║
// ║                     ──────────────────────────  × 100                      ║
// ║                     SUM(ALL brands units sold)                               ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  SISH_in_shop% =  SUM(brand_sold at POS where brand_sold > 0)               ║
// ║                  ─────────────────────────────────────────────  × 100      ║
// ║                  SUM(all_sold at those same POS)                             ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  Velocity Index = SISH% / SOS%                                              ║
// ║   > 1 → brand sells faster than it stocks  (high sell-through)              ║
// ║   < 1 → brand stocks more than it sells    (slow mover / accumulation)      ║
// ║   = 1 → perfectly aligned stock and sales                                   ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  SECTION 1 — TABLE VIEWS    : Province / Area / SubArea / Commune           ║
// ║  SECTION 2 — BAR CHARTS     : Province / Area / SubArea / Commune           ║
// ║  SECTION 3 — TREND CHART    : SISH% by Brand per Month                      ║
// ║  SECTION 4 — POWER ANALYTICS: Summary KPI / Brand Ranking / Velocity Index  ║
// ║  SECTION 5 — ADVANCED       : Heatmap / Evolution / SOS×SISH Correlation /  ║
// ║                               Gap Analysis / POS Drill-Down                 ║
// ╚══════════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// SHARED PARAM HELPER
// ─────────────────────────────────────────────────────────────────────────────

func extractSISHParams(c *fiber.Ctx) map[string]interface{} {
	country_uuid := c.Query("country_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	if country_uuid == "" || start_date == "" || end_date == "" {
		return nil
	}
	return map[string]interface{}{
		"country_uuid":  country_uuid,
		"province_uuid": c.Query("province_uuid"),
		"area_uuid":     c.Query("area_uuid"),
		"sub_area_uuid": c.Query("sub_area_uuid"),
		"commune_uuid":  c.Query("commune_uuid"),
		"start_date":    start_date,
		"end_date":      end_date,
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 1 — TABLE VIEWS
//   Each row = (territory × brand):
//     brand_sold       — units sold for this brand in the territory
//     total_sold       — all brands units sold in the territory
//     sish_percent     — brand_sold / total_sold × 100
//     sish_in_shop     — brand_sold / (sold at POS where brand sold > 0) × 100
//     pos_with_sales   — POS where brand_sold > 0
//     total_pos        — total POS visited
//     brand_fardes     — fardes stocked (for velocity calc)
//     total_fardes     — total fardes stocked
//     sos_percent      — brand_fardes / total_fardes × 100
//     velocity_index   — sish_percent / sos_percent  (>1 = fast mover)
// ═════════════════════════════════════════════════════════════════════════════

// SISHTableViewProvince — SISH% breakdown per brand at Province level
func SISHTableViewProvince(c *fiber.Ctx) error {
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
		WITH total_market AS (
			-- Denominator: total sold + total fardes + total POS across ALL brands per province
			SELECT
				pf.province_uuid,
				SUM(pfi.sold)                AS total_sold,
				SUM(pfi.number_farde)        AS total_fardes,
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
		brand_market AS (
			-- Numerator: brand sold, brand fardes, POS with sales per province
			SELECT
				pf.province_uuid,
				pfi.brand_uuid,
				SUM(pfi.sold)                AS brand_sold,
				SUM(pfi.number_farde)        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)  AS pos_with_sales
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.province_uuid, pfi.brand_uuid
		),
		in_shop_base AS (
			-- Sold total at POS where the brand was actively sold (sold > 0)
			SELECT
				pf.province_uuid,
				pfi.brand_uuid,
				SUM(pfi_all.sold_at_pos) AS sold_at_brand_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN (
				SELECT pos_form_uuid, SUM(sold) AS sold_at_pos
				FROM pos_form_items WHERE deleted_at IS NULL
				GROUP BY pos_form_uuid
			) pfi_all ON pfi_all.pos_form_uuid = pfi.pos_form_uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			  AND pfi.sold > 0
			GROUP BY pf.province_uuid, pfi.brand_uuid
		)
		SELECT
			pr.name                                                                AS territory_name,
			pr.uuid                                                                AS territory_uuid,
			'province'                                                             AS territory_level,
			b.name                                                                 AS brand_name,
			b.uuid                                                                 AS brand_uuid,
			bm.brand_sold,
			COALESCE(tm.total_sold, 0)                                             AS total_sold,
			bm.brand_fardes,
			COALESCE(tm.total_fardes, 0)                                           AS total_fardes,
			bm.pos_with_sales,
			COALESCE(tm.total_pos, 0)                                              AS total_pos,
			ROUND((bm.brand_sold * 100.0 /
			       NULLIF(tm.total_sold, 0))::numeric, 2)                          AS sish_percent,
			ROUND((bm.brand_sold * 100.0 /
			       NULLIF(isb.sold_at_brand_pos, 0))::numeric, 2)                  AS sish_in_shop,
			ROUND((bm.brand_fardes * 100.0 /
			       NULLIF(tm.total_fardes, 0))::numeric, 2)                        AS sos_percent,
			ROUND(CASE WHEN (bm.brand_fardes * 1.0 / NULLIF(tm.total_fardes, 0)) > 0
			      THEN (bm.brand_sold * 1.0 / NULLIF(tm.total_sold, 0)) /
			           (bm.brand_fardes * 1.0 / NULLIF(tm.total_fardes, 0))
			      ELSE 0 END::numeric, 3)                                          AS velocity_index
		FROM brand_market bm
		INNER JOIN brands    b  ON b.uuid  = bm.brand_uuid
		INNER JOIN provinces pr ON pr.uuid = bm.province_uuid
		LEFT  JOIN total_market tm  ON tm.province_uuid = bm.province_uuid
		LEFT  JOIN in_shop_base isb ON isb.province_uuid = bm.province_uuid
		                           AND isb.brand_uuid    = bm.brand_uuid
		ORDER BY pr.name, sish_percent DESC
	`

	type SISHRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosWithSales   int64   `json:"pos_with_sales"`
		TotalPos       int64   `json:"total_pos"`
		SishPercent    float64 `json:"sish_percent"`
		SishInShop     float64 `json:"sish_in_shop"`
		SosPercent     float64 `json:"sos_percent"`
		VelocityIndex  float64 `json:"velocity_index"`
	}

	var results []SISHRow
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
			"status": "error", "message": "Failed to fetch SISH province table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SISH Province Table", "data": results})
}

// SISHTableViewArea — SISH% breakdown per brand at Area level
func SISHTableViewArea(c *fiber.Ctx) error {
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
		WITH total_market AS (
			SELECT
				pf.area_uuid,
				SUM(pfi.sold)                AS total_sold,
				SUM(pfi.number_farde)        AS total_fardes,
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
		brand_market AS (
			SELECT
				pf.area_uuid,
				pfi.brand_uuid,
				SUM(pfi.sold)                AS brand_sold,
				SUM(pfi.number_farde)        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)  AS pos_with_sales
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.area_uuid, pfi.brand_uuid
		),
		in_shop_base AS (
			SELECT
				pf.area_uuid,
				pfi.brand_uuid,
				SUM(pfi_all.sold_at_pos) AS sold_at_brand_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN (
				SELECT pos_form_uuid, SUM(sold) AS sold_at_pos
				FROM pos_form_items WHERE deleted_at IS NULL GROUP BY pos_form_uuid
			) pfi_all ON pfi_all.pos_form_uuid = pfi.pos_form_uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.sold > 0
			GROUP BY pf.area_uuid, pfi.brand_uuid
		)
		SELECT
			a.name                                                                 AS territory_name,
			a.uuid                                                                 AS territory_uuid,
			'area'                                                                 AS territory_level,
			b.name                                                                 AS brand_name,
			b.uuid                                                                 AS brand_uuid,
			bm.brand_sold,
			COALESCE(tm.total_sold, 0)                                             AS total_sold,
			bm.brand_fardes,
			COALESCE(tm.total_fardes, 0)                                           AS total_fardes,
			bm.pos_with_sales,
			COALESCE(tm.total_pos, 0)                                              AS total_pos,
			ROUND((bm.brand_sold * 100.0 / NULLIF(tm.total_sold, 0))::numeric, 2) AS sish_percent,
			ROUND((bm.brand_sold * 100.0 / NULLIF(isb.sold_at_brand_pos, 0))::numeric, 2) AS sish_in_shop,
			ROUND((bm.brand_fardes * 100.0 / NULLIF(tm.total_fardes, 0))::numeric, 2) AS sos_percent,
			ROUND(CASE WHEN (bm.brand_fardes * 1.0 / NULLIF(tm.total_fardes, 0)) > 0
			      THEN (bm.brand_sold * 1.0 / NULLIF(tm.total_sold, 0)) /
			           (bm.brand_fardes * 1.0 / NULLIF(tm.total_fardes, 0))
			      ELSE 0 END::numeric, 3)                                          AS velocity_index
		FROM brand_market bm
		INNER JOIN brands b ON b.uuid = bm.brand_uuid
		INNER JOIN areas  a ON a.uuid = bm.area_uuid
		LEFT  JOIN total_market tm  ON tm.area_uuid = bm.area_uuid
		LEFT  JOIN in_shop_base isb ON isb.area_uuid = bm.area_uuid AND isb.brand_uuid = bm.brand_uuid
		ORDER BY a.name, sish_percent DESC
	`

	type SISHRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosWithSales   int64   `json:"pos_with_sales"`
		TotalPos       int64   `json:"total_pos"`
		SishPercent    float64 `json:"sish_percent"`
		SishInShop     float64 `json:"sish_in_shop"`
		SosPercent     float64 `json:"sos_percent"`
		VelocityIndex  float64 `json:"velocity_index"`
	}

	var results []SISHRow
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
			"status": "error", "message": "Failed to fetch SISH area table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SISH Area Table", "data": results})
}

// SISHTableViewSubArea — SISH% breakdown per brand at SubArea level
func SISHTableViewSubArea(c *fiber.Ctx) error {
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
		WITH total_market AS (
			SELECT
				pf.sub_area_uuid,
				SUM(pfi.sold)                AS total_sold,
				SUM(pfi.number_farde)        AS total_fardes,
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
		brand_market AS (
			SELECT
				pf.sub_area_uuid,
				pfi.brand_uuid,
				SUM(pfi.sold)                AS brand_sold,
				SUM(pfi.number_farde)        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)  AS pos_with_sales
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.sub_area_uuid, pfi.brand_uuid
		),
		in_shop_base AS (
			SELECT
				pf.sub_area_uuid,
				pfi.brand_uuid,
				SUM(pfi_all.sold_at_pos) AS sold_at_brand_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN (
				SELECT pos_form_uuid, SUM(sold) AS sold_at_pos
				FROM pos_form_items WHERE deleted_at IS NULL GROUP BY pos_form_uuid
			) pfi_all ON pfi_all.pos_form_uuid = pfi.pos_form_uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.sold > 0
			GROUP BY pf.sub_area_uuid, pfi.brand_uuid
		)
		SELECT
			sa.name                                                                AS territory_name,
			sa.uuid                                                                AS territory_uuid,
			'subarea'                                                              AS territory_level,
			b.name                                                                 AS brand_name,
			b.uuid                                                                 AS brand_uuid,
			bm.brand_sold,
			COALESCE(tm.total_sold, 0)                                             AS total_sold,
			bm.brand_fardes,
			COALESCE(tm.total_fardes, 0)                                           AS total_fardes,
			bm.pos_with_sales,
			COALESCE(tm.total_pos, 0)                                              AS total_pos,
			ROUND((bm.brand_sold * 100.0 / NULLIF(tm.total_sold, 0))::numeric, 2) AS sish_percent,
			ROUND((bm.brand_sold * 100.0 / NULLIF(isb.sold_at_brand_pos, 0))::numeric, 2) AS sish_in_shop,
			ROUND((bm.brand_fardes * 100.0 / NULLIF(tm.total_fardes, 0))::numeric, 2) AS sos_percent,
			ROUND(CASE WHEN (bm.brand_fardes * 1.0 / NULLIF(tm.total_fardes, 0)) > 0
			      THEN (bm.brand_sold * 1.0 / NULLIF(tm.total_sold, 0)) /
			           (bm.brand_fardes * 1.0 / NULLIF(tm.total_fardes, 0))
			      ELSE 0 END::numeric, 3)                                          AS velocity_index
		FROM brand_market bm
		INNER JOIN brands    b  ON b.uuid  = bm.brand_uuid
		INNER JOIN sub_areas sa ON sa.uuid = bm.sub_area_uuid
		LEFT  JOIN total_market tm  ON tm.sub_area_uuid = bm.sub_area_uuid
		LEFT  JOIN in_shop_base isb ON isb.sub_area_uuid = bm.sub_area_uuid AND isb.brand_uuid = bm.brand_uuid
		ORDER BY sa.name, sish_percent DESC
	`

	type SISHRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosWithSales   int64   `json:"pos_with_sales"`
		TotalPos       int64   `json:"total_pos"`
		SishPercent    float64 `json:"sish_percent"`
		SishInShop     float64 `json:"sish_in_shop"`
		SosPercent     float64 `json:"sos_percent"`
		VelocityIndex  float64 `json:"velocity_index"`
	}

	var results []SISHRow
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
			"status": "error", "message": "Failed to fetch SISH subarea table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SISH SubArea Table", "data": results})
}

// SISHTableViewCommune — SISH% breakdown per brand at Commune level
func SISHTableViewCommune(c *fiber.Ctx) error {
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
		WITH total_market AS (
			SELECT
				pf.commune_uuid,
				SUM(pfi.sold)                AS total_sold,
				SUM(pfi.number_farde)        AS total_fardes,
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
		brand_market AS (
			SELECT
				pf.commune_uuid,
				pfi.brand_uuid,
				SUM(pfi.sold)                AS brand_sold,
				SUM(pfi.number_farde)        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)  AS pos_with_sales
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.commune_uuid, pfi.brand_uuid
		),
		in_shop_base AS (
			SELECT
				pf.commune_uuid,
				pfi.brand_uuid,
				SUM(pfi_all.sold_at_pos) AS sold_at_brand_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN (
				SELECT pos_form_uuid, SUM(sold) AS sold_at_pos
				FROM pos_form_items WHERE deleted_at IS NULL GROUP BY pos_form_uuid
			) pfi_all ON pfi_all.pos_form_uuid = pfi.pos_form_uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL AND pfi.sold > 0
			GROUP BY pf.commune_uuid, pfi.brand_uuid
		)
		SELECT
			co.name                                                                AS territory_name,
			co.uuid                                                                AS territory_uuid,
			'commune'                                                              AS territory_level,
			b.name                                                                 AS brand_name,
			b.uuid                                                                 AS brand_uuid,
			bm.brand_sold,
			COALESCE(tm.total_sold, 0)                                             AS total_sold,
			bm.brand_fardes,
			COALESCE(tm.total_fardes, 0)                                           AS total_fardes,
			bm.pos_with_sales,
			COALESCE(tm.total_pos, 0)                                              AS total_pos,
			ROUND((bm.brand_sold * 100.0 / NULLIF(tm.total_sold, 0))::numeric, 2) AS sish_percent,
			ROUND((bm.brand_sold * 100.0 / NULLIF(isb.sold_at_brand_pos, 0))::numeric, 2) AS sish_in_shop,
			ROUND((bm.brand_fardes * 100.0 / NULLIF(tm.total_fardes, 0))::numeric, 2) AS sos_percent,
			ROUND(CASE WHEN (bm.brand_fardes * 1.0 / NULLIF(tm.total_fardes, 0)) > 0
			      THEN (bm.brand_sold * 1.0 / NULLIF(tm.total_sold, 0)) /
			           (bm.brand_fardes * 1.0 / NULLIF(tm.total_fardes, 0))
			      ELSE 0 END::numeric, 3)                                          AS velocity_index
		FROM brand_market bm
		INNER JOIN brands   b  ON b.uuid  = bm.brand_uuid
		INNER JOIN communes co ON co.uuid = bm.commune_uuid
		LEFT  JOIN total_market tm  ON tm.commune_uuid = bm.commune_uuid
		LEFT  JOIN in_shop_base isb ON isb.commune_uuid = bm.commune_uuid AND isb.brand_uuid = bm.brand_uuid
		ORDER BY co.name, sish_percent DESC
	`

	type SISHRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosWithSales   int64   `json:"pos_with_sales"`
		TotalPos       int64   `json:"total_pos"`
		SishPercent    float64 `json:"sish_percent"`
		SishInShop     float64 `json:"sish_in_shop"`
		SosPercent     float64 `json:"sos_percent"`
		VelocityIndex  float64 `json:"velocity_index"`
	}

	var results []SISHRow
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
			"status": "error", "message": "Failed to fetch SISH commune table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SISH Commune Table", "data": results})
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 2 — BAR CHARTS
//   Each row = (territory × brand) with sish_percent + sos_percent + velocity_index
//   Frontend builds a grouped horizontal bar chart.
// ═════════════════════════════════════════════════════════════════════════════

func sishBarChartQuery(groupField, joinTable, joinAlias string) string {
	return fmt.Sprintf(`
		WITH totals AS (
			SELECT pf.%[1]s,
				SUM(pfi.sold)               AS total_sold,
				SUM(pfi.number_farde)       AS total_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.%[1]s
		),
		brand_totals AS (
			SELECT pf.%[1]s, pfi.brand_uuid,
				SUM(pfi.sold)               AS brand_sold,
				SUM(pfi.number_farde)       AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.%[1]s, pfi.brand_uuid
		)
		SELECT
			%[3]s.name                                                               AS territory_name,
			%[3]s.uuid                                                               AS territory_uuid,
			b.name                                                                   AS brand_name,
			b.uuid                                                                   AS brand_uuid,
			bt.brand_sold,
			COALESCE(t.total_sold, 0)                                                AS total_sold,
			ROUND((bt.brand_sold * 100.0 / NULLIF(t.total_sold, 0))::numeric, 2)    AS sish_percent,
			bt.brand_fardes,
			COALESCE(t.total_fardes, 0)                                              AS total_fardes,
			ROUND((bt.brand_fardes * 100.0 / NULLIF(t.total_fardes, 0))::numeric, 2) AS sos_percent,
			ROUND(CASE WHEN (bt.brand_fardes * 1.0 / NULLIF(t.total_fardes, 0)) > 0
			      THEN (bt.brand_sold * 1.0 / NULLIF(t.total_sold, 0)) /
			           (bt.brand_fardes * 1.0 / NULLIF(t.total_fardes, 0))
			      ELSE 0 END::numeric, 3)                                            AS velocity_index
		FROM brand_totals bt
		INNER JOIN brands   b ON b.uuid = bt.brand_uuid
		INNER JOIN %[2]s %[3]s ON %[3]s.uuid = bt.%[1]s
		LEFT  JOIN totals t ON t.%[1]s = bt.%[1]s
		ORDER BY %[3]s.name, sish_percent DESC
	`, groupField, joinTable, joinAlias)
}

type SISHBarRow struct {
	TerritoryName string  `json:"territory_name"`
	TerritoryUUID string  `json:"territory_uuid"`
	BrandName     string  `json:"brand_name"`
	BrandUUID     string  `json:"brand_uuid"`
	BrandSold     float64 `json:"brand_sold"`
	TotalSold     float64 `json:"total_sold"`
	SishPercent   float64 `json:"sish_percent"`
	BrandFardes   float64 `json:"brand_fardes"`
	TotalFardes   float64 `json:"total_fardes"`
	SosPercent    float64 `json:"sos_percent"`
	VelocityIndex float64 `json:"velocity_index"`
}

func sishBarChartHandler(c *fiber.Ctx, groupField, joinTable, joinAlias, label string) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	var results []SISHBarRow
	if err := db.Raw(sishBarChartQuery(groupField, joinTable, joinAlias), params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH bar chart", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": label, "data": results})
}

func SISHBarChartProvince(c *fiber.Ctx) error {
	return sishBarChartHandler(c, "province_uuid", "provinces", "pr", "SISH Bar Chart — Province")
}
func SISHBarChartArea(c *fiber.Ctx) error {
	return sishBarChartHandler(c, "area_uuid", "areas", "a", "SISH Bar Chart — Area")
}
func SISHBarChartSubArea(c *fiber.Ctx) error {
	return sishBarChartHandler(c, "sub_area_uuid", "sub_areas", "sa", "SISH Bar Chart — SubArea")
}
func SISHBarChartCommune(c *fiber.Ctx) error {
	return sishBarChartHandler(c, "commune_uuid", "communes", "co", "SISH Bar Chart — Commune")
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 3 — MONTHLY TREND LINE CHART
//   Returns SISH% and SOS% per brand per month for a time-series line chart.
// ═════════════════════════════════════════════════════════════════════════════

// SISHLineChartByMonth — monthly SISH% and SOS% trend per brand
func SISHLineChartByMonth(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH monthly_total AS (
			SELECT
				TO_CHAR(DATE_TRUNC('month', pf.created_at), 'YYYY-MM') AS month,
				SUM(pfi.sold)               AS total_sold,
				SUM(pfi.number_farde)       AS total_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY 1
		),
		monthly_brand AS (
			SELECT
				TO_CHAR(DATE_TRUNC('month', pf.created_at), 'YYYY-MM') AS month,
				pfi.brand_uuid,
				SUM(pfi.sold)               AS brand_sold,
				SUM(pfi.number_farde)       AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY 1, pfi.brand_uuid
		)
		SELECT
			mb.month,
			b.name                                                                    AS brand_name,
			b.uuid                                                                    AS brand_uuid,
			mb.brand_sold,
			mt.total_sold,
			mb.brand_fardes,
			mt.total_fardes,
			ROUND((mb.brand_sold * 100.0 / NULLIF(mt.total_sold, 0))::numeric, 2)   AS sish_percent,
			ROUND((mb.brand_fardes * 100.0 / NULLIF(mt.total_fardes, 0))::numeric, 2) AS sos_percent,
			ROUND(CASE WHEN (mb.brand_fardes * 1.0 / NULLIF(mt.total_fardes, 0)) > 0
			      THEN (mb.brand_sold * 1.0 / NULLIF(mt.total_sold, 0)) /
			           (mb.brand_fardes * 1.0 / NULLIF(mt.total_fardes, 0))
			      ELSE 0 END::numeric, 3)                                             AS velocity_index
		FROM monthly_brand mb
		INNER JOIN brands          b  ON b.uuid  = mb.brand_uuid
		LEFT  JOIN monthly_total   mt ON mt.month = mb.month
		ORDER BY mb.month, sish_percent DESC
	`

	type TrendRow struct {
		Month         string  `json:"month"`
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		BrandSold     float64 `json:"brand_sold"`
		TotalSold     float64 `json:"total_sold"`
		BrandFardes   float64 `json:"brand_fardes"`
		TotalFardes   float64 `json:"total_fardes"`
		SishPercent   float64 `json:"sish_percent"`
		SosPercent    float64 `json:"sos_percent"`
		VelocityIndex float64 `json:"velocity_index"`
	}

	var results []TrendRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH monthly trend", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SISH Monthly Trend", "data": results})
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 4 — POWER ANALYTICS
// ═════════════════════════════════════════════════════════════════════════════

// SISHSummaryKPI — global executive KPI card for the selected period + territory
//
//	Returns:
//	  total_sold, total_fardes, total_pos, total_brands
//	  top_brand_by_sish (name + sish%)
//	  fastest_brand     (highest velocity_index, name + value)
//	  slowest_brand     (lowest velocity_index among brands with sales)
//	  avg_sish_per_brand
//	  market_entropy    — Shannon entropy on sales distribution (diversity measure)
func SISHSummaryKPI(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	type BrandKPI struct {
		BrandUUID     string  `json:"brand_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandSold     float64 `json:"brand_sold"`
		BrandFardes   float64 `json:"brand_fardes"`
		SishPercent   float64 `json:"sish_percent"`
		SosPercent    float64 `json:"sos_percent"`
		VelocityIndex float64 `json:"velocity_index"`
	}

	sqlBrands := `
		WITH totals AS (
			SELECT SUM(pfi.sold) AS total_sold, SUM(pfi.number_farde) AS total_fardes
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
		brand_data AS (
			SELECT pfi.brand_uuid,
				SUM(pfi.sold)         AS brand_sold,
				SUM(pfi.number_farde) AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.uuid AS brand_uuid, b.name AS brand_name,
			bd.brand_sold, bd.brand_fardes,
			ROUND((bd.brand_sold * 100.0  / NULLIF((SELECT total_sold   FROM totals),0))::numeric,2) AS sish_percent,
			ROUND((bd.brand_fardes * 100.0/ NULLIF((SELECT total_fardes FROM totals),0))::numeric,2) AS sos_percent,
			ROUND(CASE WHEN (bd.brand_fardes*1.0/NULLIF((SELECT total_fardes FROM totals),0))>0
			      THEN (bd.brand_sold*1.0/NULLIF((SELECT total_sold FROM totals),0)) /
			           (bd.brand_fardes*1.0/NULLIF((SELECT total_fardes FROM totals),0))
			      ELSE 0 END::numeric, 3)                                                            AS velocity_index
		FROM brand_data bd
		INNER JOIN brands b ON b.uuid = bd.brand_uuid
		ORDER BY sish_percent DESC
	`

	var brands []BrandKPI
	if err := db.Raw(sqlBrands, params).Scan(&brands).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH KPI", "error": err.Error(),
		})
	}

	// Aggregate KPIs in Go
	var totalSold, totalFardes float64
	var totalPos int64
	db.Raw(`
		SELECT SUM(pfi.sold) AS total_sold, SUM(pfi.number_farde) AS total_fardes,
		       COUNT(DISTINCT pf.pos_uuid) AS total_pos
		FROM pos_form_items pfi
		INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
		WHERE pf.country_uuid = @country_uuid
		  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
		  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
		  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
		  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
		  AND pf.created_at BETWEEN @start_date AND @end_date
		  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
	`, params).Row().Scan(&totalSold, &totalFardes, &totalPos)

	// Shannon entropy: -SUM(p * log2(p)) over brand sales shares
	var entropy float64
	for _, b := range brands {
		if b.SishPercent > 0 {
			p := b.SishPercent / 100.0
			entropy -= p * math.Log2(p)
		}
	}

	var topBrandSish, fastestBrand, slowestBrand BrandKPI
	var maxVel float64 = -1
	minVel := math.MaxFloat64
	for _, b := range brands {
		if len(topBrandSish.BrandUUID) == 0 {
			topBrandSish = b // already sorted DESC
		}
		if b.VelocityIndex > maxVel {
			maxVel = b.VelocityIndex
			fastestBrand = b
		}
		if b.BrandSold > 0 && b.VelocityIndex < minVel {
			minVel = b.VelocityIndex
			slowestBrand = b
		}
	}

	var avgSish float64
	if len(brands) > 0 {
		for _, b := range brands {
			avgSish += b.SishPercent
		}
		avgSish /= float64(len(brands))
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "SISH Summary KPI",
		"data": fiber.Map{
			"total_sold":        totalSold,
			"total_fardes":      totalFardes,
			"total_pos":         totalPos,
			"total_brands":      len(brands),
			"avg_sish_percent":  math.Round(avgSish*100) / 100,
			"market_entropy":    math.Round(entropy*1000) / 1000,
			"top_brand_by_sish": topBrandSish,
			"fastest_brand":     fastestBrand,
			"slowest_brand":     slowestBrand,
		},
	})
}

// SISHBrandRanking — brands ranked by SISH% (market share of sales)
//
//	Also includes velocity_index and category:
//	  "market_leader"   — SISH ≥ top tercile
//	  "challenger"      — SISH ≥ median and < top tercile
//	  "niche"           — SISH < median
func SISHBrandRanking(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH totals AS (
			SELECT SUM(pfi.sold) AS total_sold, SUM(pfi.number_farde) AS total_fardes,
			       COUNT(DISTINCT pf.pos_uuid) AS total_pos
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
		brand_data AS (
			SELECT pfi.brand_uuid,
				SUM(pfi.sold)               AS brand_sold,
				SUM(pfi.number_farde)       AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid) AS pos_with_sales
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		),
		ranked AS (
			SELECT
				b.uuid                                                                  AS brand_uuid,
				b.name                                                                  AS brand_name,
				bd.brand_sold,
				(SELECT total_sold   FROM totals)                                       AS total_sold,
				bd.brand_fardes,
				(SELECT total_fardes FROM totals)                                       AS total_fardes,
				bd.pos_with_sales,
				(SELECT total_pos    FROM totals)                                       AS total_pos,
				ROUND((bd.brand_sold * 100.0 /
				       NULLIF((SELECT total_sold FROM totals),0))::numeric, 2)          AS sish_percent,
				ROUND((bd.brand_fardes * 100.0 /
				       NULLIF((SELECT total_fardes FROM totals),0))::numeric, 2)        AS sos_percent,
				ROUND(CASE WHEN bd.brand_fardes > 0
				      THEN (bd.brand_sold / NULLIF((SELECT total_sold   FROM totals),0)) /
				           (bd.brand_fardes/ NULLIF((SELECT total_fardes FROM totals),0))
				      ELSE 0 END::numeric, 3)                                           AS velocity_index,
				ROW_NUMBER() OVER (ORDER BY bd.brand_sold DESC)                         AS rank,
				SUM(bd.brand_sold) OVER ()                                              AS running_total,
				SUM(bd.brand_sold) OVER (ORDER BY bd.brand_sold DESC
				                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sold
			FROM brand_data bd
			INNER JOIN brands b ON b.uuid = bd.brand_uuid
		)
		SELECT
			rank,
			brand_uuid, brand_name,
			brand_sold, total_sold,
			brand_fardes, total_fardes,
			pos_with_sales, total_pos,
			sish_percent, sos_percent,
			ROUND((sish_percent - sos_percent)::numeric, 2) AS sish_sos_delta,
			velocity_index,
			ROUND((cumulative_sold * 100.0 / NULLIF(running_total, 0))::numeric, 2) AS cumulative_sish,
			CASE
				WHEN PERCENT_RANK() OVER (ORDER BY sish_percent) >= 0.66 THEN 'market_leader'
				WHEN PERCENT_RANK() OVER (ORDER BY sish_percent) >= 0.33 THEN 'challenger'
				ELSE 'niche'
			END AS category
		FROM ranked
		ORDER BY rank
	`

	type RankRow struct {
		Rank           int64   `json:"rank"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandName      string  `json:"brand_name"`
		BrandSold      float64 `json:"brand_sold"`
		TotalSold      float64 `json:"total_sold"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosWithSales   int64   `json:"pos_with_sales"`
		TotalPos       int64   `json:"total_pos"`
		SishPercent    float64 `json:"sish_percent"`
		SosPercent     float64 `json:"sos_percent"`
		SishSosDelta   float64 `json:"sish_sos_delta"`
		VelocityIndex  float64 `json:"velocity_index"`
		CumulativeSish float64 `json:"cumulative_sish"`
		Category       string  `json:"category"`
	}

	var results []RankRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH brand ranking", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SISH Brand Ranking", "data": results})
}

// SISHVelocityIndex — Velocity analysis: how fast each brand turns shelf stock into sales.
//
//	velocity_index = (brand_sold / total_sold) / (brand_fardes / total_fardes)
//	  > 1.0  → fast mover: sells more than it stocks relative to market
//	  = 1.0  → aligned: stock and sales proportionate
//	  < 1.0  → slow mover: accumulates more stock than it sells
//
//	Also computes stock_turn_days = brand_fardes / (brand_sold / period_days)
func SISHVelocityIndex(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	// Calculate period length in days
	var periodDays int
	db.Raw(`SELECT (@end_date::date - @start_date::date + 1)`, params).Row().Scan(&periodDays)
	if periodDays < 1 {
		periodDays = 1
	}
	params["period_days"] = periodDays

	sqlQuery := `
		WITH totals AS (
			SELECT SUM(pfi.sold) AS total_sold, SUM(pfi.number_farde) AS total_fardes
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
		brand_data AS (
			SELECT pfi.brand_uuid,
				SUM(pfi.sold)               AS brand_sold,
				SUM(pfi.number_farde)       AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.uuid                                                                   AS brand_uuid,
			b.name                                                                   AS brand_name,
			bd.brand_sold,
			bd.brand_fardes,
			(SELECT total_sold   FROM totals)                                        AS total_sold,
			(SELECT total_fardes FROM totals)                                        AS total_fardes,
			ROUND((bd.brand_sold   * 100.0 / NULLIF((SELECT total_sold  FROM totals),0))::numeric,2) AS sish_percent,
			ROUND((bd.brand_fardes * 100.0 / NULLIF((SELECT total_fardes FROM totals),0))::numeric,2) AS sos_percent,
			ROUND(CASE WHEN bd.brand_fardes > 0
			      THEN (bd.brand_sold / NULLIF((SELECT total_sold FROM totals),0)) /
			           (bd.brand_fardes/NULLIF((SELECT total_fardes FROM totals),0))
			      ELSE 0 END::numeric, 3)                                            AS velocity_index,
			-- Stock Turn: days worth of current stock at current sell rate
			ROUND(CASE WHEN bd.brand_sold > 0
			      THEN bd.brand_fardes / (bd.brand_sold / @period_days::float)
			      ELSE NULL END::numeric, 1)                                         AS stock_turn_days,
			CASE
				WHEN CASE WHEN bd.brand_fardes > 0
				          THEN (bd.brand_sold / NULLIF((SELECT total_sold FROM totals),0)) /
				               (bd.brand_fardes/NULLIF((SELECT total_fardes FROM totals),0))
				          ELSE 0 END > 1.1  THEN 'fast_mover'
				WHEN CASE WHEN bd.brand_fardes > 0
				          THEN (bd.brand_sold / NULLIF((SELECT total_sold FROM totals),0)) /
				               (bd.brand_fardes/NULLIF((SELECT total_fardes FROM totals),0))
				          ELSE 0 END BETWEEN 0.9 AND 1.1 THEN 'aligned'
				ELSE 'slow_mover'
			END AS velocity_category
		FROM brand_data bd
		INNER JOIN brands b ON b.uuid = bd.brand_uuid
		ORDER BY velocity_index DESC
	`

	type VelRow struct {
		BrandUUID        string  `json:"brand_uuid"`
		BrandName        string  `json:"brand_name"`
		BrandSold        float64 `json:"brand_sold"`
		BrandFardes      float64 `json:"brand_fardes"`
		TotalSold        float64 `json:"total_sold"`
		TotalFardes      float64 `json:"total_fardes"`
		SishPercent      float64 `json:"sish_percent"`
		SosPercent       float64 `json:"sos_percent"`
		VelocityIndex    float64 `json:"velocity_index"`
		StockTurnDays    float64 `json:"stock_turn_days"`
		VelocityCategory string  `json:"velocity_category"`
	}

	var results []VelRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH velocity index", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"status": "success", "message": "SISH Velocity Index",
		"period_days": periodDays,
		"data":        results,
	})
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION 5 — ADVANCED ANALYTICS
// ═════════════════════════════════════════════════════════════════════════════

// SISHHeatmap — brand × territory SISH% matrix
// ?level=province|area|subarea|commune (default: province)
func SISHHeatmap(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	level := c.Query("level", "province")
	var groupField, joinTable, joinAlias string
	switch level {
	case "area":
		groupField, joinTable, joinAlias = "area_uuid", "areas", "terr"
	case "subarea":
		groupField, joinTable, joinAlias = "sub_area_uuid", "sub_areas", "terr"
	case "commune":
		groupField, joinTable, joinAlias = "commune_uuid", "communes", "terr"
	default:
		level = "province"
		groupField, joinTable, joinAlias = "province_uuid", "provinces", "terr"
	}

	sqlQuery := fmt.Sprintf(`
		WITH region_total AS (
			SELECT pf.%[1]s,
				SUM(pfi.sold) AS total_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.%[1]s
		),
		brand_region AS (
			SELECT pf.%[1]s, pfi.brand_uuid,
				SUM(pfi.sold) AS brand_sold
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.%[1]s, pfi.brand_uuid
		)
		SELECT
			%[3]s.uuid                                                               AS territory_uuid,
			%[3]s.name                                                               AS territory_name,
			b.uuid                                                                   AS brand_uuid,
			b.name                                                                   AS brand_name,
			br.brand_sold,
			COALESCE(rt.total_sold, 0)                                               AS total_sold,
			ROUND((br.brand_sold * 100.0 / NULLIF(rt.total_sold, 0))::numeric, 2)   AS sish_percent
		FROM brand_region br
		INNER JOIN brands  b    ON b.uuid    = br.brand_uuid
		INNER JOIN %[2]s %[3]s  ON %[3]s.uuid = br.%[1]s
		LEFT  JOIN region_total rt ON rt.%[1]s = br.%[1]s
		ORDER BY %[3]s.name, sish_percent DESC
	`, groupField, joinTable, joinAlias)

	type RawCell struct {
		TerritoryUUID string  `json:"territory_uuid"`
		TerritoryName string  `json:"territory_name"`
		BrandUUID     string  `json:"brand_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandSold     float64 `json:"brand_sold"`
		TotalSold     float64 `json:"total_sold"`
		SishPercent   float64 `json:"sish_percent"`
	}

	var raw []RawCell
	if err := db.Raw(sqlQuery, params).Scan(&raw).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH heatmap", "error": err.Error(),
		})
	}

	brandIndex := map[string]int{}
	terrIndex := map[string]int{}
	var brands, territories []map[string]string

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
		matrix[brandIndex[r.BrandUUID]][terrIndex[r.TerritoryUUID]] = r.SishPercent
	}

	return c.JSON(fiber.Map{
		"status": "success", "message": "SISH Heatmap — Brand × Territory", "level": level,
		"data": fiber.Map{"brands": brands, "territories": territories, "matrix": matrix},
	})
}

// SISHEvolution — Period-over-Period SISH% comparison per brand.
//
//	Compares current window vs the preceding equal-length window.
//	  current_sish_percent  — SISH% in selected period
//	  previous_sish_percent — SISH% in previous equal period
//	  delta                 — current - previous (pp change)
//	  velocity_trend        — change in velocity_index
//	  trend                 — "gaining" | "losing" | "stable"
func SISHEvolution(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH curr_data AS (
			SELECT pfi.brand_uuid,
				SUM(pfi.sold)         AS brand_sold,
				SUM(pfi.number_farde) AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		),
		prev_data AS (
			SELECT pfi.brand_uuid,
				SUM(pfi.sold)         AS brand_sold,
				SUM(pfi.number_farde) AS brand_fardes
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
			GROUP BY pfi.brand_uuid
		),
		curr_totals AS (SELECT SUM(brand_sold) AS ts, SUM(brand_fardes) AS tf FROM curr_data),
		prev_totals AS (SELECT SUM(brand_sold) AS ts, SUM(brand_fardes) AS tf FROM prev_data)
		SELECT
			b.uuid                                                                    AS brand_uuid,
			b.name                                                                    AS brand_name,
			COALESCE(cd.brand_sold, 0)                                                AS current_sold,
			COALESCE(pd.brand_sold, 0)                                                AS previous_sold,
			COALESCE(cd.brand_fardes, 0)                                              AS current_fardes,
			COALESCE(pd.brand_fardes, 0)                                              AS previous_fardes,
			(SELECT ts FROM curr_totals)                                              AS current_total_sold,
			(SELECT ts FROM prev_totals)                                              AS previous_total_sold,
			ROUND((COALESCE(cd.brand_sold,0)*100.0/NULLIF((SELECT ts FROM curr_totals),0))::numeric,2) AS current_sish_percent,
			ROUND((COALESCE(pd.brand_sold,0)*100.0/NULLIF((SELECT ts FROM prev_totals),0))::numeric,2) AS previous_sish_percent,
			ROUND((COALESCE(cd.brand_sold,0)*100.0/NULLIF((SELECT ts FROM curr_totals),0) -
			       COALESCE(pd.brand_sold,0)*100.0/NULLIF((SELECT ts FROM prev_totals),0))::numeric,2) AS delta,
			-- Velocity index change
			ROUND(
				CASE WHEN (COALESCE(cd.brand_fardes,0)*1.0/NULLIF((SELECT tf FROM curr_totals),0)) > 0
				     THEN (COALESCE(cd.brand_sold,0)*1.0/NULLIF((SELECT ts FROM curr_totals),0)) /
				          (COALESCE(cd.brand_fardes,0)*1.0/NULLIF((SELECT tf FROM curr_totals),0))
				     ELSE 0 END -
				CASE WHEN (COALESCE(pd.brand_fardes,0)*1.0/NULLIF((SELECT tf FROM prev_totals),0)) > 0
				     THEN (COALESCE(pd.brand_sold,0)*1.0/NULLIF((SELECT ts FROM prev_totals),0)) /
				          (COALESCE(pd.brand_fardes,0)*1.0/NULLIF((SELECT tf FROM prev_totals),0))
				     ELSE 0 END
			::numeric, 3)                                                             AS velocity_delta,
			CASE
				WHEN COALESCE(cd.brand_sold,0)*1.0/NULLIF((SELECT ts FROM curr_totals),0) >
				     COALESCE(pd.brand_sold,0)*1.0/NULLIF((SELECT ts FROM prev_totals),0) THEN 'gaining'
				WHEN COALESCE(cd.brand_sold,0)*1.0/NULLIF((SELECT ts FROM curr_totals),0) <
				     COALESCE(pd.brand_sold,0)*1.0/NULLIF((SELECT ts FROM prev_totals),0) THEN 'losing'
				ELSE 'stable'
			END AS trend
		FROM (SELECT brand_uuid FROM curr_data UNION SELECT brand_uuid FROM prev_data) all_brands
		INNER JOIN brands b  ON b.uuid = all_brands.brand_uuid
		LEFT  JOIN curr_data cd ON cd.brand_uuid = all_brands.brand_uuid
		LEFT  JOIN prev_data pd ON pd.brand_uuid = all_brands.brand_uuid
		ORDER BY current_sish_percent DESC
	`

	type EvoRow struct {
		BrandUUID           string  `json:"brand_uuid"`
		BrandName           string  `json:"brand_name"`
		CurrentSold         float64 `json:"current_sold"`
		PreviousSold        float64 `json:"previous_sold"`
		CurrentFardes       float64 `json:"current_fardes"`
		PreviousFardes      float64 `json:"previous_fardes"`
		CurrentTotalSold    float64 `json:"current_total_sold"`
		PreviousTotalSold   float64 `json:"previous_total_sold"`
		CurrentSishPercent  float64 `json:"current_sish_percent"`
		PreviousSishPercent float64 `json:"previous_sish_percent"`
		Delta               float64 `json:"delta"`
		VelocityDelta       float64 `json:"velocity_delta"`
		Trend               string  `json:"trend"`
	}

	var results []EvoRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH evolution", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SISH Period-over-Period Evolution", "data": results})
}

// SISHGapAnalysis — brands below a target SISH% threshold.
// ?target=25.0  (default: equal share = 100 / brand_count)
//
//	For each brand:
//	  sish_percent        — actual SISH%
//	  sos_percent         — actual SOS% (for comparison)
//	  target_sish         — target threshold
//	  gap                 — target - actual (positive = under-performing)
//	  gap_units           — extra units to sell to reach target
//	  velocity_index      — current sell-through speed
//	  status              — "above_target" | "below_target"
func SISHGapAnalysis(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	var targetSish float64
	if t := c.Query("target"); t != "" {
		var parsed float64
		if n, _ := fmt.Sscanf(t, "%f", &parsed); n == 1 {
			targetSish = parsed
		}
	}
	if targetSish == 0 {
		var brandCount int64
		db.Raw(`
			SELECT COUNT(DISTINCT pfi.brand_uuid)
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
		`, params).Scan(&brandCount)
		if brandCount > 0 {
			targetSish = 100.0 / float64(brandCount)
		} else {
			targetSish = 25.0
		}
	}
	params["target_sish"] = targetSish

	sqlQuery := `
		WITH totals AS (
			SELECT SUM(pfi.sold) AS total_sold, SUM(pfi.number_farde) AS total_fardes,
			       COUNT(*) AS brand_count
			FROM (
				SELECT pfi.brand_uuid,
					SUM(pfi.sold)         AS brand_sold,
					SUM(pfi.number_farde) AS brand_fardes
				FROM pos_form_items pfi
				INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
				WHERE pf.country_uuid = @country_uuid
				  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
				  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
				  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
				  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
				  AND pf.created_at BETWEEN @start_date AND @end_date
				  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
				GROUP BY pfi.brand_uuid
			) sub
		),
		brand_data AS (
			SELECT pfi.brand_uuid,
				SUM(pfi.sold)         AS brand_sold,
				SUM(pfi.number_farde) AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.uuid                                                                     AS brand_uuid,
			b.name                                                                     AS brand_name,
			bd.brand_sold,
			(SELECT total_sold   FROM totals)                                          AS total_sold,
			bd.brand_fardes,
			(SELECT total_fardes FROM totals)                                          AS total_fardes,
			ROUND((bd.brand_sold   * 100.0 / NULLIF((SELECT total_sold  FROM totals),0))::numeric,2) AS sish_percent,
			ROUND((bd.brand_fardes * 100.0 / NULLIF((SELECT total_fardes FROM totals),0))::numeric,2) AS sos_percent,
			ROUND((100.0 / NULLIF((SELECT brand_count FROM totals),0))::numeric,2)    AS equal_share_target,
			ROUND((@target_sish - bd.brand_sold * 100.0 / NULLIF((SELECT total_sold FROM totals),0))::numeric,2) AS gap,
			ROUND(GREATEST(0, (
				(@target_sish / 100.0) * (SELECT total_sold FROM totals) - bd.brand_sold
			))::numeric, 0)                                                            AS gap_units,
			ROUND(CASE WHEN bd.brand_fardes > 0
			      THEN (bd.brand_sold/(SELECT total_sold FROM totals)) /
			           (bd.brand_fardes/(SELECT total_fardes FROM totals))
			      ELSE 0 END::numeric, 3)                                              AS velocity_index,
			CASE WHEN bd.brand_sold * 100.0 / NULLIF((SELECT total_sold FROM totals),0) >= @target_sish
			     THEN 'above_target' ELSE 'below_target' END                          AS status
		FROM brand_data bd
		INNER JOIN brands b ON b.uuid = bd.brand_uuid
		ORDER BY gap DESC
	`

	type GapRow struct {
		BrandUUID        string  `json:"brand_uuid"`
		BrandName        string  `json:"brand_name"`
		BrandSold        float64 `json:"brand_sold"`
		TotalSold        float64 `json:"total_sold"`
		BrandFardes      float64 `json:"brand_fardes"`
		TotalFardes      float64 `json:"total_fardes"`
		SishPercent      float64 `json:"sish_percent"`
		SosPercent       float64 `json:"sos_percent"`
		EqualShareTarget float64 `json:"equal_share_target"`
		Gap              float64 `json:"gap"`
		GapUnits         float64 `json:"gap_units"`
		VelocityIndex    float64 `json:"velocity_index"`
		Status           string  `json:"status"`
	}

	var results []GapRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH gap analysis", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"status":      "success",
		"message":     "SISH Gap Analysis",
		"target_sish": targetSish,
		"data":        results,
	})
}

// SISHVsSosCorrelation — Cross-metric analysis: SISH% vs SOS% per brand.
//
//	Reveals 4 strategic positions:
//	  "fast_leader"      — high SISH (≥SOS threshold) AND high SOS (≥33%)
//	                        sells it AND stocks it → dominant brand
//	  "sell_through_star"— high SISH but low SOS
//	                        sells more than it stocks → reorder urgently
//	  "shelf_hoarder"    — low SISH but high SOS
//	                        stocks more than it sells → execution or taste problem
//	  "underperformer"   — low SISH and low SOS → not present and not moving
func SISHVsSosCorrelation(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH totals AS (
			SELECT SUM(pfi.sold) AS total_sold, SUM(pfi.number_farde) AS total_fardes
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
		brand_data AS (
			SELECT pfi.brand_uuid,
				SUM(pfi.sold)               AS brand_sold,
				SUM(pfi.number_farde)       AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid) AS pos_count
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.uuid                                                                     AS brand_uuid,
			b.name                                                                     AS brand_name,
			bd.brand_sold,
			bd.brand_fardes,
			(SELECT total_sold   FROM totals)                                          AS total_sold,
			(SELECT total_fardes FROM totals)                                          AS total_fardes,
			ROUND((bd.brand_sold   * 100.0 / NULLIF((SELECT total_sold  FROM totals),0))::numeric,2) AS sish_percent,
			ROUND((bd.brand_fardes * 100.0 / NULLIF((SELECT total_fardes FROM totals),0))::numeric,2) AS sos_percent,
			ROUND((bd.brand_sold * 100.0  / NULLIF((SELECT total_sold  FROM totals),0) -
			       bd.brand_fardes * 100.0/ NULLIF((SELECT total_fardes FROM totals),0))::numeric,2) AS delta_sish_sos,
			ROUND(CASE WHEN bd.brand_fardes > 0
			      THEN (bd.brand_sold/(SELECT total_sold FROM totals)) /
			           (bd.brand_fardes/(SELECT total_fardes FROM totals))
			      ELSE 0 END::numeric, 3)                                              AS velocity_index,
			CASE
				WHEN (bd.brand_sold*100.0/NULLIF((SELECT total_sold  FROM totals),0)) >=
				     (SELECT AVG(brand_sold*100.0/NULLIF(total_sold,0)) FROM (
				        SELECT pfi2.brand_uuid,
				               SUM(pfi2.sold) AS brand_sold,
				               (SELECT SUM(sold) FROM pos_form_items) AS total_sold
				        FROM pos_form_items pfi2 GROUP BY pfi2.brand_uuid
				     ) sub)
				 AND (bd.brand_fardes*100.0/NULLIF((SELECT total_fardes FROM totals),0)) >= 33
				THEN 'fast_leader'
				WHEN (bd.brand_sold*100.0/NULLIF((SELECT total_sold  FROM totals),0)) >=
				     (bd.brand_fardes*100.0/NULLIF((SELECT total_fardes FROM totals),0))
				 AND (bd.brand_fardes*100.0/NULLIF((SELECT total_fardes FROM totals),0)) < 33
				THEN 'sell_through_star'
				WHEN (bd.brand_sold*100.0/NULLIF((SELECT total_sold  FROM totals),0)) <
				     (bd.brand_fardes*100.0/NULLIF((SELECT total_fardes FROM totals),0))
				 AND (bd.brand_fardes*100.0/NULLIF((SELECT total_fardes FROM totals),0)) >= 33
				THEN 'shelf_hoarder'
				ELSE 'underperformer'
			END AS position
		FROM brand_data bd
		INNER JOIN brands b ON b.uuid = bd.brand_uuid
		ORDER BY sish_percent DESC
	`

	type CorrRow struct {
		BrandUUID     string  `json:"brand_uuid"`
		BrandName     string  `json:"brand_name"`
		BrandSold     float64 `json:"brand_sold"`
		BrandFardes   float64 `json:"brand_fardes"`
		TotalSold     float64 `json:"total_sold"`
		TotalFardes   float64 `json:"total_fardes"`
		SishPercent   float64 `json:"sish_percent"`
		SosPercent    float64 `json:"sos_percent"`
		DeltaSishSos  float64 `json:"delta_sish_sos"`
		VelocityIndex float64 `json:"velocity_index"`
		Position      string  `json:"position"`
	}

	var results []CorrRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH vs SOS correlation", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SISH vs SOS Correlation Matrix", "data": results})
}

// SISHPosDrillDown — deep-dive on a single brand's in-shop sales performance per POS.
// Required: ?brand_uuid=...
//
//	Per POS:
//	  pos_name, pos_shop, pos_type
//	  brand_sold, total_sold_at_pos, sish_at_pos
//	  brand_fardes, total_fardes_at_pos, sos_at_pos
//	  velocity_at_pos — sish_at_pos / sos_at_pos
//	  visit_count, last_visit
func SISHPosDrillDown(c *fiber.Ctx) error {
	db := database.DB
	params := extractSISHParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	brandUUID := c.Query("brand_uuid")
	if brandUUID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "brand_uuid is required",
		})
	}
	params["brand_uuid"] = brandUUID

	sqlQuery := `
		WITH pos_totals AS (
			-- Total sold + fardes per pos_form (all brands)
			SELECT pos_form_uuid,
				SUM(sold)         AS total_sold_at_pos,
				SUM(number_farde) AS total_fardes_at_pos
			FROM pos_form_items WHERE deleted_at IS NULL GROUP BY pos_form_uuid
		),
		brand_per_form AS (
			-- Brand sold + fardes per pos_form
			SELECT pos_form_uuid,
				SUM(sold)         AS brand_sold_at_form,
				SUM(number_farde) AS brand_fardes_at_form
			FROM pos_form_items
			WHERE brand_uuid = @brand_uuid AND deleted_at IS NULL
			GROUP BY pos_form_uuid
		)
		SELECT
			p.uuid                                                                    AS pos_uuid,
			p.name                                                                    AS pos_name,
			p.shop                                                                    AS pos_shop,
			p.postype                                                                 AS pos_type,
			SUM(COALESCE(bpf.brand_sold_at_form, 0))                                 AS brand_sold,
			SUM(pt.total_sold_at_pos)                                                AS total_sold_at_pos,
			SUM(COALESCE(bpf.brand_fardes_at_form, 0))                               AS brand_fardes,
			SUM(pt.total_fardes_at_pos)                                              AS total_fardes_at_pos,
			COUNT(pf.uuid)                                                            AS visit_count,
			MAX(pf.created_at)                                                        AS last_visit,
			ROUND(AVG(
				CASE WHEN pt.total_sold_at_pos > 0
				THEN COALESCE(bpf.brand_sold_at_form,0) * 100.0 / pt.total_sold_at_pos
				ELSE 0 END
			)::numeric, 2)                                                            AS avg_sish_at_pos,
			ROUND(AVG(
				CASE WHEN pt.total_fardes_at_pos > 0
				THEN COALESCE(bpf.brand_fardes_at_form,0) * 100.0 / pt.total_fardes_at_pos
				ELSE 0 END
			)::numeric, 2)                                                            AS avg_sos_at_pos,
			ROUND((SUM(COALESCE(bpf.brand_sold_at_form,0)) * 100.0 /
			       NULLIF(SUM(pt.total_sold_at_pos), 0))::numeric, 2)               AS sish_at_pos,
			ROUND((SUM(COALESCE(bpf.brand_fardes_at_form,0)) * 100.0 /
			       NULLIF(SUM(pt.total_fardes_at_pos), 0))::numeric, 2)             AS sos_at_pos,
			ROUND(CASE WHEN SUM(COALESCE(bpf.brand_fardes_at_form,0)) > 0
			      THEN (SUM(COALESCE(bpf.brand_sold_at_form,0))/NULLIF(SUM(pt.total_sold_at_pos),0)) /
			           (SUM(COALESCE(bpf.brand_fardes_at_form,0))/NULLIF(SUM(pt.total_fardes_at_pos),0))
			      ELSE 0 END::numeric, 3)                                            AS velocity_at_pos
		FROM pos_forms pf
		INNER JOIN pos p    ON p.uuid = pf.pos_uuid
		INNER JOIN pos_totals pt ON pt.pos_form_uuid = pf.uuid
		LEFT  JOIN brand_per_form bpf ON bpf.pos_form_uuid = pf.uuid
		WHERE pf.country_uuid = @country_uuid
		  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
		  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
		  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
		  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
		  AND pf.created_at BETWEEN @start_date AND @end_date
		  AND pf.deleted_at IS NULL
		GROUP BY p.uuid, p.name, p.shop, p.postype
		ORDER BY sish_at_pos DESC
		LIMIT 100
	`

	type DrillRow struct {
		PosUUID          string  `json:"pos_uuid"`
		PosName          string  `json:"pos_name"`
		PosShop          string  `json:"pos_shop"`
		PosType          string  `json:"pos_type"`
		BrandSold        float64 `json:"brand_sold"`
		TotalSoldAtPos   float64 `json:"total_sold_at_pos"`
		BrandFardes      float64 `json:"brand_fardes"`
		TotalFardesAtPos float64 `json:"total_fardes_at_pos"`
		VisitCount       int64   `json:"visit_count"`
		LastVisit        string  `json:"last_visit"`
		AvgSishAtPos     float64 `json:"avg_sish_at_pos"`
		AvgSosAtPos      float64 `json:"avg_sos_at_pos"`
		SishAtPos        float64 `json:"sish_at_pos"`
		SosAtPos         float64 `json:"sos_at_pos"`
		VelocityAtPos    float64 `json:"velocity_at_pos"`
	}

	var results []DrillRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SISH POS drill-down", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "SISH POS-Level Drill-Down",
		"brand_uuid": brandUUID,
		"data":       results,
	})
}

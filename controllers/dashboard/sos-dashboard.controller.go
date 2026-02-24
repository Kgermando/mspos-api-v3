package dashboard

import (
	"fmt"
	"time"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════════╗
// ║          SHARE OF STOCK (SOS) DASHBOARD — SHELF SPACE ANALYTICS            ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  Share of Stock % = SUM(brand fardes at POS)                                ║
// ║                    ────────────────────────────  × 100                     ║
// ║                    SUM(ALL brands fardes at POS)                            ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  A high SOS% means a brand dominates shelf space.                           ║
// ║  A low  SOS% signals a brand is losing shelf real-estate.                   ║
// ╠══════════════════════════════════════════════════════════════════════════════╣
// ║  SECTION 1 — TABLE VIEWS    : Province / Area / SubArea / Commune           ║
// ║  SECTION 2 — BAR CHARTS     : Province / Area / SubArea / Commune           ║
// ║  SECTION 3 — TREND CHART    : SOS% by Brand per Month                       ║
// ║  SECTION 4 — POWER ANALYTICS: Summary KPI / Brand Ranking / HHI Index       ║
// ║  SECTION 5 — ADVANCED       : Heatmap / Evolution / Gap Analysis / Pos Drill ║
// ╚══════════════════════════════════════════════════════════════════════════════╝

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SECTION 1 — TABLE VIEWS
//
//	Each row = (territory × brand) with:
//	  brand_fardes   — total fardes of that brand in the territory
//	  total_fardes   — total fardes of ALL brands in the territory
//	  sos_percent    — brand_fardes / total_fardes × 100
//	  avg_sos_per_pos— average SOS% per individual POS (per-visit weighted)
//	  pos_count      — distinct POS visited carrying this brand
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// SOSTableViewProvince — SOS% breakdown per brand at Province level
func SOSTableViewProvince(c *fiber.Ctx) error {
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
		WITH total_shelf AS (
			-- Total fardes of ALL brands per province (the denominator)
			SELECT
				pf.province_uuid,
				SUM(pfi.number_farde) AS total_fardes,
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
			GROUP BY pf.province_uuid
		),
		brand_shelf AS (
			-- Fardes per brand per province (the numerator)
			SELECT
				pf.province_uuid,
				pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)  AS pos_count,
				-- Per-POS weighted SOS: AVG over each POS of brand_farde/pos_total_farde
				ROUND(AVG(
					CASE WHEN pos_total.pos_fardes > 0
					     THEN pfi.number_farde * 100.0 / pos_total.pos_fardes
					     ELSE 0 END
				)::numeric, 2) AS avg_sos_per_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			-- Per-POS total fardes for the weighted average
			INNER JOIN (
				SELECT pos_form_uuid, SUM(number_farde) AS pos_fardes
				FROM pos_form_items
				WHERE deleted_at IS NULL
				GROUP BY pos_form_uuid
			) pos_total ON pos_total.pos_form_uuid = pfi.pos_form_uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.province_uuid, pfi.brand_uuid
		)
		SELECT
			pr.name                                                                  AS territory_name,
			pr.uuid                                                                  AS territory_uuid,
			'province'                                                               AS territory_level,
			b.name                                                                   AS brand_name,
			b.uuid                                                                   AS brand_uuid,
			bs.brand_fardes,
			COALESCE(ts.total_fardes, 0)                                             AS total_fardes,
			bs.pos_count,
			COALESCE(ts.total_pos, 0)                                                AS total_pos,
			bs.avg_sos_per_pos,
			ROUND((bs.brand_fardes * 100.0 /
			       NULLIF(ts.total_fardes, 0))::numeric, 2)                         AS sos_percent
		FROM brand_shelf bs
		INNER JOIN brands    b  ON b.uuid  = bs.brand_uuid
		INNER JOIN provinces pr ON pr.uuid = bs.province_uuid
		LEFT  JOIN total_shelf ts ON ts.province_uuid = bs.province_uuid
		ORDER BY pr.name, sos_percent DESC
	`

	type SOSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosCount       int64   `json:"pos_count"`
		TotalPos       int64   `json:"total_pos"`
		AvgSosPerPos   float64 `json:"avg_sos_per_pos"`
		SosPercent     float64 `json:"sos_percent"`
	}

	var results []SOSRow
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
			"status": "error", "message": "Failed to fetch SOS province table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Province Table", "data": results})
}

// SOSTableViewArea — SOS% breakdown per brand at Area level
func SOSTableViewArea(c *fiber.Ctx) error {
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
		WITH total_shelf AS (
			SELECT pf.area_uuid,
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
		brand_shelf AS (
			SELECT pf.area_uuid, pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)  AS pos_count,
				ROUND(AVG(
					CASE WHEN pos_total.pos_fardes > 0
					     THEN pfi.number_farde * 100.0 / pos_total.pos_fardes
					     ELSE 0 END
				)::numeric, 2)               AS avg_sos_per_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN (
				SELECT pos_form_uuid, SUM(number_farde) AS pos_fardes
				FROM pos_form_items WHERE deleted_at IS NULL
				GROUP BY pos_form_uuid
			) pos_total ON pos_total.pos_form_uuid = pfi.pos_form_uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.area_uuid, pfi.brand_uuid
		)
		SELECT
			a.name                                                             AS territory_name,
			a.uuid                                                             AS territory_uuid,
			'area'                                                             AS territory_level,
			b.name                                                             AS brand_name,
			b.uuid                                                             AS brand_uuid,
			bs.brand_fardes,
			COALESCE(ts.total_fardes, 0)                                       AS total_fardes,
			bs.pos_count,
			COALESCE(ts.total_pos, 0)                                          AS total_pos,
			bs.avg_sos_per_pos,
			ROUND((bs.brand_fardes * 100.0 /
			       NULLIF(ts.total_fardes, 0))::numeric, 2)                   AS sos_percent
		FROM brand_shelf bs
		INNER JOIN brands b ON b.uuid = bs.brand_uuid
		INNER JOIN areas  a ON a.uuid = bs.area_uuid
		LEFT  JOIN total_shelf ts ON ts.area_uuid = bs.area_uuid
		ORDER BY a.name, sos_percent DESC
	`

	type SOSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosCount       int64   `json:"pos_count"`
		TotalPos       int64   `json:"total_pos"`
		AvgSosPerPos   float64 `json:"avg_sos_per_pos"`
		SosPercent     float64 `json:"sos_percent"`
	}

	var results []SOSRow
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
			"status": "error", "message": "Failed to fetch SOS area table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Area Table", "data": results})
}

// SOSTableViewSubArea — SOS% breakdown per brand at SubArea level
func SOSTableViewSubArea(c *fiber.Ctx) error {
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
		WITH total_shelf AS (
			SELECT pf.sub_area_uuid,
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
		brand_shelf AS (
			SELECT pf.sub_area_uuid, pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)  AS pos_count,
				ROUND(AVG(
					CASE WHEN pos_total.pos_fardes > 0
					     THEN pfi.number_farde * 100.0 / pos_total.pos_fardes
					     ELSE 0 END
				)::numeric, 2)               AS avg_sos_per_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN (
				SELECT pos_form_uuid, SUM(number_farde) AS pos_fardes
				FROM pos_form_items WHERE deleted_at IS NULL
				GROUP BY pos_form_uuid
			) pos_total ON pos_total.pos_form_uuid = pfi.pos_form_uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.sub_area_uuid, pfi.brand_uuid
		)
		SELECT
			sa.name                                                            AS territory_name,
			sa.uuid                                                            AS territory_uuid,
			'subarea'                                                          AS territory_level,
			b.name                                                             AS brand_name,
			b.uuid                                                             AS brand_uuid,
			bs.brand_fardes,
			COALESCE(ts.total_fardes, 0)                                       AS total_fardes,
			bs.pos_count,
			COALESCE(ts.total_pos, 0)                                          AS total_pos,
			bs.avg_sos_per_pos,
			ROUND((bs.brand_fardes * 100.0 /
			       NULLIF(ts.total_fardes, 0))::numeric, 2)                   AS sos_percent
		FROM brand_shelf bs
		INNER JOIN brands    b  ON b.uuid  = bs.brand_uuid
		INNER JOIN sub_areas sa ON sa.uuid = bs.sub_area_uuid
		LEFT  JOIN total_shelf ts ON ts.sub_area_uuid = bs.sub_area_uuid
		ORDER BY sa.name, sos_percent DESC
	`

	type SOSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosCount       int64   `json:"pos_count"`
		TotalPos       int64   `json:"total_pos"`
		AvgSosPerPos   float64 `json:"avg_sos_per_pos"`
		SosPercent     float64 `json:"sos_percent"`
	}

	var results []SOSRow
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
			"status": "error", "message": "Failed to fetch SOS subarea table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS SubArea Table", "data": results})
}

// SOSTableViewCommune — SOS% breakdown per brand at Commune level
func SOSTableViewCommune(c *fiber.Ctx) error {
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
		WITH total_shelf AS (
			SELECT pf.commune_uuid,
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
		brand_shelf AS (
			SELECT pf.commune_uuid, pfi.brand_uuid,
				SUM(pfi.number_farde)        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)  AS pos_count,
				ROUND(AVG(
					CASE WHEN pos_total.pos_fardes > 0
					     THEN pfi.number_farde * 100.0 / pos_total.pos_fardes
					     ELSE 0 END
				)::numeric, 2)               AS avg_sos_per_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			INNER JOIN (
				SELECT pos_form_uuid, SUM(number_farde) AS pos_fardes
				FROM pos_form_items WHERE deleted_at IS NULL
				GROUP BY pos_form_uuid
			) pos_total ON pos_total.pos_form_uuid = pfi.pos_form_uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pf.commune_uuid, pfi.brand_uuid
		)
		SELECT
			c.name                                                             AS territory_name,
			c.uuid                                                             AS territory_uuid,
			'commune'                                                          AS territory_level,
			b.name                                                             AS brand_name,
			b.uuid                                                             AS brand_uuid,
			bs.brand_fardes,
			COALESCE(ts.total_fardes, 0)                                       AS total_fardes,
			bs.pos_count,
			COALESCE(ts.total_pos, 0)                                          AS total_pos,
			bs.avg_sos_per_pos,
			ROUND((bs.brand_fardes * 100.0 /
			       NULLIF(ts.total_fardes, 0))::numeric, 2)                   AS sos_percent
		FROM brand_shelf bs
		INNER JOIN brands   b ON b.uuid = bs.brand_uuid
		INNER JOIN communes c ON c.uuid = bs.commune_uuid
		LEFT  JOIN total_shelf ts ON ts.commune_uuid = bs.commune_uuid
		ORDER BY c.name, sos_percent DESC
	`

	type SOSRow struct {
		TerritoryName  string  `json:"territory_name"`
		TerritoryUUID  string  `json:"territory_uuid"`
		TerritoryLevel string  `json:"territory_level"`
		BrandName      string  `json:"brand_name"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandFardes    float64 `json:"brand_fardes"`
		TotalFardes    float64 `json:"total_fardes"`
		PosCount       int64   `json:"pos_count"`
		TotalPos       int64   `json:"total_pos"`
		AvgSosPerPos   float64 `json:"avg_sos_per_pos"`
		SosPercent     float64 `json:"sos_percent"`
	}

	var results []SOSRow
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
			"status": "error", "message": "Failed to fetch SOS commune table", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Commune Table", "data": results})
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SECTION 2 — BAR CHARTS
//
//	Each endpoint returns the data shaped for a grouped bar chart:
//	  [{ territory_name, brands: [{ brand_name, sos_percent }] }]
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

func sosBarChartQuery(level string) string {
	dims := map[string]struct {
		sel, grp, join, order string
	}{
		"province": {
			sel: "pf.province_uuid AS dim_uuid", grp: "pf.province_uuid",
			join: "INNER JOIN provinces t ON t.uuid = bs.dim_uuid", order: "t.name",
		},
		"area": {
			sel: "pf.area_uuid AS dim_uuid", grp: "pf.area_uuid",
			join: "INNER JOIN areas t ON t.uuid = bs.dim_uuid", order: "t.name",
		},
		"subarea": {
			sel: "pf.sub_area_uuid AS dim_uuid", grp: "pf.sub_area_uuid",
			join: "INNER JOIN sub_areas t ON t.uuid = bs.dim_uuid", order: "t.name",
		},
		"commune": {
			sel: "pf.commune_uuid AS dim_uuid", grp: "pf.commune_uuid",
			join: "INNER JOIN communes t ON t.uuid = bs.dim_uuid", order: "t.name",
		},
	}
	d := dims[level]
	return `
		WITH total_shelf AS (
			SELECT ` + d.grp + ` AS dim_uuid, SUM(pfi.number_farde) AS total_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY ` + d.grp + `
		),
		brand_shelf AS (
			SELECT ` + d.sel + `, pfi.brand_uuid, SUM(pfi.number_farde) AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY ` + d.grp + `, pfi.brand_uuid
		)
		SELECT
			t.name                                                             AS territory_name,
			t.uuid                                                             AS territory_uuid,
			b.name                                                             AS brand_name,
			b.uuid                                                             AS brand_uuid,
			bs.brand_fardes,
			COALESCE(ts.total_fardes, 0)                                       AS total_fardes,
			ROUND((bs.brand_fardes * 100.0 /
			       NULLIF(ts.total_fardes, 0))::numeric, 2)                   AS sos_percent
		FROM brand_shelf bs
		` + d.join + `
		INNER JOIN brands b ON b.uuid = bs.brand_uuid
		LEFT  JOIN total_shelf ts ON ts.dim_uuid = bs.dim_uuid
		ORDER BY ` + d.order + `, sos_percent DESC
	`
}

type sosBarRaw struct {
	TerritoryName string  `json:"territory_name"`
	TerritoryUUID string  `json:"territory_uuid"`
	BrandName     string  `json:"brand_name"`
	BrandUUID     string  `json:"brand_uuid"`
	BrandFardes   float64 `json:"brand_fardes"`
	TotalFardes   float64 `json:"total_fardes"`
	SosPercent    float64 `json:"sos_percent"`
}

// groupSOSBarChart assembles flat rows into the chart-ready grouped structure
func groupSOSBarChart(raw []sosBarRaw) []map[string]interface{} {
	order := []string{}
	index := map[string]int{}
	grouped := []map[string]interface{}{}

	for _, r := range raw {
		if _, exists := index[r.TerritoryUUID]; !exists {
			index[r.TerritoryUUID] = len(grouped)
			order = append(order, r.TerritoryUUID)
			grouped = append(grouped, map[string]interface{}{
				"territory_name": r.TerritoryName,
				"territory_uuid": r.TerritoryUUID,
				"total_fardes":   r.TotalFardes,
				"brands":         []map[string]interface{}{},
			})
		}
		idx := index[r.TerritoryUUID]
		grouped[idx]["brands"] = append(
			grouped[idx]["brands"].([]map[string]interface{}),
			map[string]interface{}{
				"brand_name":   r.BrandName,
				"brand_uuid":   r.BrandUUID,
				"brand_fardes": r.BrandFardes,
				"sos_percent":  r.SosPercent,
			},
		)
	}
	_ = order
	return grouped
}

// SOSBarChartProvince — grouped bar chart data at Province level
func SOSBarChartProvince(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	var raw []sosBarRaw
	if err := db.Raw(sosBarChartQuery("province"), params).Scan(&raw).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS bar chart province", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Bar Chart Province", "data": groupSOSBarChart(raw)})
}

// SOSBarChartArea — grouped bar chart data at Area level
func SOSBarChartArea(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	var raw []sosBarRaw
	if err := db.Raw(sosBarChartQuery("area"), params).Scan(&raw).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS bar chart area", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Bar Chart Area", "data": groupSOSBarChart(raw)})
}

// SOSBarChartSubArea — grouped bar chart data at SubArea level
func SOSBarChartSubArea(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	var raw []sosBarRaw
	if err := db.Raw(sosBarChartQuery("subarea"), params).Scan(&raw).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS bar chart subarea", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Bar Chart SubArea", "data": groupSOSBarChart(raw)})
}

// SOSBarChartCommune — grouped bar chart data at Commune level
func SOSBarChartCommune(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	var raw []sosBarRaw
	if err := db.Raw(sosBarChartQuery("commune"), params).Scan(&raw).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS bar chart commune", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Bar Chart Commune", "data": groupSOSBarChart(raw)})
}

// extractSOSParams is a helper that reads common query params.
// Returns nil when required params are missing.
func extractSOSParams(c *fiber.Ctx) map[string]interface{} {
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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SECTION 3 — MONTHLY TREND LINE CHART
//
//	Returns one data point per (month × brand):
//	  month           — "YYYY-MM"
//	  brand_name
//	  brand_fardes    — total fardes of that brand in the month
//	  total_fardes    — total fardes all brands in the month
//	  sos_percent     — brand_fardes / total_fardes × 100
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// SOSLineChartByMonth — SOS% trend by brand per month
func SOSLineChartByMonth(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH monthly_total AS (
			SELECT
				TO_CHAR(pf.created_at, 'YYYY-MM') AS month,
				SUM(pfi.number_farde)              AS total_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY TO_CHAR(pf.created_at, 'YYYY-MM')
		),
		monthly_brand AS (
			SELECT
				TO_CHAR(pf.created_at, 'YYYY-MM') AS month,
				pfi.brand_uuid,
				SUM(pfi.number_farde)              AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY TO_CHAR(pf.created_at, 'YYYY-MM'), pfi.brand_uuid
		)
		SELECT
			mb.month,
			b.name                                                              AS brand_name,
			b.uuid                                                              AS brand_uuid,
			mb.brand_fardes,
			COALESCE(mt.total_fardes, 0)                                        AS total_fardes,
			ROUND((mb.brand_fardes * 100.0 /
			       NULLIF(mt.total_fardes, 0))::numeric, 2)                    AS sos_percent
		FROM monthly_brand mb
		INNER JOIN brands b ON b.uuid = mb.brand_uuid
		LEFT  JOIN monthly_total mt ON mt.month = mb.month
		ORDER BY mb.month, sos_percent DESC
	`

	type TrendRow struct {
		Month       string  `json:"month"`
		BrandName   string  `json:"brand_name"`
		BrandUUID   string  `json:"brand_uuid"`
		BrandFardes float64 `json:"brand_fardes"`
		TotalFardes float64 `json:"total_fardes"`
		SosPercent  float64 `json:"sos_percent"`
	}

	var raw []TrendRow
	if err := db.Raw(sqlQuery, params).Scan(&raw).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS monthly trend", "error": err.Error(),
		})
	}

	// Pivot: one series per brand, x-axis = months
	type brandSeries struct {
		Name   string                   `json:"brand_name"`
		UUID   string                   `json:"brand_uuid"`
		Points []map[string]interface{} `json:"data"`
	}
	brandMap := map[string]*brandSeries{}
	brandOrder := []string{}

	for _, r := range raw {
		if _, ok := brandMap[r.BrandUUID]; !ok {
			brandOrder = append(brandOrder, r.BrandUUID)
			brandMap[r.BrandUUID] = &brandSeries{Name: r.BrandName, UUID: r.BrandUUID}
		}
		brandMap[r.BrandUUID].Points = append(brandMap[r.BrandUUID].Points, map[string]interface{}{
			"month":        r.Month,
			"brand_fardes": r.BrandFardes,
			"total_fardes": r.TotalFardes,
			"sos_percent":  r.SosPercent,
		})
	}

	series := make([]interface{}, 0, len(brandOrder))
	for _, uuid := range brandOrder {
		series = append(series, brandMap[uuid])
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "SOS Monthly Trend by Brand",
		"data":    series,
	})
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SECTION 4 — POWER ANALYTICS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// SOSSummaryKPI — single-card executive metrics:
//
//	total_fardes        — all fardes across all brands in the period
//	total_pos_visited   — distinct POS visited
//	dominant_brand      — brand with the highest SOS%
//	dominant_sos        — SOS% of the dominant brand
//	weakest_brand       — brand with the lowest SOS%
//	weakest_sos         — SOS% of the weakest brand
//	brand_count         — number of distinct brands on shelf
//	hhi_index           — Herfindahl-Hirschman Index (market concentration 0–10000)
//	                      < 1500 competitive | 1500–2500 moderate | > 2500 concentrated
func SOSSummaryKPI(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH shelf AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_fardes
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
		grand_total AS (
			SELECT SUM(brand_fardes) AS total FROM shelf
		),
		brand_sos AS (
			SELECT
				s.brand_uuid,
				b.name                                                             AS brand_name,
				s.brand_fardes,
				(SELECT total FROM grand_total)                                    AS total_fardes,
				ROUND((s.brand_fardes * 100.0 /
				       NULLIF((SELECT total FROM grand_total), 0))::numeric, 2)   AS sos_pct
			FROM shelf s
			INNER JOIN brands b ON b.uuid = s.brand_uuid
		),
		pos_visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS cnt
			FROM pos_forms pf
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		)
		SELECT
			(SELECT total FROM grand_total)                                        AS total_fardes,
			(SELECT cnt FROM pos_visited)                                          AS total_pos_visited,
			(SELECT brand_name FROM brand_sos ORDER BY sos_pct DESC LIMIT 1)      AS dominant_brand,
			(SELECT sos_pct    FROM brand_sos ORDER BY sos_pct DESC LIMIT 1)      AS dominant_sos,
			(SELECT brand_name FROM brand_sos ORDER BY sos_pct ASC  LIMIT 1)      AS weakest_brand,
			(SELECT sos_pct    FROM brand_sos ORDER BY sos_pct ASC  LIMIT 1)      AS weakest_sos,
			(SELECT COUNT(*)   FROM brand_sos)                                     AS brand_count,
			ROUND((SELECT SUM(sos_pct * sos_pct) FROM brand_sos)::numeric, 2)     AS hhi_index
		FROM (SELECT 1) _
	`

	type KPIResult struct {
		TotalFardes     float64 `json:"total_fardes"`
		TotalPosVisited int64   `json:"total_pos_visited"`
		DominantBrand   string  `json:"dominant_brand"`
		DominantSos     float64 `json:"dominant_sos"`
		WeakestBrand    string  `json:"weakest_brand"`
		WeakestSos      float64 `json:"weakest_sos"`
		BrandCount      int64   `json:"brand_count"`
		HHIIndex        float64 `json:"hhi_index"`
	}

	var result KPIResult
	if err := db.Raw(sqlQuery, params).Scan(&result).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS summary KPI", "error": err.Error(),
		})
	}

	var marketStructure string
	switch {
	case result.HHIIndex > 2500:
		marketStructure = "concentrated"
	case result.HHIIndex >= 1500:
		marketStructure = "moderate"
	default:
		marketStructure = "competitive"
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "SOS Summary KPI",
		"data": fiber.Map{
			"total_fardes":      result.TotalFardes,
			"total_pos_visited": result.TotalPosVisited,
			"dominant_brand":    result.DominantBrand,
			"dominant_sos":      result.DominantSos,
			"weakest_brand":     result.WeakestBrand,
			"weakest_sos":       result.WeakestSos,
			"brand_count":       result.BrandCount,
			"hhi_index":         result.HHIIndex,
			"market_structure":  marketStructure,
		},
	})
}

// SOSBrandRanking — all brands ranked by SOS% (highest shelf space first).
// Includes:
//   - rank, brand_name, brand_fardes, total_fardes, sos_percent
//   - dominance: "leader" (>33%), "challenger" (15-33%), "follower" (<15%)
//   - cumulative_sos — rolling cumulative share (Pareto 80/20 chart)
func SOSBrandRanking(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	sqlQuery := `
		WITH shelf AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_fardes
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
		grand_total AS (SELECT SUM(brand_fardes) AS total FROM shelf),
		ranked AS (
			SELECT
				ROW_NUMBER() OVER (ORDER BY s.brand_fardes DESC)                  AS rank,
				b.name                                                             AS brand_name,
				b.uuid                                                             AS brand_uuid,
				s.brand_fardes,
				(SELECT total FROM grand_total)                                    AS total_fardes,
				ROUND((s.brand_fardes * 100.0 /
				       NULLIF((SELECT total FROM grand_total), 0))::numeric, 2)   AS sos_percent
			FROM shelf s
			INNER JOIN brands b ON b.uuid = s.brand_uuid
		)
		SELECT
			rank,
			brand_name,
			brand_uuid,
			brand_fardes,
			total_fardes,
			sos_percent,
			ROUND(SUM(sos_percent) OVER (ORDER BY rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::numeric, 2) AS cumulative_sos,
			CASE
				WHEN sos_percent > 33 THEN 'leader'
				WHEN sos_percent > 15 THEN 'challenger'
				ELSE                       'follower'
			END AS dominance
		FROM ranked
		ORDER BY rank
	`

	type RankRow struct {
		Rank          int64   `json:"rank"`
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		BrandFardes   float64 `json:"brand_fardes"`
		TotalFardes   float64 `json:"total_fardes"`
		SosPercent    float64 `json:"sos_percent"`
		CumulativeSos float64 `json:"cumulative_sos"`
		Dominance     string  `json:"dominance"`
	}

	var results []RankRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS brand ranking", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Brand Ranking (highest share first)", "data": results})
}

// SOSConcentrationIndex — HHI market concentration analysis per territory.
// ?level=province|area|subarea|commune
//
//	Returns per territory:
//	  hhi_index, market_structure, top_brand_name, top_brand_sos, brand_count, total_fardes
func SOSConcentrationIndex(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	level := c.Query("level", "province")

	dimSQL := map[string]struct{ dim, join string }{
		"province": {"pf.province_uuid", "INNER JOIN provinces t ON t.uuid = hhi.dim_uuid"},
		"area":     {"pf.area_uuid", "INNER JOIN areas t ON t.uuid = hhi.dim_uuid"},
		"subarea":  {"pf.sub_area_uuid", "INNER JOIN sub_areas t ON t.uuid = hhi.dim_uuid"},
		"commune":  {"pf.commune_uuid", "INNER JOIN communes t ON t.uuid = hhi.dim_uuid"},
	}
	d, ok := dimSQL[level]
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "invalid level; use province|area|subarea|commune",
		})
	}

	sqlQuery := `
		WITH brand_shelf AS (
			SELECT ` + d.dim + ` AS dim_uuid, pfi.brand_uuid,
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
			GROUP BY ` + d.dim + `, pfi.brand_uuid
		),
		dim_total AS (
			SELECT dim_uuid, SUM(brand_fardes) AS total_fardes, COUNT(*) AS brand_count
			FROM brand_shelf GROUP BY dim_uuid
		),
		brand_pct AS (
			SELECT bs.dim_uuid, bs.brand_uuid,
				b.name AS brand_name,
				bs.brand_fardes,
				dt.total_fardes,
				ROUND((bs.brand_fardes * 100.0 / NULLIF(dt.total_fardes, 0))::numeric, 2) AS sos_pct
			FROM brand_shelf bs
			INNER JOIN brands b ON b.uuid = bs.brand_uuid
			INNER JOIN dim_total dt ON dt.dim_uuid = bs.dim_uuid
		),
		hhi_agg AS (
			SELECT dim_uuid,
				ROUND(SUM(sos_pct * sos_pct)::numeric, 2) AS hhi_index
			FROM brand_pct GROUP BY dim_uuid
		)
		SELECT
			t.name                                                             AS territory_name,
			t.uuid                                                             AS territory_uuid,
			hhi.hhi_index,
			CASE
				WHEN hhi.hhi_index > 2500 THEN 'concentrated'
				WHEN hhi.hhi_index >= 1500 THEN 'moderate'
				ELSE 'competitive'
			END                                                                AS market_structure,
			(SELECT bp2.brand_name FROM brand_pct bp2
			 WHERE bp2.dim_uuid = hhi.dim_uuid
			 ORDER BY bp2.sos_pct DESC LIMIT 1)                                AS top_brand_name,
			(SELECT bp2.sos_pct FROM brand_pct bp2
			 WHERE bp2.dim_uuid = hhi.dim_uuid
			 ORDER BY bp2.sos_pct DESC LIMIT 1)                                AS top_brand_sos,
			dt.brand_count,
			dt.total_fardes
		FROM hhi_agg hhi
		` + d.join + `
		INNER JOIN dim_total dt ON dt.dim_uuid = hhi.dim_uuid
		ORDER BY hhi.hhi_index DESC
	`

	type HHIRow struct {
		TerritoryName   string  `json:"territory_name"`
		TerritoryUUID   string  `json:"territory_uuid"`
		HHIIndex        float64 `json:"hhi_index"`
		MarketStructure string  `json:"market_structure"`
		TopBrandName    string  `json:"top_brand_name"`
		TopBrandSos     float64 `json:"top_brand_sos"`
		BrandCount      int64   `json:"brand_count"`
		TotalFardes     float64 `json:"total_fardes"`
	}

	var results []HHIRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS concentration index", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "SOS HHI Market Concentration Index",
		"level":   level,
		"data":    results,
	})
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SECTION 5 — ADVANCED ANALYTICS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// SOSHeatmap — Brand × Territory matrix of SOS%.
// ?level=province|area|subarea|commune
// Returns brands[], territories[], matrix[][] (brand-major order, pivoted server-side)
func SOSHeatmap(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}
	level := c.Query("level", "province")

	dimSQL := map[string]struct{ dim, join string }{
		"province": {"pf.province_uuid", "INNER JOIN provinces t ON t.uuid = bs.dim_uuid"},
		"area":     {"pf.area_uuid", "INNER JOIN areas t ON t.uuid = bs.dim_uuid"},
		"subarea":  {"pf.sub_area_uuid", "INNER JOIN sub_areas t ON t.uuid = bs.dim_uuid"},
		"commune":  {"pf.commune_uuid", "INNER JOIN communes t ON t.uuid = bs.dim_uuid"},
	}
	d, ok := dimSQL[level]
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "invalid level; use province|area|subarea|commune",
		})
	}

	sqlQuery := `
		WITH brand_shelf AS (
			SELECT ` + d.dim + ` AS dim_uuid, pfi.brand_uuid,
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
			GROUP BY ` + d.dim + `, pfi.brand_uuid
		),
		dim_total AS (
			SELECT dim_uuid, SUM(brand_fardes) AS total_fardes
			FROM brand_shelf GROUP BY dim_uuid
		)
		SELECT
			b.name  AS brand_name,
			b.uuid  AS brand_uuid,
			t.name  AS territory_name,
			t.uuid  AS territory_uuid,
			bs.brand_fardes,
			COALESCE(dt.total_fardes, 0)                                           AS total_fardes,
			ROUND((bs.brand_fardes * 100.0 /
			       NULLIF(dt.total_fardes, 0))::numeric, 2)                       AS sos_percent
		FROM brand_shelf bs
		` + d.join + `
		INNER JOIN brands b ON b.uuid = bs.brand_uuid
		LEFT  JOIN dim_total dt ON dt.dim_uuid = bs.dim_uuid
		ORDER BY b.name, t.name
	`

	type RawCell struct {
		BrandName     string  `json:"brand_name"`
		BrandUUID     string  `json:"brand_uuid"`
		TerritoryName string  `json:"territory_name"`
		TerritoryUUID string  `json:"territory_uuid"`
		BrandFardes   float64 `json:"brand_fardes"`
		TotalFardes   float64 `json:"total_fardes"`
		SosPercent    float64 `json:"sos_percent"`
	}

	var raw []RawCell
	if err := db.Raw(sqlQuery, params).Scan(&raw).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS heatmap", "error": err.Error(),
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
		matrix[brandIndex[r.BrandUUID]][terrIndex[r.TerritoryUUID]] = r.SosPercent
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "SOS Heatmap — Brand × Territory",
		"level":   level,
		"data": fiber.Map{
			"brands":      brands,
			"territories": territories,
			"matrix":      matrix,
		},
	})
}

// SOSEvolution — Period-over-Period SOS% comparison per brand.
//
//	Compares current window vs the preceding window of equal length.
//	  current_sos_percent  — SOS% in selected period
//	  previous_sos_percent — SOS% in equal prior period
//	  delta                — current - previous (pp change)
//	  trend                — "gaining" | "losing" | "stable"
func SOSEvolution(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	// Compute previous period in Go to avoid @param::date cast issues with GORM
	const dateFmt = "2006-01-02"
	startDate, err1 := time.Parse(dateFmt, params["start_date"].(string))
	endDate, err2 := time.Parse(dateFmt, params["end_date"].(string))
	if err1 != nil || err2 != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "start_date and end_date must be YYYY-MM-DD",
		})
	}
	window := endDate.Sub(startDate) + 24*time.Hour // inclusive window
	prevEnd := startDate.AddDate(0, 0, -1)
	prevStart := prevEnd.Add(-window + 24*time.Hour)
	params["prev_start_date"] = prevStart.Format(dateFmt)
	params["prev_end_date"] = prevEnd.Format(dateFmt)

	sqlQuery := `
		WITH curr_shelf AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_fardes
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
		prev_shelf AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_fardes
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @prev_start_date AND @prev_end_date
			  AND pf.deleted_at IS NULL AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		),
		curr_total AS (SELECT SUM(brand_fardes) AS total FROM curr_shelf),
		prev_total AS (SELECT SUM(brand_fardes) AS total FROM prev_shelf)
		SELECT
			b.name                                                                  AS brand_name,
			b.uuid                                                                  AS brand_uuid,
			COALESCE(cs.brand_fardes, 0)                                            AS current_fardes,
			COALESCE(ps.brand_fardes, 0)                                            AS previous_fardes,
			(SELECT total FROM curr_total)                                          AS current_total_fardes,
			(SELECT total FROM prev_total)                                          AS previous_total_fardes,
			ROUND((COALESCE(cs.brand_fardes, 0) * 100.0 /
			       NULLIF((SELECT total FROM curr_total), 0))::numeric, 2)         AS current_sos_percent,
			ROUND((COALESCE(ps.brand_fardes, 0) * 100.0 /
			       NULLIF((SELECT total FROM prev_total), 0))::numeric, 2)         AS previous_sos_percent,
			ROUND((COALESCE(cs.brand_fardes, 0) * 100.0 /
			       NULLIF((SELECT total FROM curr_total), 0) -
			       COALESCE(ps.brand_fardes, 0) * 100.0 /
			       NULLIF((SELECT total FROM prev_total), 0))::numeric, 2)         AS delta,
			CASE
				WHEN COALESCE(cs.brand_fardes, 0) * 1.0 / NULLIF((SELECT total FROM curr_total), 0) >
				     COALESCE(ps.brand_fardes, 0) * 1.0 / NULLIF((SELECT total FROM prev_total), 0) THEN 'gaining'
				WHEN COALESCE(cs.brand_fardes, 0) * 1.0 / NULLIF((SELECT total FROM curr_total), 0) <
				     COALESCE(ps.brand_fardes, 0) * 1.0 / NULLIF((SELECT total FROM prev_total), 0) THEN 'losing'
				ELSE 'stable'
			END AS trend
		FROM (
			SELECT brand_uuid FROM curr_shelf
			UNION
			SELECT brand_uuid FROM prev_shelf
		) all_brands
		INNER JOIN brands b ON b.uuid = all_brands.brand_uuid
		LEFT  JOIN curr_shelf cs ON cs.brand_uuid = all_brands.brand_uuid
		LEFT  JOIN prev_shelf ps ON ps.brand_uuid = all_brands.brand_uuid
		ORDER BY current_sos_percent DESC
	`

	type EvoRow struct {
		BrandName           string  `json:"brand_name"`
		BrandUUID           string  `json:"brand_uuid"`
		CurrentFardes       float64 `json:"current_fardes"`
		PreviousFardes      float64 `json:"previous_fardes"`
		CurrentTotalFardes  float64 `json:"current_total_fardes"`
		PreviousTotalFardes float64 `json:"previous_total_fardes"`
		CurrentSosPercent   float64 `json:"current_sos_percent"`
		PreviousSosPercent  float64 `json:"previous_sos_percent"`
		Delta               float64 `json:"delta"`
		Trend               string  `json:"trend"`
	}

	var results []EvoRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS evolution", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS Period-over-Period Evolution", "data": results})
}

// SOSShareGapAnalysis — identifies brands that are below a target SOS threshold.
// ?target=25.0  (default: equal share = 100 / brand_count)
//
//	For each brand:
//	  sos_percent       — actual SOS%
//	  equal_share_target— 100 / n_brands equal-share benchmark
//	  gap               — target - actual (positive = under-performing)
//	  gap_fardes        — extra fardes needed to reach target (estimated)
//	  status            — "above_target" | "below_target"
func SOSShareGapAnalysis(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
	}

	// Resolve target SOS — explicit ?target= or fall back to equal-share
	var targetSos float64
	if t := c.Query("target"); t != "" {
		var parsed float64
		if n, _ := fmt.Sscanf(t, "%f", &parsed); n == 1 {
			targetSos = parsed
		}
	}
	if targetSos == 0 {
		var brandCount int64
		countSQL := `
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
		`
		db.Raw(countSQL, params).Scan(&brandCount)
		if brandCount > 0 {
			targetSos = 100.0 / float64(brandCount)
		} else {
			targetSos = 25.0
		}
	}
	params["target_sos"] = targetSos

	sqlQuery := `
		WITH shelf AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_fardes
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
		grand_total AS (SELECT SUM(brand_fardes) AS total, COUNT(*) AS brand_count FROM shelf),
		brand_sos AS (
			SELECT
				s.brand_uuid,
				b.name                                                               AS brand_name,
				s.brand_fardes,
				(SELECT total FROM grand_total)                                      AS total_fardes,
				(SELECT brand_count FROM grand_total)                                AS brand_count,
				ROUND((s.brand_fardes * 100.0 /
				       NULLIF((SELECT total FROM grand_total), 0))::numeric, 2)     AS sos_percent
			FROM shelf s
			INNER JOIN brands b ON b.uuid = s.brand_uuid
		)
		SELECT
			brand_name,
			brand_uuid,
			brand_fardes,
			total_fardes,
			sos_percent,
			ROUND((100.0 / NULLIF(brand_count, 0))::numeric, 2)                    AS equal_share_target,
			ROUND((@target_sos - sos_percent)::numeric, 2)                         AS gap,
			ROUND(GREATEST(0,
				((@target_sos / 100.0) * total_fardes - brand_fardes)
			)::numeric, 0)                                                          AS gap_fardes,
			CASE WHEN sos_percent >= @target_sos THEN 'above_target' ELSE 'below_target' END AS status
		FROM brand_sos
		ORDER BY gap DESC
	`

	type GapRow struct {
		BrandName        string  `json:"brand_name"`
		BrandUUID        string  `json:"brand_uuid"`
		BrandFardes      float64 `json:"brand_fardes"`
		TotalFardes      float64 `json:"total_fardes"`
		SosPercent       float64 `json:"sos_percent"`
		EqualShareTarget float64 `json:"equal_share_target"`
		Gap              float64 `json:"gap"`
		GapFardes        float64 `json:"gap_fardes"`
		Status           string  `json:"status"`
	}

	var results []GapRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS gap analysis", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "SOS Share Gap Analysis",
		"target_sos": targetSos,
		"data":       results,
	})
}

// SOSPosDrillDown — deep-dive on a single brand's SOS across individual POS.
// Required: ?brand_uuid=...
// Optional: standard territory + date filters
//
//	Per POS:
//	  pos_name, pos_shop, pos_type
//	  visit_count, last_visit
//	  min_sos / max_sos / avg_sos — volatility across visits
//	  sos_percent — aggregate SOS over the full period
func SOSPosDrillDown(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
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
		WITH pos_visit_fardes AS (
			SELECT
				pf.pos_uuid,
				pf.uuid                                    AS pos_form_uuid,
				pf.created_at,
				COALESCE(brand_item.brand_fardes, 0)       AS brand_fardes,
				pos_total.pos_fardes
			FROM pos_forms pf
			INNER JOIN (
				SELECT pos_form_uuid, SUM(number_farde) AS pos_fardes
				FROM pos_form_items WHERE deleted_at IS NULL
				GROUP BY pos_form_uuid
			) pos_total ON pos_total.pos_form_uuid = pf.uuid
			LEFT JOIN (
				SELECT pos_form_uuid, SUM(number_farde) AS brand_fardes
				FROM pos_form_items
				WHERE brand_uuid = @brand_uuid AND deleted_at IS NULL
				GROUP BY pos_form_uuid
			) brand_item ON brand_item.pos_form_uuid = pf.uuid
			WHERE pf.country_uuid = @country_uuid
			  AND (@province_uuid = '' OR pf.province_uuid = @province_uuid)
			  AND (@area_uuid     = '' OR pf.area_uuid     = @area_uuid)
			  AND (@sub_area_uuid = '' OR pf.sub_area_uuid = @sub_area_uuid)
			  AND (@commune_uuid  = '' OR pf.commune_uuid  = @commune_uuid)
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		)
		SELECT
			p.uuid                                                             AS pos_uuid,
			p.name                                                             AS pos_name,
			p.shop                                                             AS pos_shop,
			p.postype                                                          AS pos_type,
			SUM(pvf.brand_fardes)                                              AS brand_fardes,
			SUM(pvf.pos_fardes)                                                AS total_fardes,
			COUNT(pvf.pos_form_uuid)                                           AS visit_count,
			MAX(pvf.created_at)                                                AS last_visit,
			ROUND(MIN(
				CASE WHEN pvf.pos_fardes > 0
				THEN pvf.brand_fardes * 100.0 / pvf.pos_fardes ELSE 0 END
			)::numeric, 2)                                                     AS min_sos,
			ROUND(MAX(
				CASE WHEN pvf.pos_fardes > 0
				THEN pvf.brand_fardes * 100.0 / pvf.pos_fardes ELSE 0 END
			)::numeric, 2)                                                     AS max_sos,
			ROUND(AVG(
				CASE WHEN pvf.pos_fardes > 0
				THEN pvf.brand_fardes * 100.0 / pvf.pos_fardes ELSE 0 END
			)::numeric, 2)                                                     AS avg_sos,
			ROUND((SUM(pvf.brand_fardes) * 100.0 /
			       NULLIF(SUM(pvf.pos_fardes), 0))::numeric, 2)               AS sos_percent
		FROM pos_visit_fardes pvf
		INNER JOIN pos p ON p.uuid = pvf.pos_uuid
		GROUP BY p.uuid, p.name, p.shop, p.postype
		ORDER BY avg_sos DESC
		LIMIT 100
	`

	type DrillRow struct {
		PosUUID     string  `json:"pos_uuid"`
		PosName     string  `json:"pos_name"`
		PosShop     string  `json:"pos_shop"`
		PosType     string  `json:"pos_type"`
		BrandFardes float64 `json:"brand_fardes"`
		TotalFardes float64 `json:"total_fardes"`
		VisitCount  int64   `json:"visit_count"`
		LastVisit   string  `json:"last_visit"`
		MinSos      float64 `json:"min_sos"`
		MaxSos      float64 `json:"max_sos"`
		AvgSos      float64 `json:"avg_sos"`
		SosPercent  float64 `json:"sos_percent"`
	}

	var results []DrillRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS POS drill-down", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "SOS POS-Level Drill-Down",
		"brand_uuid": brandUUID,
		"data":       results,
	})
}

// SOSVsNDCorrelation — cross-metric analysis: SOS% vs ND% per brand.
//
//	Uncovers the 4 commercial positions:
//	  "leader"                 — high ND (≥50%) AND high SOS (≥33%)
//	  "present_not_dominant"   — high ND but low SOS  (wide distribution, thin shelf)
//	  "stocked_not_distributed"— low ND but high SOS  (focused strongholds)
//	  "niche"                  — low ND and low SOS
func SOSVsNDCorrelation(c *fiber.Ctx) error {
	db := database.DB
	params := extractSOSParams(c)
	if params == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status": "error", "message": "country_uuid, start_date and end_date are required",
		})
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
		brand_nd AS (
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
		brand_shelf AS (
			SELECT pfi.brand_uuid, SUM(pfi.number_farde) AS brand_fardes
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
		grand_total AS (SELECT SUM(brand_fardes) AS total FROM brand_shelf)
		SELECT
			b.name                                                                     AS brand_name,
			b.uuid                                                                     AS brand_uuid,
			COALESCE(nd.nd_pos, 0)                                                     AS nd_pos,
			(SELECT total_pos FROM visited)                                            AS total_pos,
			ROUND((COALESCE(nd.nd_pos,0)*100.0/NULLIF((SELECT total_pos FROM visited),0))::numeric,2) AS nd_percent,
			COALESCE(bs.brand_fardes, 0)                                               AS brand_fardes,
			(SELECT total FROM grand_total)                                            AS total_fardes,
			ROUND((COALESCE(bs.brand_fardes,0)*100.0/NULLIF((SELECT total FROM grand_total),0))::numeric,2) AS sos_percent,
			ROUND((COALESCE(nd.nd_pos,0)*100.0/NULLIF((SELECT total_pos FROM visited),0) -
			       COALESCE(bs.brand_fardes,0)*100.0/NULLIF((SELECT total FROM grand_total),0))::numeric,2) AS delta_nd_sos,
			CASE
				WHEN (COALESCE(nd.nd_pos,0)*100.0/NULLIF((SELECT total_pos FROM visited),0)) >= 50
				 AND (COALESCE(bs.brand_fardes,0)*100.0/NULLIF((SELECT total FROM grand_total),0)) >= 33
				THEN 'leader'
				WHEN (COALESCE(nd.nd_pos,0)*100.0/NULLIF((SELECT total_pos FROM visited),0)) >= 50
				 AND (COALESCE(bs.brand_fardes,0)*100.0/NULLIF((SELECT total FROM grand_total),0)) < 33
				THEN 'present_not_dominant'
				WHEN (COALESCE(nd.nd_pos,0)*100.0/NULLIF((SELECT total_pos FROM visited),0)) < 50
				 AND (COALESCE(bs.brand_fardes,0)*100.0/NULLIF((SELECT total FROM grand_total),0)) >= 33
				THEN 'stocked_not_distributed'
				ELSE 'niche'
			END AS position
		FROM (SELECT brand_uuid FROM brand_nd UNION SELECT brand_uuid FROM brand_shelf) all_brands
		INNER JOIN brands b    ON b.uuid  = all_brands.brand_uuid
		LEFT  JOIN brand_nd nd ON nd.brand_uuid = all_brands.brand_uuid
		LEFT  JOIN brand_shelf bs ON bs.brand_uuid = all_brands.brand_uuid
		ORDER BY nd_percent DESC, sos_percent DESC
	`

	type CorrelRow struct {
		BrandName   string  `json:"brand_name"`
		BrandUUID   string  `json:"brand_uuid"`
		NdPos       int64   `json:"nd_pos"`
		TotalPos    int64   `json:"total_pos"`
		NdPercent   float64 `json:"nd_percent"`
		BrandFardes float64 `json:"brand_fardes"`
		TotalFardes float64 `json:"total_fardes"`
		SosPercent  float64 `json:"sos_percent"`
		DeltaNdSos  float64 `json:"delta_nd_sos"`
		Position    string  `json:"position"`
	}

	var results []CorrelRow
	if err := db.Raw(sqlQuery, params).Scan(&results).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": "Failed to fetch SOS vs ND correlation", "error": err.Error(),
		})
	}
	return c.JSON(fiber.Map{"status": "success", "message": "SOS vs ND Correlation Matrix", "data": results})
}

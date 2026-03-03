package ndindividual

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║              SOS INDIVIDUEL — PAR AGENT                                 ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║  Chaque agent peut consulter son propre SOS pour chaque marque afin      ║
// ║  de comprendre son positionnement des marques en rayon.                 ║
// ║                                                                          ║
// ║  SOS% = SUM(brand fardes) / SUM(ALL brands fardes) × 100                ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║  GET /sos-individual/summary/:user_uuid    — KPI global de l'agent      ║
// ║  GET /sos-individual/by-brand/:user_uuid   — SOS par marque             ║
// ║  GET /sos-individual/pos-list/:user_uuid   — Liste des POS visités      ║
// ║                                              avec fardes par marque       ║
// ╚══════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// 1. SUMMARY KPI — chiffres globaux de l'agent (SOS)
// ─────────────────────────────────────────────────────────────────────────────

// GetSOSSummary retourne un résumé KPI global de l'agent :
//   - total_pos_visited   : total de POS distincts visités
//   - total_fardes_pos    : total de fardes enregistrées par l'agent
//   - brand_count         : nombre de marques distinctes
//   - universe_pos        : total de POS enregistrés dans la commune
//   - reach_rate          : total_pos_visited / universe_pos × 100
//   - dominant_brand      : marque avec le plus grand nombre de fardes
//   - dominant_brand_sos  : SOS% de la marque dominante
//
// Params : user_uuid (path), start_date, end_date (query)
func GetSOSSummary(c *fiber.Ctx) error {
	db := database.DB

	userUUID := c.Params("user_uuid")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if userUUID == "" || startDate == "" || endDate == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status":  "error",
			"message": "user_uuid, start_date et end_date sont obligatoires",
		})
	}

	type SummaryRow struct {
		UserUUID         string  `json:"user_uuid"`
		Fullname         string  `json:"fullname"`
		TotalPosVisited  int64   `json:"total_pos_visited"`
		TotalFardesPos   float64 `json:"total_fardes_pos"`
		BrandCount       int64   `json:"brand_count"`
		UniversePos      int64   `json:"universe_pos"`
		ReachRate        float64 `json:"reach_rate"`
		DominantBrand    string  `json:"dominant_brand"`
		DominantBrandSos float64 `json:"dominant_brand_sos"`
	}

	sqlQuery := `
		WITH agent AS (
			SELECT uuid, fullname, commune_uuid
			FROM users
			WHERE uuid = @user_uuid AND deleted_at IS NULL
		),
		visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.user_uuid = @user_uuid
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		),
		fardes_summary AS (
			SELECT 
				SUM(pfi.number_farde)::float8 AS total_fardes,
				COUNT(DISTINCT pfi.brand_uuid) AS brand_count,
				pfi.brand_uuid
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.user_uuid = @user_uuid
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			  AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		),
		total_fardes AS (
			SELECT SUM(pfi.number_farde)::float8 AS total
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.user_uuid = @user_uuid
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			  AND pfi.deleted_at IS NULL
		),
		universe AS (
			SELECT COUNT(p.uuid) AS universe_pos
			FROM pos p
			INNER JOIN agent a ON p.commune_uuid = a.commune_uuid
			WHERE p.deleted_at IS NULL
		),
		dominant AS (
			SELECT 
				b.name AS brand_name,
				ROUND((fs.total_fardes * 100.0 / NULLIF((SELECT total FROM total_fardes), 0))::numeric, 2) AS sos_percent
			FROM fardes_summary fs
			INNER JOIN brands b ON b.uuid = fs.brand_uuid
			ORDER BY fs.total_fardes DESC
			LIMIT 1
		)
		SELECT
			a.uuid                                                            AS user_uuid,
			a.fullname                                                        AS fullname,
			COALESCE(v.total_pos, 0)                                          AS total_pos_visited,
			COALESCE((SELECT total FROM total_fardes), 0)                     AS total_fardes_pos,
			COALESCE((SELECT COUNT(*) FROM fardes_summary), 0)                AS brand_count,
			COALESCE(u.universe_pos, 0)                                       AS universe_pos,
			ROUND((COALESCE(v.total_pos, 0) * 100.0 /
			       NULLIF(COALESCE(u.universe_pos, 0), 0))::numeric, 2)      AS reach_rate,
			COALESCE((SELECT brand_name FROM dominant), 'N/A')                AS dominant_brand,
			COALESCE((SELECT sos_percent FROM dominant), 0)                   AS dominant_brand_sos
		FROM agent a
		CROSS JOIN visited v
		CROSS JOIN universe u
	`

	var result SummaryRow
	if err := db.Raw(sqlQuery,
		map[string]interface{}{
			"user_uuid":  userUUID,
			"start_date": startDate,
			"end_date":   endDate,
		},
	).Scan(&result).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status": "success",
		"data":   result,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. SOS PAR MARQUE — détail marque par marque pour l'agent
// ─────────────────────────────────────────────────────────────────────────────

// GetSOSByBrand retourne le SOS par marque pour l'agent :
//   - brand_name       : nom de la marque
//   - brand_fardes     : total de fardes enregistrées pour cette marque
//   - total_fardes     : total de fardes pour TOUTES les marques
//   - sos_percent      : brand_fardes / total_fardes × 100
//   - pos_count        : nombre de POS où cette marque a des fardes
//   - avg_fardes_per_pos: moyenne de fardes par POS
//
// Params : user_uuid (path), start_date, end_date (query)
func GetSOSByBrand(c *fiber.Ctx) error {
	db := database.DB

	userUUID := c.Params("user_uuid")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if userUUID == "" || startDate == "" || endDate == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status":  "error",
			"message": "user_uuid, start_date et end_date sont obligatoires",
		})
	}

	type BrandRow struct {
		BrandUUID       string  `json:"brand_uuid"`
		BrandName       string  `json:"brand_name"`
		BrandFardes     float64 `json:"brand_fardes"`
		TotalFardes     float64 `json:"total_fardes"`
		SosPercent      float64 `json:"sos_percent"`
		PosCount        int64   `json:"pos_count"`
		AvgFardesPerPos float64 `json:"avg_fardes_per_pos"`
	}

	sqlQuery := `
		WITH brand_fardes AS (
			SELECT
				pfi.brand_uuid,
				SUM(pfi.number_farde)::float8        AS brand_fardes,
				COUNT(DISTINCT pf.pos_uuid)          AS pos_count,
				AVG(pfi.number_farde)::float8        AS avg_fardes_per_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.user_uuid = @user_uuid
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			  AND pfi.deleted_at IS NULL
			GROUP BY pfi.brand_uuid
		),
		total_fardes AS (
			SELECT SUM(pfi.number_farde)::float8 AS total
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.user_uuid = @user_uuid
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			  AND pfi.deleted_at IS NULL
		)
		SELECT
			b.uuid                                                           AS brand_uuid,
			b.name                                                           AS brand_name,
			COALESCE(bf.brand_fardes, 0)                                     AS brand_fardes,
			COALESCE((SELECT total FROM total_fardes), 0)                    AS total_fardes,
			ROUND((COALESCE(bf.brand_fardes, 0) * 100.0 /
			       NULLIF((SELECT total FROM total_fardes), 0))::numeric, 2)AS sos_percent,
			COALESCE(bf.pos_count, 0)                                        AS pos_count,
			COALESCE(bf.avg_fardes_per_pos, 0)                              AS avg_fardes_per_pos
		FROM brands b
		LEFT JOIN brand_fardes bf ON b.uuid = bf.brand_uuid
		WHERE b.deleted_at IS NULL
		ORDER BY brand_fardes DESC
	`

	var rows []BrandRow
	if err := db.Raw(sqlQuery,
		map[string]interface{}{
			"user_uuid":  userUUID,
			"start_date": startDate,
			"end_date":   endDate,
		},
	).Scan(&rows).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status": "success",
		"data":   rows,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. LISTE DES POS VISITÉS — avec détail des fardes par marque
// ─────────────────────────────────────────────────────────────────────────────

// GetSOSPosList retourne la liste des POS visités par l'agent avec,
// pour chaque POS, le détail par marque :
//   - pos_name       : nom du POS
//   - shop           : nom du shop
//   - commune        : commune du POS
//   - brand_name     : nom de la marque
//   - number_farde   : nombre de fardes enregistrées
//   - pos_total_fardes: total de fardes pour ce POS
//   - sos_per_pos    : brand_fardes / pos_total_fardes × 100
//   - visit_date     : date de la visite
//
// Params : user_uuid (path), start_date, end_date (query)
func GetSOSPosList(c *fiber.Ctx) error {
	db := database.DB

	userUUID := c.Params("user_uuid")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if userUUID == "" || startDate == "" || endDate == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status":  "error",
			"message": "user_uuid, start_date et end_date sont obligatoires",
		})
	}

	type PosRow struct {
		PosUUID        string  `json:"pos_uuid"`
		PosName        string  `json:"pos_name"`
		Shop           string  `json:"shop"`
		Commune        string  `json:"commune"`
		BrandUUID      string  `json:"brand_uuid"`
		BrandName      string  `json:"brand_name"`
		NumberFarde    float64 `json:"number_farde"`
		PosTotalFardes float64 `json:"pos_total_fardes"`
		SosPerPos      float64 `json:"sos_per_pos"`
		VisitDate      string  `json:"visit_date"`
	}

	sqlQuery := `
		WITH pos_totals AS (
			SELECT 
				pf.uuid AS pos_form_uuid,
				SUM(pfi.number_farde)::float8 AS pos_total_farde
			FROM pos_form_items pfi
			WHERE pfi.deleted_at IS NULL
			GROUP BY pf.uuid
		)
		SELECT
			pf.pos_uuid                                      AS pos_uuid,
			p.name                                           AS pos_name,
			p.shop                                           AS pos_shop,
			c.name                                           AS commune,
			b.uuid                                           AS brand_uuid,
			b.name                                           AS brand_name,
			pfi.number_farde                                 AS number_farde,
			COALESCE(pt.pos_total_farde, 0)::float8          AS pos_total_fardes,
			ROUND((pfi.number_farde * 100.0 / 
				   NULLIF(COALESCE(pt.pos_total_farde, 0), 0))::numeric, 2)
			                                                 AS sos_per_pos,
			TO_CHAR(pf.created_at, 'YYYY-MM-DD')             AS visit_date
		FROM pos_forms pf
		INNER JOIN pos_form_items pfi ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN pos p                ON p.uuid = pf.pos_uuid
		INNER JOIN brands b             ON b.uuid = pfi.brand_uuid
		LEFT  JOIN communes c           ON c.uuid = pf.commune_uuid
		LEFT  JOIN pos_totals pt        ON pt.pos_form_uuid = pf.uuid
		WHERE pf.user_uuid = @user_uuid
		  AND pf.created_at BETWEEN @start_date AND @end_date
		  AND pf.deleted_at IS NULL
		  AND pfi.deleted_at IS NULL
		ORDER BY pf.created_at DESC, p.name, b.name
	`

	var rows []PosRow
	if err := db.Raw(sqlQuery,
		map[string]interface{}{
			"user_uuid":  userUUID,
			"start_date": startDate,
			"end_date":   endDate,
		},
	).Scan(&rows).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status": "error", "message": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status": "success",
		"data":   rows,
	})
}

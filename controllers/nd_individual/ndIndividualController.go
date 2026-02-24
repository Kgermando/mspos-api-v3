package ndindividual

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║              ND INDIVIDUEL — PAR AGENT                                  ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║  Chaque agent peut consulter son propre ND pour chaque marque afin      ║
// ║  de le défendre lors des revues.                                        ║
// ║                                                                          ║
// ║  ND% = POS où counter > 0 / Total POS visités × 100                    ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║  GET /nd-individual/summary/:user_uuid    — KPI global de l'agent       ║
// ║  GET /nd-individual/by-brand/:user_uuid   — ND par marque               ║
// ║  GET /nd-individual/pos-list/:user_uuid   — Liste des POS visités + ND  ║
// ╚══════════════════════════════════════════════════════════════════════════╝

// ─────────────────────────────────────────────────────────────────────────────
// 1. SUMMARY KPI — chiffres globaux de l'agent
// ─────────────────────────────────────────────────────────────────────────────

// GetNDSummary retourne un résumé KPI global de l'agent :
//   - total_pos_visited : total de POS distincts visités
//   - nd_pos            : POS où au moins une marque a un counter > 0
//   - nd_percent        : nd_pos / total_pos_visited × 100
//   - universe_pos      : total de POS enregistrés dans le territoire de l'agent
//   - reach_rate        : total_pos_visited / universe_pos × 100
//
// Params : user_uuid (path), start_date, end_date (query)
func GetNDSummary(c *fiber.Ctx) error {
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
		UserUUID        string  `json:"user_uuid"`
		Fullname        string  `json:"fullname"`
		TotalPosVisited int64   `json:"total_pos_visited"`
		NdPos           int64   `json:"nd_pos"`
		NdPercent       float64 `json:"nd_percent"`
		UniversePos     int64   `json:"universe_pos"`
		ReachRate       float64 `json:"reach_rate"`
	}

	sqlQuery := `
		WITH agent AS (
			SELECT uuid, fullname, commune_uuid, sub_area_uuid, area_uuid, province_uuid, country_uuid
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
		nd_visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_forms pf
			INNER JOIN pos_form_items pfi ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.user_uuid = @user_uuid
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			  AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
		),
		universe AS (
			SELECT COUNT(p.uuid) AS universe_pos
			FROM pos p
			INNER JOIN agent a ON p.commune_uuid = a.commune_uuid
			WHERE p.deleted_at IS NULL
		)
		SELECT
			a.uuid                                                            AS user_uuid,
			a.fullname                                                        AS fullname,
			COALESCE(v.total_pos, 0)                                          AS total_pos_visited,
			COALESCE(nd.nd_pos, 0)                                            AS nd_pos,
			ROUND((COALESCE(nd.nd_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)         AS nd_percent,
			COALESCE(u.universe_pos, 0)                                       AS universe_pos,
			ROUND((COALESCE(v.total_pos, 0) * 100.0 /
			       NULLIF(COALESCE(u.universe_pos, 0), 0))::numeric, 2)      AS reach_rate
		FROM agent a
		CROSS JOIN visited v
		CROSS JOIN nd_visited nd
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
// 2. ND PAR MARQUE — détail marque par marque pour l'agent
// ─────────────────────────────────────────────────────────────────────────────

// GetNDByBrand retourne le ND par marque pour l'agent :
//   - brand_name  : nom de la marque
//   - nd_pos      : POS où counter > 0 pour cette marque
//   - total_pos   : total POS visités par l'agent
//   - nd_percent  : nd_pos / total_pos × 100
//
// Params : user_uuid (path), start_date, end_date (query)
func GetNDByBrand(c *fiber.Ctx) error {
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
		BrandUUID string  `json:"brand_uuid"`
		BrandName string  `json:"brand_name"`
		NdPos     int64   `json:"nd_pos"`
		TotalPos  int64   `json:"total_pos"`
		NdPercent float64 `json:"nd_percent"`
	}

	sqlQuery := `
		WITH visited AS (
			SELECT COUNT(DISTINCT pf.pos_uuid) AS total_pos
			FROM pos_forms pf
			WHERE pf.user_uuid = @user_uuid
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
		),
		nd_counts AS (
			SELECT
				pfi.brand_uuid,
				COUNT(DISTINCT pf.pos_uuid) AS nd_pos
			FROM pos_form_items pfi
			INNER JOIN pos_forms pf ON pfi.pos_form_uuid = pf.uuid
			WHERE pf.user_uuid = @user_uuid
			  AND pf.created_at BETWEEN @start_date AND @end_date
			  AND pf.deleted_at IS NULL
			  AND pfi.deleted_at IS NULL
			  AND pfi.counter > 0
			GROUP BY pfi.brand_uuid
		)
		SELECT
			b.uuid                                                           AS brand_uuid,
			b.name                                                           AS brand_name,
			COALESCE(nd.nd_pos, 0)                                           AS nd_pos,
			COALESCE(v.total_pos, 0)                                         AS total_pos,
			ROUND((COALESCE(nd.nd_pos, 0) * 100.0 /
			       NULLIF(COALESCE(v.total_pos, 0), 0))::numeric, 2)        AS nd_percent
		FROM nd_counts nd
		INNER JOIN brands b ON b.uuid = nd.brand_uuid
		CROSS JOIN visited v
		ORDER BY nd_percent DESC
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
// 3. LISTE DES POS VISITÉS — avec statut ND par marque (pour la défense)
// ─────────────────────────────────────────────────────────────────────────────

// GetNDPosList retourne la liste des POS visités par l'agent avec,
// pour chaque POS, le détail par marque :
//   - pos_name    : nom du POS
//   - shop        : nom du shop
//   - commune     : commune du POS
//   - brand_name  : nom de la marque
//   - counter     : valeur enregistrée (> 0 = ND actif)
//   - nd_active   : true si counter > 0
//   - visit_date  : date de la visite
//
// Params : user_uuid (path), start_date, end_date (query)
func GetNDPosList(c *fiber.Ctx) error {
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
		PosUUID   string `json:"pos_uuid"`
		PosName   string `json:"pos_name"`
		Shop      string `json:"shop"`
		Commune   string `json:"commune"`
		BrandUUID string `json:"brand_uuid"`
		BrandName string `json:"brand_name"`
		Counter   int    `json:"counter"`
		NdActive  bool   `json:"nd_active"`
		VisitDate string `json:"visit_date"`
	}

	sqlQuery := `
		SELECT
			pf.pos_uuid                                      AS pos_uuid,
			p.name                                           AS pos_name,
			p.shop                                           AS shop,
			c.name                                           AS commune,
			b.uuid                                           AS brand_uuid,
			b.name                                           AS brand_name,
			pfi.counter                                      AS counter,
			(pfi.counter > 0)                                AS nd_active,
			TO_CHAR(pf.created_at, 'YYYY-MM-DD')             AS visit_date
		FROM pos_forms pf
		INNER JOIN pos_form_items pfi ON pfi.pos_form_uuid = pf.uuid
		INNER JOIN pos p              ON p.uuid = pf.pos_uuid
		INNER JOIN brands b           ON b.uuid = pfi.brand_uuid
		LEFT  JOIN communes c         ON c.uuid = pf.commune_uuid
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

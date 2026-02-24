package observation

import (
	"strconv"
	"strings"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/danny19977/mspos-api-v3/utils"
	"github.com/gofiber/fiber/v2"
)

// —————————————————————————————————————————————————————————————
// HELPER: shared paginated query builder
// —————————————————————————————————————————————————————————————

func paginatedObservations(c *fiber.Ctx, extraWhere ...interface{}) error {
	db := database.DB

	startDate := c.Query("start_date", "1970-01-01T00:00:00Z")
	endDate := c.Query("end_date", "2100-01-01T00:00:00Z")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries  ON pos_forms.country_uuid  = countries.uuid").
		Joins("LEFT JOIN provinces  ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas      ON pos_forms.area_uuid     = areas.uuid").
		Joins("LEFT JOIN sub_areas  ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes   ON pos_forms.commune_uuid  = communes.uuid").
		Joins("LEFT JOIN pos        ON pos_forms.pos_uuid      = pos.uuid").
		Joins("LEFT JOIN users      ON pos_forms.user_uuid     = users.uuid").
		// Only records with a non-empty comment
		Where("pos_forms.comment IS NOT NULL AND pos_forms.comment != ''").
		Where("pos_forms.created_at BETWEEN ? AND ?", startDate, endDate)

	// Apply caller-supplied predicates (role-based territory filter)
	for i := 0; i+1 < len(extraWhere); i += 2 {
		query = query.Where(extraWhere[i], extraWhere[i+1])
	}
	if len(extraWhere) == 1 { // raw string condition without args
		if cond, ok := extraWhere[0].(string); ok {
			query = query.Where(cond)
		}
	}

	// Common search / geographic / agent filters
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

	query.Count(&totalRecords)

	err = query.
		Select("pos_forms.*").
		Offset(offset).
		Limit(limit).
		Order("pos_forms.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("Pos").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Erreur lors de la récupération des observations",
			"error":   err.Error(),
		})
	}

	// Map to ObservationResponse DTO
	results := make([]models.ObservationResponse, 0, len(dataList))
	for _, pf := range dataList {
		results = append(results, models.ObservationResponse{
			UUID:         pf.UUID,
			CreatedAt:    pf.CreatedAt,
			UpdatedAt:    pf.UpdatedAt,
			Comment:      pf.Comment,
			PosUUID:      pf.PosUUID,
			PosName:      pf.Pos.Name,
			CountryUUID:  pf.CountryUUID,
			CountryName:  pf.Country.Name,
			ProvinceUUID: pf.ProvinceUUID,
			ProvinceName: pf.Province.Name,
			AreaUUID:     pf.AreaUUID,
			AreaName:     pf.Area.Name,
			SubAreaUUID:  pf.SubAreaUUID,
			SubAreaName:  pf.SubArea.Name,
			CommuneUUID:  pf.CommuneUUID,
			CommuneName:  pf.Commune.Name,
			AsmUUID:      pf.AsmUUID,
			Asm:          pf.Asm,
			SupUUID:      pf.SupUUID,
			Sup:          pf.Sup,
			DrUUID:       pf.DrUUID,
			Dr:           pf.Dr,
			CycloUUID:    pf.CycloUUID,
			Cyclo:        pf.Cyclo,
			UserUUID:     pf.UserUUID,
			UserName:     pf.User.Fullname,
			UserRole:     pf.User.Role,
		})
	}

	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Observations récupérées avec succès",
		"data":    results,
		"pagination": fiber.Map{
			"total_records": totalRecords,
			"total_pages":   totalPages,
			"current_page":  page,
			"page_size":     limit,
		},
	})
}

// —————————————————————————————————————————————————————————————
// 1. ROLE-BASED endpoint  –  auto-filters according to current user's role
//    GET /api/observations/all/paginate?token=<jwt>
// —————————————————————————————————————————————————————————————

// GetObservationsByRole retrieves paginated observations filtered by the role
// of the authenticated user:
//   - Support / Manager  → no territory filter (see everything)
//   - ASM                → filtered to their province
//   - Supervisor (Sup)   → filtered to their area
//   - DR                 → filtered to their sub-area
//   - Cyclo              → filtered to their commune
func GetObservationsByRole(c *fiber.Ctx) error {
	// Resolve JWT from query param or cookie (mirrors AuthUser pattern)
	token := c.Query("token")
	if token == "" {
		token = c.Cookies("token")
	}

	userUUID, err := utils.VerifyJwt(token)
	if err != nil || userUUID == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status":  "error",
			"message": "Non autorisé — token invalide ou manquant",
		})
	}

	// Load the current user
	var currentUser models.User
	if err := database.DB.Where("uuid = ?", userUUID).First(&currentUser).Error; err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"status":  "error",
			"message": "Utilisateur introuvable",
		})
	}

	role := strings.ToLower(strings.TrimSpace(currentUser.Role))

	switch role {
	case "support", "manager":
		// No territory restriction — see everything
		return paginatedObservations(c)

	case "asm":
		return paginatedObservations(c,
			"pos_forms.province_uuid = ?", currentUser.ProvinceUUID)

	case "supervisor", "sup":
		return paginatedObservations(c,
			"pos_forms.area_uuid = ?", currentUser.AreaUUID)

	case "dr":
		return paginatedObservations(c,
			"pos_forms.sub_area_uuid = ?", currentUser.SubAreaUUID)

	case "cyclo":
		return paginatedObservations(c,
			"pos_forms.commune_uuid = ?", currentUser.CommuneUUID)

	default:
		// Unknown role — restricted to nothing
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status":  "error",
			"message": "Rôle non reconnu — accès refusé",
		})
	}
}

// —————————————————————————————————————————————————————————————
// 2. TERRITORY-scoped endpoints  (explicit UUID in path)
// —————————————————————————————————————————————————————————————

// GetAllObservations returns all observations without territory restriction.
// Intended for Support / Manager.
// GET /api/observations/all/paginate
func GetAllObservations(c *fiber.Ctx) error {
	return paginatedObservations(c)
}

// GetObservationsByCountry filters by country UUID.
// GET /api/observations/all/paginate/country/:country_uuid
func GetObservationsByCountry(c *fiber.Ctx) error {
	countryUUID := c.Params("country_uuid")
	return paginatedObservations(c, "pos_forms.country_uuid = ?", countryUUID)
}

// GetObservationsByProvince filters by province UUID.  Used by ASM.
// GET /api/observations/all/paginate/province/:province_uuid
func GetObservationsByProvince(c *fiber.Ctx) error {
	provinceUUID := c.Params("province_uuid")
	return paginatedObservations(c, "pos_forms.province_uuid = ?", provinceUUID)
}

// GetObservationsByArea filters by area UUID.  Used by Supervisor.
// GET /api/observations/all/paginate/area/:area_uuid
func GetObservationsByArea(c *fiber.Ctx) error {
	areaUUID := c.Params("area_uuid")
	return paginatedObservations(c, "pos_forms.area_uuid = ?", areaUUID)
}

// GetObservationsBySubArea filters by sub-area UUID.  Used by DR.
// GET /api/observations/all/paginate/subarea/:sub_area_uuid
func GetObservationsBySubArea(c *fiber.Ctx) error {
	subAreaUUID := c.Params("sub_area_uuid")
	return paginatedObservations(c, "pos_forms.sub_area_uuid = ?", subAreaUUID)
}

// GetObservationsByCommune filters by commune UUID.  Used by Cyclo.
// GET /api/observations/all/paginate/commune/:commune_uuid
func GetObservationsByCommune(c *fiber.Ctx) error {
	communeUUID := c.Params("commune_uuid")
	return paginatedObservations(c, "pos_forms.commune_uuid = ?", communeUUID)
}

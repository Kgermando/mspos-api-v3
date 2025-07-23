package pos

import (
	"fmt"
	"strconv"
	"time"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/danny19977/mspos-api-v3/utils"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Paginate
func GetPaginatedPos(c *fiber.Ctx) error {
	db := database.DB

	// Parse query parameters for pagination
	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.Pos
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.Pos{}).
		Joins("LEFT JOIN countries ON pos.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos.commune_uuid = communes.uuid").
		Joins("LEFT JOIN users ON pos.user_uuid = users.uuid")

	// Apply advanced filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch paginated data
	err = query.
		Select("pos.*").
		Offset(offset).
		Limit(limit).
		Order("pos.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosForms").
		Preload("PosEquipments").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch POS",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "POS retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by Country uuid
func GetPaginatedPosByCountryUUID(c *fiber.Ctx) error {
	db := database.DB

	CountryUUID := c.Params("country_uuid")

	// Parse query parameters for pagination
	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.Pos
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.Pos{}).
		Joins("LEFT JOIN countries ON pos.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos.commune_uuid = communes.uuid").
		Joins("LEFT JOIN users ON pos.user_uuid = users.uuid").
		Where("pos.country_uuid = ?", CountryUUID)

	// Apply advanced filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch paginated data
	err = query.
		Select("pos.*").
		Offset(offset).
		Limit(limit).
		Order("pos.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosForms").
		Preload("PosEquipments").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch POS by province",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "POS retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by province id
func GetPaginatedPosByProvinceUUID(c *fiber.Ctx) error {
	db := database.DB

	ProvinceUUID := c.Params("province_uuid")

	// Parse query parameters for pagination
	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.Pos
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.Pos{}).
		Joins("LEFT JOIN countries ON pos.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos.commune_uuid = communes.uuid").
		Joins("LEFT JOIN users ON pos.user_uuid = users.uuid").
		Where("pos.province_uuid = ?", ProvinceUUID)

	// Apply advanced filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch paginated data
	err = query.
		Select("pos.*").
		Offset(offset).
		Limit(limit).
		Order("pos.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosForms").
		Preload("PosEquipments").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch POS by province",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "POS retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by area id
func GetPaginatedPosByAreaUUID(c *fiber.Ctx) error {
	db := database.DB

	AreaUUID := c.Params("area_uuid")

	// Parse query parameters for pagination
	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.Pos
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.Pos{}).
		Joins("LEFT JOIN countries ON pos.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos.commune_uuid = communes.uuid").
		Joins("LEFT JOIN users ON pos.user_uuid = users.uuid").
		Where("pos.area_uuid = ?", AreaUUID)

	// Apply advanced filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch paginated data
	err = query.
		Select("pos.*").
		Offset(offset).
		Limit(limit).
		Order("pos.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosEquipments").
		Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch POS by area",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "POS retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by SubArea id
func GetPaginatedPosBySubAreaUUID(c *fiber.Ctx) error {
	db := database.DB

	SubAreaUUID := c.Params("sub_area_uuid")

	// Parse query parameters for pagination
	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.Pos
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.Pos{}).
		Joins("LEFT JOIN countries ON pos.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos.commune_uuid = communes.uuid").
		Joins("LEFT JOIN users ON pos.user_uuid = users.uuid").
		Where("pos.sub_area_uuid = ?", SubAreaUUID)

	// Apply advanced filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch paginated data
	err = query.
		Select("pos.*").
		Offset(offset).
		Limit(limit).
		Order("pos.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosEquipments").
		Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch POS by sub area",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "POS retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by Commune id / UserUUID
func GetPaginatedPosByCommuneUUID(c *fiber.Ctx) error {
	db := database.DB

	UserUUID := c.Params("user_uuid")

	// Parse query parameters for pagination
	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.Pos
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.Pos{}).
		Joins("LEFT JOIN countries ON pos.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos.commune_uuid = communes.uuid").
		Joins("LEFT JOIN users ON pos.user_uuid = users.uuid").
		Where("pos.user_uuid = ?", UserUUID)

	// Apply advanced filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch paginated data
	err = query.
		Select("pos.*").
		Offset(offset).
		Limit(limit).
		Order("pos.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosForms").
		Preload("PosEquipments").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch POS by user",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "POS retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

func GetPaginatedPosByCommuneUserUUIDFilter(c *fiber.Ctx) error {
	db := database.DB

	communeUUID := c.Params("commune_uuid")

	// Parse query parameters for pagination
	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.Pos
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.Pos{}).
		Joins("LEFT JOIN countries ON pos.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos.commune_uuid = communes.uuid").
		Where("pos.commune_uuid = ?", communeUUID)

	// Apply advanced filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch paginated data
	err = query.
		Select("pos.*").
		Offset(offset).
		Limit(limit).
		Order("pos.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosForms").
		Preload("PosEquipments").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch POS by user",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "POS retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllPoss(c *fiber.Ctx) error {
	db := database.DB
	var data []models.Pos
	db.Where("status = ?", true).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Pos",
		"data":    data,
	})
}

// Get All data by manager
func GetAllPosByManager(c *fiber.Ctx) error {
	db := database.DB

	countryUUID := c.Params("country_uuid")

	var data []models.Pos
	db.Where("country_uuid = ?", countryUUID).
		Where("status = ?", true).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Pos",
		"data":    data,
	})
}

// Get All data by ASM
func GetAllPosByASM(c *fiber.Ctx) error {
	db := database.DB

	ProvinceUUID := c.Params("province_uuid")

	var data []models.Pos
	db.Where("province_uuid = ?", ProvinceUUID).
		Where("status = ?", true).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Pos",
		"data":    data,
	})
}

// Get All data by Supervisor
func GetAllPosBySup(c *fiber.Ctx) error {
	db := database.DB

	AreaUUID := c.Params("area_uuid")

	var data []models.Pos
	db.Where("area_uuid = ?", AreaUUID).
		Where("status = ?", true).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Pos",
		"data":    data,
	})
}

// Get All data by DR
func GetAllPosByDR(c *fiber.Ctx) error {
	db := database.DB

	SubAreaUUID := c.Params("sub_area_uuid")

	var data []models.Pos
	db.Where("sub_area_uuid = ?", SubAreaUUID).
		Where("status = ?", true).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Pos",
		"data":    data,
	})
}

// Get All data by CYclo
func GetAllPosByCyclo(c *fiber.Ctx) error {
	db := database.DB

	UserUUID := c.Params("user_uuid")

	var data []models.Pos
	db.Where("user_uuid = ?", UserUUID).
		Where("status = ?", true).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Pos",
		"data":    data,
	})
}

// Get one data
func GetPos(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var pos models.Pos
	db.Where("uuid = ?", uuid).
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosForms").
		Preload("PosEquipments").
		First(&pos)
	if pos.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No pos name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "pos found",
			"data":    pos,
		},
	)
}

// Create data
func CreatePos(c *fiber.Ctx) error {
	p := &models.Pos{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	p.Sync = true
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "pos created success",
			"data":    p,
		},
	)
}

// Update data
func UpdatePos(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		Name      string `gorm:"not null" json:"name"` // Celui qui vend
		Shop      string `json:"shop"`                 // Nom du shop
		Postype   string `json:"postype"`              // Type de POS
		Gerant    string `json:"gerant"`               // name of the onwer of the pos
		Avenue    string `json:"avenue"`
		Quartier  string `json:"quartier"`
		Reference string `json:"reference"`
		Telephone string `json:"telephone"`
		Image     string `json:"image"`

		CountryUUID  string `json:"country_uuid"`
		ProvinceUUID string `json:"province_uuid"`
		AreaUUID     string `json:"area_uuid"`
		SubAreaUUID  string `json:"sub_area_uuid"`

		ManagerUUID string `json:"manager_uuid"`
		Manager     string `json:"manager" gorm:"default:''"`
		SupportUUID string `json:"support_uuid"`
		Support     string `json:"support" gorm:"default:''"`
		AsmUUID     string `json:"asm_uuid"`
		Asm         string `json:"asm" gorm:"default:''"`
		SupUUID     string `json:"sup_uuid"`
		Sup         string `json:"sup" gorm:"default:''"`
		DrUUID      string `json:"dr_uuid"`
		Dr          string `json:"dr" gorm:"default:''"`
		CycloUUID   string `json:"cyclo_uuid"`
		Cyclo       string `json:"cyclo" gorm:"default:''"`

		UserUUID string `json:"user_uuid"`

		Status    bool   `json:"status"`
		Signature string `json:"signature"`
	}

	var updateData UpdateData

	if err := c.BodyParser(&updateData); err != nil {
		return c.Status(500).JSON(
			fiber.Map{
				"status":  "error",
				"message": "Review your input",
				"data":    nil,
			},
		)
	}

	pos := new(models.Pos)

	db.Where("uuid = ?", uuid).First(&pos)
	pos.Name = updateData.Name
	pos.Shop = updateData.Shop
	pos.Postype = updateData.Postype
	pos.Gerant = updateData.Gerant
	pos.Avenue = updateData.Avenue
	pos.Quartier = updateData.Quartier
	pos.Reference = updateData.Reference
	pos.Telephone = updateData.Telephone
	pos.CountryUUID = updateData.CountryUUID
	pos.ProvinceUUID = updateData.ProvinceUUID
	pos.AreaUUID = updateData.AreaUUID
	pos.SubAreaUUID = updateData.SubAreaUUID
	pos.AsmUUID = updateData.AsmUUID
	pos.Asm = updateData.Asm
	pos.SupUUID = updateData.SupUUID
	pos.Sup = updateData.Sup
	pos.DrUUID = updateData.DrUUID
	pos.Dr = updateData.Dr
	pos.CycloUUID = updateData.CycloUUID
	pos.Cyclo = updateData.Cyclo
	pos.UserUUID = updateData.UserUUID
	pos.Status = updateData.Status
	pos.Signature = updateData.Signature
	pos.Sync = true

	db.Save(&pos)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "POS updated success",
			"data":    pos,
		},
	)
}

// Delete data
func DeletePos(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var pos models.Pos
	db.Where("uuid = ?", uuid).First(&pos)
	if pos.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No POS name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&pos)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "POS deleted success",
			"data":    nil,
		},
	)
}

// Helper function to apply advanced filters for all paginated functions
func applyAdvancedFilters(query *gorm.DB, c *fiber.Ctx) *gorm.DB {
	// Filtres de recherche générale
	search := c.Query("search", "")

	// Filtres géographiques
	country := c.Query("country", "")
	province := c.Query("province", "")
	area := c.Query("area", "")
	subarea := c.Query("subarea", "")
	commune := c.Query("commune", "")

	// Filtres POS spécifiques
	posType := c.Query("posType", "")
	status := c.Query("status", "")
	gerant := c.Query("gerant", "")
	quartier := c.Query("quartier", "")

	// Filtres utilisateur
	userFullname := c.Query("userFullname", "")
	userSearch := c.Query("userSearch", "")

	// Filtres hiérarchie commerciale avec recherche intégrée
	asm := c.Query("asm", "")
	asmSearch := c.Query("asmSearch", "")
	supervisor := c.Query("supervisor", "")
	supervisorSearch := c.Query("supervisorSearch", "")
	dr := c.Query("dr", "")
	drSearch := c.Query("drSearch", "")
	cyclo := c.Query("cyclo", "")
	cycloSearch := c.Query("cycloSearch", "")

	// Filtres temporels
	quickDate := c.Query("quickDate", "")

	// 🔍 Recherche générale dans tous les champs pertinents
	if search != "" {
		query = query.Where("pos.name ILIKE ? OR pos.shop ILIKE ? OR pos.postype ILIKE ? OR pos.gerant ILIKE ? OR pos.quartier ILIKE ? OR pos.reference ILIKE ?",
			"%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	// 🌍 Filtres géographiques
	if country != "" {
		query = query.Where("countries.name = ?", country)
	}
	if province != "" {
		query = query.Where("provinces.name = ?", province)
	}
	if area != "" {
		query = query.Where("areas.name = ?", area)
	}
	if subarea != "" {
		query = query.Where("sub_areas.name = ?", subarea)
	}
	if commune != "" {
		query = query.Where("communes.name = ?", commune)
	}

	// 🏪 Filtres POS spécifiques
	if posType != "" {
		query = query.Where("pos.postype = ?", posType)
	}
	if gerant != "" {
		query = query.Where("pos.gerant ILIKE ?", "%"+gerant+"%")
	}
	if quartier != "" {
		query = query.Where("pos.quartier ILIKE ?", "%"+quartier+"%")
	}

	// 📊 Filtre statut du POS
	switch status {
	case "active":
		query = query.Where("pos.status = ?", true)
	case "inactive":
		query = query.Where("pos.status = ?", false)
	}

	// 👤 Filtres utilisateur
	if userFullname != "" {
		query = query.Where("signature = ?", userFullname)
	}
	if userSearch != "" {
		query = query.Where("signature ILIKE ?", "%"+userSearch+"%")
	}

	// 👔 Filtres hiérarchie commerciale avec recherche intégrée
	// ASM - support recherche intégrée
	if asm != "" {
		query = query.Where("pos.asm = ?", asm)
	}
	if asmSearch != "" {
		query = query.Where("pos.asm ILIKE ?", "%"+asmSearch+"%")
	}

	// Supervisor - support recherche intégrée
	if supervisor != "" {
		query = query.Where("pos.sup = ?", supervisor)
	}
	if supervisorSearch != "" {
		query = query.Where("pos.sup ILIKE ?", "%"+supervisorSearch+"%")
	}

	// DR - support recherche intégrée
	if dr != "" {
		query = query.Where("pos.dr = ?", dr)
	}
	if drSearch != "" {
		query = query.Where("pos.dr ILIKE ?", "%"+drSearch+"%")
	}

	// Cyclo - support recherche intégrée
	if cyclo != "" {
		query = query.Where("pos.cyclo = ?", cyclo)
	}
	if cycloSearch != "" {
		query = query.Where("pos.cyclo ILIKE ?", "%"+cycloSearch+"%")
	}

	// 📅 Filtres rapides par date
	if quickDate != "" {
		switch quickDate {
		case "today":
			query = query.Where("DATE(pos.created_at) = CURRENT_DATE")
		case "yesterday":
			query = query.Where("DATE(pos.created_at) = CURRENT_DATE - INTERVAL '1 day'")
		case "last7days":
			query = query.Where("pos.created_at >= CURRENT_DATE - INTERVAL '7 days'")
		case "last30days":
			query = query.Where("pos.created_at >= CURRENT_DATE - INTERVAL '30 days'")
		}
	}

	return query
}

// applyAdvancedFiltersForExcel applies advanced filters including date range for Excel reports
func applyAdvancedFiltersForExcel(query *gorm.DB, c *fiber.Ctx) *gorm.DB {
	// Apply all standard filters first
	query = applyAdvancedFilters(query, c)

	// Additional filters specific to Excel reports
	startDate := c.Query("startDate", "")
	endDate := c.Query("endDate", "")

	// 📅 Filtres par plage de dates personnalisée (uniquement pour Excel)
	if startDate != "" && endDate != "" {
		// Validation et parsing des dates
		startTime, err := time.Parse("2006-01-02", startDate)
		if err == nil {
			endTime, err := time.Parse("2006-01-02", endDate)
			if err == nil {
				// Ajouter 23:59:59 à la date de fin pour inclure toute la journée
				endTime = endTime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
				query = query.Where("pos.created_at >= ? AND pos.created_at <= ?", startTime, endTime)
			}
		}
	} else if startDate != "" {
		// Filtre à partir d'une date de début seulement
		startTime, err := time.Parse("2006-01-02", startDate)
		if err == nil {
			query = query.Where("pos.created_at >= ?", startTime)
		}
	} else if endDate != "" {
		// Filtre jusqu'à une date de fin seulement
		endTime, err := time.Parse("2006-01-02", endDate)
		if err == nil {
			// Ajouter 23:59:59 à la date de fin pour inclure toute la journée
			endTime = endTime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
			query = query.Where("pos.created_at <= ?", endTime)
		}
	}

	return query
}

// GeneratePosExcelReport generates an Excel report for POS data
func GeneratePosExcelReport(c *fiber.Ctx) error {
	db := database.DB

	// Parse query parameters for filtering (same as pagination)
	var dataList []models.Pos
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.Pos{}).
		Joins("LEFT JOIN countries ON pos.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos.commune_uuid = communes.uuid").
		Joins("LEFT JOIN users ON pos.user_uuid = users.uuid")

	// Apply advanced filters (including date range filters for Excel)
	query = applyAdvancedFiltersForExcel(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Get all filtered data for the report (no pagination for Excel)
	// Limit to 10000 records to prevent memory issues
	limit := 10000
	if totalRecords > int64(limit) {
		return c.Status(400).JSON(fiber.Map{
			"status":  "error",
			"message": fmt.Sprintf("Trop de données pour le rapport Excel. Maximum %d enregistrements autorisés, %d trouvés. Veuillez utiliser des filtres plus spécifiques.", limit, totalRecords),
			"data":    nil,
		})
	}

	err := query.
		Select("pos.*").
		Order("pos.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("PosForms").
		Preload("PosEquipments").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Échec de la récupération des données POS pour le rapport Excel",
			"error":   err.Error(),
		})
	}

	// Create Excel file
	config := utils.ExcelReportConfig{
		Title:       "Rapport des Points de Vente (POS)",
		CompanyName: "MSPOS System",
		ReportDate:  time.Now(),
		Author:      "Système de Rapport Automatique",
	}

	f := utils.CreateExcelFile(config)
	sheetName := "Rapport POS"

	// Rename default sheet
	f.SetSheetName("Sheet1", sheetName)

	// Setup styles
	styles, err := utils.SetupExcelStyles(f)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Erreur lors de la configuration des styles Excel",
			"error":   err.Error(),
		})
	}

	// Add report header
	err = utils.AddReportHeader(f, sheetName, config, styles)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Erreur lors de l'ajout de l'en-tête du rapport",
			"error":   err.Error(),
		})
	}

	// Add summary statistics
	summaryData := map[string]interface{}{
		"Total des POS":      totalRecords,
		"POS Actifs":         countActivePos(dataList),
		"POS Inactifs":       countInactivePos(dataList),
		"Total Provinces":    countUniqueProvinces(dataList),
		"Total Aires":        countUniqueAreas(dataList),
		"Total Sous-Aires":   countUniqueSubAreas(dataList),
		"Date de génération": time.Now().Format("02/01/2006 15:04:05"),
	}

	err = utils.AddSummaryTable(f, sheetName, summaryData, 6, styles)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Erreur lors de l'ajout du résumé",
			"error":   err.Error(),
		})
	}

	// Define headers for the main data table
	headers := []string{
		"UUID", "Nom POS", "Shop", "Type POS", "Gérant", "Avenue", "Quartier",
		"Référence", "Téléphone", "Pays", "Province", "Aire", "Sous-Aire",
		"Commune", "Utilisateur", "ASM", "Superviseur", "DR", "Cyclo",
		"Statut", "Date Création", "Date Modification",
	}

	// Start data table after summary (row 15)
	dataStartRow := 15

	// Add main data table title
	f.SetCellValue(sheetName, fmt.Sprintf("A%d", dataStartRow), "DONNÉES DÉTAILLÉES DES POS")
	f.SetCellStyle(sheetName, fmt.Sprintf("A%d", dataStartRow), fmt.Sprintf("V%d", dataStartRow), styles["title"])
	f.MergeCell(sheetName, fmt.Sprintf("A%d", dataStartRow), fmt.Sprintf("V%d", dataStartRow))
	dataStartRow += 2

	// Add headers
	for i, header := range headers {
		col := string(rune('A' + i))
		if i >= 26 {
			// Handle columns beyond Z (AA, AB, etc.)
			col = string(rune('A'+(i/26-1))) + string(rune('A'+(i%26)))
		}
		cell := fmt.Sprintf("%s%d", col, dataStartRow)
		f.SetCellValue(sheetName, cell, header)
		f.SetCellStyle(sheetName, cell, cell, styles["header"])
	}

	// Add data rows
	for i, pos := range dataList {
		row := dataStartRow + 1 + i

		// Convert status to readable format
		statusText := "Inactif"
		if pos.Status {
			statusText = "Actif"
		}

		// Get related data safely
		countryName := ""
		if pos.Country.Name != "" {
			countryName = pos.Country.Name
		}

		provinceName := ""
		if pos.Province.Name != "" {
			provinceName = pos.Province.Name
		}

		areaName := ""
		if pos.Area.Name != "" {
			areaName = pos.Area.Name
		}

		subAreaName := ""
		if pos.SubArea.Name != "" {
			subAreaName = pos.SubArea.Name
		}

		communeName := ""
		if pos.Commune.Name != "" {
			communeName = pos.Commune.Name
		}

		userName := ""
		if pos.User.Fullname != "" {
			userName = pos.User.Fullname
		}

		// Data array
		rowData := []interface{}{
			pos.UUID,
			pos.Name,
			pos.Shop,
			pos.Postype,
			pos.Gerant,
			pos.Avenue,
			pos.Quartier,
			pos.Reference,
			pos.Telephone,
			countryName,
			provinceName,
			areaName,
			subAreaName,
			communeName,
			userName,
			pos.Asm,
			pos.Sup,
			pos.Dr,
			pos.Cyclo,
			statusText,
			pos.CreatedAt.Format("02/01/2006 15:04:05"),
			pos.UpdatedAt.Format("02/01/2006 15:04:05"),
		}

		// Set data in cells
		for j, data := range rowData {
			col := string(rune('A' + j))
			if j >= 26 {
				col = string(rune('A'+(j/26-1))) + string(rune('A'+(j%26)))
			}
			cell := fmt.Sprintf("%s%d", col, row)
			f.SetCellValue(sheetName, cell, data)

			// Apply appropriate style based on data type
			style := styles["data"]
			if j == 19 { // Status column
				if pos.Status {
					style = styles["success"]
				} else {
					style = styles["warning"]
				}
			} else if j == 20 || j == 21 { // Date columns
				style = styles["date"]
			}
			f.SetCellStyle(sheetName, cell, cell, style)
		}
	}

	// Auto-fit columns
	columns := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V"}
	err = utils.AutoFitColumns(f, sheetName, columns, 15.0)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Erreur lors de l'ajustement des colonnes",
			"error":   err.Error(),
		})
	}

	// Generate filename with timestamp
	filename := fmt.Sprintf("rapport_pos_%s.xlsx", time.Now().Format("2006-01-02_15-04-05"))

	// Set response headers for file download
	c.Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))

	// Write file to response
	buffer, err := f.WriteToBuffer()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Erreur lors de la génération du fichier Excel",
			"error":   err.Error(),
		})
	}

	return c.Send(buffer.Bytes())
}

// Helper functions for summary statistics
func countActivePos(posList []models.Pos) int {
	count := 0
	for _, pos := range posList {
		if pos.Status {
			count++
		}
	}
	return count
}

func countInactivePos(posList []models.Pos) int {
	count := 0
	for _, pos := range posList {
		if !pos.Status {
			count++
		}
	}
	return count
}

func countUniqueProvinces(posList []models.Pos) int {
	provinces := make(map[string]bool)
	for _, pos := range posList {
		if pos.Province.Name != "" {
			provinces[pos.Province.Name] = true
		}
	}
	return len(provinces)
}

func countUniqueAreas(posList []models.Pos) int {
	areas := make(map[string]bool)
	for _, pos := range posList {
		if pos.Area.Name != "" {
			areas[pos.Area.Name] = true
		}
	}
	return len(areas)
}

func countUniqueSubAreas(posList []models.Pos) int {
	subAreas := make(map[string]bool)
	for _, pos := range posList {
		if pos.SubArea.Name != "" {
			subAreas[pos.SubArea.Name] = true
		}
	}
	return len(subAreas)
}

func MapPos(c *fiber.Ctx) error {
	db := database.DB

	posUUID := c.Params("pos_uuid")

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Latitude  float64 `json:"latitude"`  // Latitude of the user
		Longitude float64 `json:"longitude"` // Longitude of the user
		Signature string  `json:"signature"`
		PosName   string  `json:"pos_name"`   // Name of the POS
		PosUUID   string  `json:"pos_uuid"`   // UUID of the POS
		Postype   string  `json:"postype"`    // Type de POS
		Asm       string  `json:"asm"`        // Name of the ASM
		Sup       string  `json:"sup"`        // Name of the Supervisor
		Dr        string  `json:"dr"`         // Name of the DR
		Cyclo     string  `json:"cyclo"`      // Name of the Cyclo
		CreatedAt string  `json:"created_at"` // Creation date of the form
	}

	err := db.Table("pos_forms").
		Joins("JOIN pos ON pos.uuid = pos_forms.pos_uuid").
		Select(`
			pos_forms.latitude AS latitude,
			pos_forms.longitude AS longitude,
			pos_forms.signature AS signature,
			pos_forms.created_at AS created_at,
			pos.name AS pos_name,
			pos.uuid AS pos_uuid,
			pos.postype AS postype,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				ELSE pos_forms.asm 
			END AS asm,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN '' 
				ELSE pos_forms.sup 
			END AS sup,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				WHEN pos_forms.signature = pos_forms.sup THEN '' 
				ELSE pos_forms.dr 
			END AS dr,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				WHEN pos_forms.signature = pos_forms.sup THEN ''
				WHEN pos_forms.signature = pos_forms.dr THEN '' 
				ELSE pos_forms.cyclo 
			END AS cyclo
		`).Where("pos_forms.pos_uuid = ?", posUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})

}

package area

import (
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// Paginate
func GetPaginatedAreas(c *fiber.Ctx) error {
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

	// Parse search query
	search := c.Query("search", "")

	var dataList []models.Area
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Area{}).
		Joins("LEFT JOIN countries ON areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON areas.province_uuid = provinces.uuid").
		Where("areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Joins("LEFT JOIN countries ON areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON areas.province_uuid = provinces.uuid").
		Where("areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			areas.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = areas.country_uuid
				AND u2.province_uuid = areas.province_uuid
				AND u2.area_uuid = areas.uuid
			) AS total_users,
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = areas.country_uuid 
				AND p.province_uuid = areas.province_uuid
				AND p.area_uuid = areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT pf.uuid)
				FROM
				pos_forms pf  
				WHERE pf.area_uuid = areas.uuid
				AND pf.area_uuid != ''
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("SubAreas").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Users").
		// Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch countries",
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
		"message":    "Areas retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Query Area by Country ID
func GetAreaByCountry(c *fiber.Ctx) error {
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

	// Parse search query
	search := c.Query("search", "")

	var dataList []models.Area
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Area{}).
		Joins("LEFT JOIN countries ON areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON areas.province_uuid = provinces.uuid").
		Where("areas.country_uuid = ?", CountryUUID).
		Where("areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Joins("LEFT JOIN countries ON areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON areas.province_uuid = provinces.uuid").
		Where("areas.country_uuid = ?", CountryUUID).
		Where("areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			areas.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2 
				WHERE u2.country_uuid = areas.country_uuid
				AND u2.province_uuid = areas.province_uuid
				AND u2.area_uuid = areas.uuid
			) AS total_users,
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p  
				WHERE p.country_uuid = areas.country_uuid 
				AND p.province_uuid = areas.province_uuid
				AND p.area_uuid = areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT pf.uuid)
				FROM
				pos_forms pf  
				WHERE pf.area_uuid = areas.uuid
				AND pf.area_uuid != ''
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("SubAreas").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Users").
		// Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch countries",
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
		"message":    "Areas retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Query Area by Asm ID
func GetAreaByASM(c *fiber.Ctx) error {
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

	// Parse search query
	search := c.Query("search", "")

	var dataList []models.Area
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Area{}).
		Joins("LEFT JOIN countries ON areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON areas.province_uuid = provinces.uuid").
		Where("areas.province_uuid = ?", ProvinceUUID).
		Where("areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Joins("LEFT JOIN countries ON areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON areas.province_uuid = provinces.uuid").
		Where("areas.province_uuid = ?", ProvinceUUID).
		Where("areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			areas.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = areas.country_uuid
				AND u2.province_uuid = areas.province_uuid
				AND u2.area_uuid = areas.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = areas.country_uuid 
				AND p.province_uuid = areas.province_uuid
				AND p.area_uuid = areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT pf.uuid)
				FROM
				pos_forms pf  
				WHERE pf.area_uuid = areas.uuid
				AND pf.area_uuid != ''
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("SubAreas").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Users").
		// Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch countries",
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
		"message":    "Areas retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Query Area by Sup ID
func GetAreaBySups(c *fiber.Ctx) error {
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

	// Parse search query
	search := c.Query("search", "")

	var dataList []models.Area
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Area{}).
		Joins("LEFT JOIN countries ON areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON areas.province_uuid = provinces.uuid").
		Where("areas.uuid = ?", AreaUUID).
		Where("areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Joins("LEFT JOIN countries ON areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON areas.province_uuid = provinces.uuid").
		Where("areas.uuid = ?", AreaUUID).
		Where("areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			areas.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = areas.country_uuid
				AND u2.province_uuid = areas.province_uuid
				AND u2.area_uuid = areas.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = areas.country_uuid 
				AND p.province_uuid = areas.province_uuid
				AND p.area_uuid = areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT pf.uuid)
				FROM
				pos_forms pf  
				WHERE pf.area_uuid = areas.uuid
				AND pf.area_uuid != ''
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("SubAreas").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Users").
		// Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch countries",
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
		"message":    "Areas retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllAreas(c *fiber.Ctx) error {
	db := database.DB
	var data []models.Area
	db.Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Areas",
		"data":    data,
	})
}

// Get All data by province uuid Dashboard
func GetAllAreasByProvinceUUID(c *fiber.Ctx) error {
	db := database.DB

	ProvinceUUID := c.Params("province_uuid")

	var data []models.Area
	db.Where("province_uuid = ?", ProvinceUUID).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Areas by province",
		"data":    data,
	})
}

// query data
func GetAreaByID(c *fiber.Ctx) error {
	id := c.Params("id")
	db := database.DB
	var areas []models.Area
	db.Where("province_uuid = ?", id).Find(&areas)

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "areas by id found",
		"data":    areas,
	})
}

// query data
func GetSupAreaByID(c *fiber.Ctx) error {
	id := c.Params("id")
	db := database.DB
	var areas []models.Area
	db.Where("sup_uuid = ?", id).Find(&areas)

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "poss by id found",
		"data":    areas,
	})
}

// Get one data
func GetArea(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var area models.Area
	db.Where("uuid = ?", uuid).First(&area)
	if area.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No area name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "area found",
			"data":    area,
		},
	)
}

// Get one data by name
func GetAreaByName(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var area models.Area
	db.Where("uuid = ?", uuid).
		Preload("Country").
		Preload("Province").
		First(&area)
	if area.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No area name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "area found",
			"data":    area,
		},
	)
}

// Create data
func CreateArea(c *fiber.Ctx) error {
	p := &models.Area{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "area created success",
			"data":    p,
		},
	)
}

// Update data
func UpdateArea(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		UUID         string `json:"uuid"`
		Name         string `gorm:"not null" json:"name"`
		CountryUUID  string `json:"country_uuid" gorm:"type:varchar(255);not null"`
		ProvinceUUID string `json:"province_uuid" gorm:"type:varchar(255);not null"`
		Signature    string `json:"signature"`
	}

	var updateData UpdateData

	if err := c.BodyParser(&updateData); err != nil {
		return c.Status(500).JSON(
			fiber.Map{
				"status":  "error",
				"message": "Review your iunput",
				"data":    nil,
			},
		)
	}

	area := new(models.Area)

	db.Where("uuid = ?", uuid).First(&area)
	area.Name = updateData.Name
	area.CountryUUID = updateData.CountryUUID
	area.ProvinceUUID = updateData.ProvinceUUID
	area.Signature = updateData.Signature

	db.Save(&area)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "area updated success",
			"data":    area,
		},
	)

}

// Delete data
func DeleteArea(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var area models.Area
	db.Where("uuid = ?", uuid).First(&area)
	if area.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No area name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&area)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "area deleted success",
			"data":    nil,
		},
	)
}

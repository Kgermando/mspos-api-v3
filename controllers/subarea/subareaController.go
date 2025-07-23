package Subarea

import (
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// Paginate
func GetPaginatedSubArea(c *fiber.Ctx) error {
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

	var dataList []models.SubArea
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.SubArea{}).
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			sub_areas.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = sub_areas.country_uuid
				AND u2.province_uuid = sub_areas.province_uuid
				AND u2.area_uuid = sub_areas.area_uuid
				AND u2.sub_area_uuid = sub_areas.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = sub_areas.country_uuid 
				AND p.province_uuid = sub_areas.province_uuid
				AND p.area_uuid = sub_areas.area_uuid
				AND p.sub_area_uuid = sub_areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = sub_areas.country_uuid
				AND ps.province_uuid = sub_areas.province_uuid
				AND ps.area_uuid = sub_areas.area_uuid
				AND ps.sub_area_uuid = sub_areas.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Posforms").
		// Preload("Cyclos").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch subareas",
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
		"message":    "Subareas retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by Country
func GetPaginatedSubAreaByCountry(c *fiber.Ctx) error {
	db := database.DB

	countryUUID := c.Params("country_uuid")

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

	var dataList []models.SubArea
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.SubArea{}).
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.country_uuid = ?", countryUUID).
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.country_uuid = ?", countryUUID).
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			sub_areas.*, 
			( 
				SELECT COUNT(DISTINCT u2.uuid) 
				FROM users u2
				WHERE u2.country_uuid = sub_areas.country_uuid
				AND u2.province_uuid = sub_areas.province_uuid
				AND u2.area_uuid = sub_areas.area_uuid
				AND u2.sub_area_uuid = sub_areas.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = sub_areas.country_uuid 
				AND p.province_uuid = sub_areas.province_uuid
				AND p.area_uuid = sub_areas.area_uuid
				AND p.sub_area_uuid = sub_areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = sub_areas.country_uuid
				AND ps.province_uuid = sub_areas.province_uuid
				AND ps.area_uuid = sub_areas.area_uuid
				AND ps.sub_area_uuid = sub_areas.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Posforms").
		// Preload("Cyclos").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch subareas",
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
		"message":    "Subares retrieved asm successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by ASM
func GetPaginatedSubAreaByASM(c *fiber.Ctx) error {
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

	var dataList []models.SubArea
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.SubArea{}).
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.province_uuid = ?", ProvinceUUID).
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.province_uuid = ?", ProvinceUUID).
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			sub_areas.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = sub_areas.country_uuid
				AND u2.province_uuid = sub_areas.province_uuid
				AND u2.area_uuid = sub_areas.area_uuid
				AND u2.sub_area_uuid = sub_areas.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = sub_areas.country_uuid 
				AND p.province_uuid = sub_areas.province_uuid
				AND p.area_uuid = sub_areas.area_uuid
				AND p.sub_area_uuid = sub_areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = sub_areas.country_uuid
				AND ps.province_uuid = sub_areas.province_uuid
				AND ps.area_uuid = sub_areas.area_uuid
				AND ps.sub_area_uuid = sub_areas.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Posforms").
		// Preload("Cyclos").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch subareas",
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
		"message":    "Subares retrieved asm successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by Sup
func GetPaginatedSubAreaBySup(c *fiber.Ctx) error {
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

	var dataList []models.SubArea
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.SubArea{}).
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.area_uuid = ?", AreaUUID).
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.area_uuid = ?", AreaUUID).
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			sub_areas.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = sub_areas.country_uuid
				AND u2.province_uuid = sub_areas.province_uuid
				AND u2.area_uuid = sub_areas.area_uuid
				AND u2.sub_area_uuid = sub_areas.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = sub_areas.country_uuid 
				AND p.province_uuid = sub_areas.province_uuid
				AND p.area_uuid = sub_areas.area_uuid
				AND p.sub_area_uuid = sub_areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = sub_areas.country_uuid
				AND ps.province_uuid = sub_areas.province_uuid
				AND ps.area_uuid = sub_areas.area_uuid
				AND ps.sub_area_uuid = sub_areas.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Posforms").
		// Preload("Cyclos").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch subareas",
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
		"message":    "Subares Sup retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Query by DR id
func GetAllSubAreaDr(c *fiber.Ctx) error {
	db := database.DB

	subAreaUUID := c.Params("sub_area_uuid")

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

	var dataList []models.SubArea
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.SubArea{}).
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.uuid = ?", subAreaUUID).
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON sub_areas.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON sub_areas.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON sub_areas.area_uuid = areas.uuid").
		Where("sub_areas.uuid = ?", subAreaUUID).
		Where("sub_areas.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			sub_areas.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = sub_areas.country_uuid
				AND u2.province_uuid = sub_areas.province_uuid
				AND u2.area_uuid = sub_areas.area_uuid
				AND u2.sub_area_uuid = sub_areas.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = sub_areas.country_uuid 
				AND p.province_uuid = sub_areas.province_uuid
				AND p.area_uuid = sub_areas.area_uuid
				AND p.sub_area_uuid = sub_areas.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = sub_areas.country_uuid
				AND ps.province_uuid = sub_areas.province_uuid
				AND ps.area_uuid = sub_areas.area_uuid
				AND ps.sub_area_uuid = sub_areas.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("Communes").
		// Preload("Pos").
		// Preload("Posforms").
		// Preload("Cyclos").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch subareas",
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
		"message":    "Subareas Dr retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data DR
func GetAllSubArea(c *fiber.Ctx) error {
	db := database.DB

	var data []models.SubArea
	db.Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Subarea",
		"data":    data,
	})
}

// Get All data by Subarea UUID
func GetAllDataBySubAreaByAreaUUID(c *fiber.Ctx) error {
	db := database.DB

	areaUUID := c.Params("area_uuid")

	var data []models.SubArea
	db.
		Where("area_uuid = ?", areaUUID).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Subarea",
		"data":    data,
	})
}

// Get one data
func GetSubArea(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	var SubArea models.SubArea
	db.Where("uuid = ?", uuid).First(&SubArea)
	if SubArea.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Subarea name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Subarea found",
			"data":    SubArea,
		},
	)
}

// Get one data by name
func GetSubAreaByName(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	var SubArea models.SubArea
	db.Where("uuid = ?", uuid).
		Preload("Country").
		Preload("Province").
		Preload("Area").
		First(&SubArea)
	if SubArea.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Subarea name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Subarea found",
			"data":    SubArea,
		},
	)
}

// Create data
func CreateSubArea(c *fiber.Ctx) error {
	p := &models.SubArea{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Subarea created success",
			"data":    p,
		},
	)
}

// Update data
func UpdateSubArea(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		UUID string `json:"uuid"`

		Name         string `gorm:"not null" json:"name"`
		CountryUUID  string `json:"country_uuid" gorm:"type:varchar(255);not null"`
		ProvinceUUID string `json:"province_uuid" gorm:"type:varchar(255);not null"`
		AreaUUID     string `json:"area_uuid" gorm:"type:varchar(255);not null"`
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

	SubArea := new(models.SubArea)

	db.Where("uuid = ?", uuid).First(&SubArea)
	SubArea.Name = updateData.Name
	SubArea.CountryUUID = updateData.CountryUUID
	SubArea.ProvinceUUID = updateData.ProvinceUUID
	SubArea.AreaUUID = updateData.AreaUUID
	SubArea.Signature = updateData.Signature

	db.Save(&SubArea)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Subarea updated success",
			"data":    SubArea,
		},
	)

}

// Delete data
func DeleteSubarea(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var SubArea models.SubArea
	db.Where("uuid = ?", uuid).First(&SubArea)
	if SubArea.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Subarea name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&SubArea)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Subarea deleted success",
			"data":    nil,
		},
	)
}

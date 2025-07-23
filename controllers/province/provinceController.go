package province

import (
	"fmt"
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// Paginate
func GetPaginatedProvince(c *fiber.Ctx) error {
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

	var dataList []models.Province
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Province{}).
		Joins("LEFT JOIN countries ON provinces.country_uuid = countries.uuid").
		Where("provinces.name ILIKE ? OR countries.name ILIKE ?", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Joins("LEFT JOIN countries ON provinces.country_uuid = countries.uuid").
		Where("provinces.name ILIKE ? OR countries.name ILIKE ?", "%"+search+"%", "%"+search+"%").
		Select(` 
			provinces.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = provinces.country_uuid
				AND u2.province_uuid = provinces.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = provinces.country_uuid 
				AND p.province_uuid = provinces.uuid
			) AS total_pos, 
			(
				SELECT COUNT(DISTINCT ps.uuid)
				FROM pos_forms ps  
				WHERE ps.country_uuid = provinces.country_uuid
				AND ps.province_uuid = provinces.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Areas").
		Preload("SubAreas").
		Preload("Communes").
		Preload("Brands").
		// Preload("Pos").
		// Preload("Users").
		// Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces",
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
		"message":    "Get all Provinces Paginate success",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Query Country
func GetPaginatedProvinceByCountry(c *fiber.Ctx) error {
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

	var dataList []models.Province
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Province{}).
		Joins("LEFT JOIN countries ON provinces.country_uuid = countries.uuid").
		Where("provinces.country_uuid = ?", CountryUUID).
		Where("provinces.name ILIKE ? OR countries.name ILIKE ?", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Joins("LEFT JOIN countries ON provinces.country_uuid = countries.uuid").
		Where("provinces.country_uuid = ?", CountryUUID).
		Where("provinces.name ILIKE ? OR countries.name ILIKE ?", "%"+search+"%", "%"+search+"%").
		Select(` 
			provinces.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = provinces.country_uuid
				AND u2.province_uuid = provinces.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = provinces.country_uuid 
				AND p.province_uuid = provinces.uuid
			) AS total_pos, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = provinces.country_uuid
				AND u2.province_uuid = provinces.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = provinces.country_uuid 
				AND p.province_uuid = provinces.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = provinces.country_uuid
				AND ps.province_uuid = provinces.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Areas").
		Preload("SubAreas").
		Preload("Communes").
		Preload("Brands").
		// Preload("Pos").
		// Preload("Users").
		// Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	fmt.Printf("Total Records: %d,Total Page: %d, Total Pages: %d\n", totalRecords, page, totalPages)

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
		"message":    "Provinces retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Query ASM
func GetPaginatedASM(c *fiber.Ctx) error {
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

	var dataList []models.Province
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Province{}).
		Joins("LEFT JOIN countries ON provinces.country_uuid = countries.uuid").
		Where("provinces.uuid = ?", ProvinceUUID).
		Where("provinces.name ILIKE ? OR countries.name ILIKE ?", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Joins("LEFT JOIN countries ON provinces.country_uuid = countries.uuid").
		Where("provinces.uuid = ?", ProvinceUUID).
		Where("provinces.name ILIKE ? OR countries.name ILIKE ?", "%"+search+"%", "%"+search+"%").
		Select(` 
			provinces.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = provinces.country_uuid
				AND u2.province_uuid = provinces.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = provinces.country_uuid 
				AND p.province_uuid = provinces.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = provinces.country_uuid
				AND ps.province_uuid = provinces.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Areas").
		Preload("SubAreas").
		Preload("Communes").
		Preload("Brands").
		// Preload("Pos").
		// Preload("Users").
		// Preload("PosForms").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	fmt.Printf("Total Records: %d,Total Page: %d, Total Pages: %d\n", totalRecords, page, totalPages)

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
		"message":    "Provinces retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllProvinces(c *fiber.Ctx) error {
	db := database.DB
	var data []models.Province
	db.Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All provinces support",
		"data":    data,
	})
}

// Get All data by country Dashboard
func GetAllProvinceByCountry(c *fiber.Ctx) error {
	db := database.DB

	countryUUID := c.Params("country_uuid")

	var data []models.Province

	db.
		Where("country_uuid = ?", countryUUID).
		Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All province by country",
		"data":    data,
	})
}

// Get one data
func GetProvince(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var province models.Province
	db.Where("uuid = ?", uuid).First(&province)
	if province.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Province name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Province found",
			"data":    province,
		},
	)
}

// Get one data by name
func GetProvinceByName(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var province models.Province
	db.Where("uuid = ?", uuid).First(&province)
	if province.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Province name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Province found",
			"data":    province,
		},
	)
}

// Create data
func CreateProvince(c *fiber.Ctx) error {
	p := &models.Province{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Province created success",
			"data":    p,
		},
	)
}

// Update data
func UpdateProvince(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		UUID string `json:"uuid"`

		Name        string `json:"name"`
		CountryUUID string `json:"country_uuid"`
		Signature   string `json:"signature"`
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

	province := new(models.Province)

	db.Where("uuid = ?", uuid).First(&province)
	province.Name = updateData.Name
	province.CountryUUID = updateData.CountryUUID
	province.Signature = updateData.Signature

	db.Save(&province)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Province updated success",
			"data":    province,
		},
	)

}

// Delete data
func DeleteProvince(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var province models.Province
	db.Where("uuid = ?", uuid).First(&province)
	if province.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Province name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&province)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Province deleted success",
			"data":    nil,
		},
	)
}

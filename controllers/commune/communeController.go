package commune

import (
	"fmt"
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// Paginate
func GetPaginatedCommunes(c *fiber.Ctx) error {
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

	var dataList []models.Commune
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Commune{}).
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			communes.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = communes.country_uuid
				AND u2.province_uuid = communes.province_uuid
				AND u2.area_uuid = communes.area_uuid
				AND u2.sub_area_uuid = communes.sub_area_uuid
				AND u2.commune_uuid = communes.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = communes.country_uuid 
				AND p.province_uuid = communes.province_uuid
				AND p.area_uuid = communes.area_uuid
				AND p.sub_area_uuid = communes.sub_area_uuid
				AND p.commune_uuid = communes.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = communes.country_uuid
				AND ps.province_uuid = communes.province_uuid
				AND ps.area_uuid = communes.area_uuid
				AND ps.sub_area_uuid = communes.sub_area_uuid
				AND ps.commune_uuid = communes.uuid
			) AS total_posforms
		`).
		Offset(offset).
		Limit(limit).
		Order("communes.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		// Preload("Pos").
		// Preload("PosForms").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch Communes",
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
		"message":    "Provinces retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// query data ASM by Country id
func GetPaginatedCommunesByCountryUUID(c *fiber.Ctx) error {
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

	var dataList []models.Commune
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Commune{}).
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.country_uuid = ?", CountryUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.country_uuid = ?", CountryUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			communes.*,  
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = communes.country_uuid
				AND u2.province_uuid = communes.province_uuid
				AND u2.area_uuid = communes.area_uuid
				AND u2.sub_area_uuid = communes.sub_area_uuid
				AND u2.commune_uuid = communes.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = communes.country_uuid 
				AND p.province_uuid = communes.province_uuid
				AND p.area_uuid = communes.area_uuid
				AND p.sub_area_uuid = communes.sub_area_uuid
				AND p.commune_uuid = communes.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = communes.country_uuid
				AND ps.province_uuid = communes.province_uuid
				AND ps.area_uuid = communes.area_uuid
				AND ps.sub_area_uuid = communes.sub_area_uuid
				AND ps.commune_uuid = communes.uuid
			) AS total_posforms
		`).
		Offset(offset).
		Limit(limit).
		Order("communes.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		// Preload("Pos").
		// Preload("PosForms").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch Communes",
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
		"message":    "Provinces retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// query data ASM by Province id
func GetPaginatedCommunesByProvinceUUID(c *fiber.Ctx) error {
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

	var dataList []models.Commune
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Commune{}).
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.province_uuid = ?", ProvinceUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.province_uuid = ?", ProvinceUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			communes.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = communes.country_uuid
				AND u2.province_uuid = communes.province_uuid
				AND u2.area_uuid = communes.area_uuid
				AND u2.sub_area_uuid = communes.sub_area_uuid
				AND u2.commune_uuid = communes.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = communes.country_uuid 
				AND p.province_uuid = communes.province_uuid
				AND p.area_uuid = communes.area_uuid
				AND p.sub_area_uuid = communes.sub_area_uuid
				AND p.commune_uuid = communes.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = communes.country_uuid
				AND ps.province_uuid = communes.province_uuid
				AND ps.area_uuid = communes.area_uuid
				AND ps.sub_area_uuid = communes.sub_area_uuid
				AND ps.commune_uuid = communes.uuid
			) AS total_posforms
		`).
		Offset(offset).
		Limit(limit).
		Order("communes.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		// Preload("Pos").
		// Preload("PosForms").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch Communes",
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
		"message":    "Provinces retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// query data SUP by Area id
func GetPaginatedCommunesByAreaUUID(c *fiber.Ctx) error {
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

	var dataList []models.Commune
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Commune{}).
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.area_uuid = ?", AreaUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.area_uuid = ?", AreaUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			communes.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = communes.country_uuid
				AND u2.province_uuid = communes.province_uuid
				AND u2.area_uuid = communes.area_uuid
				AND u2.sub_area_uuid = communes.sub_area_uuid
				AND u2.commune_uuid = communes.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = communes.country_uuid 
				AND p.province_uuid = communes.province_uuid
				AND p.area_uuid = communes.area_uuid
				AND p.sub_area_uuid = communes.sub_area_uuid
				AND p.commune_uuid = communes.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = communes.country_uuid
				AND ps.province_uuid = communes.province_uuid
				AND ps.area_uuid = communes.area_uuid
				AND ps.sub_area_uuid = communes.sub_area_uuid
				AND ps.commune_uuid = communes.uuid
			) AS total_posforms
		`).
		Offset(offset).
		Limit(limit).
		Order("communes.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		// Preload("Pos").
		// Preload("PosForms").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch communes",
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
		"message":    "Commnues retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// query data DR by Subaarea id
func GetPaginatedCommunesBySubAreaUUID(c *fiber.Ctx) error {
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

	var dataList []models.Commune
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Commune{}).
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.sub_area_uuid = ?", subAreaUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.sub_area_uuid = ?", subAreaUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			communes.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = communes.country_uuid
				AND u2.province_uuid = communes.province_uuid
				AND u2.area_uuid = communes.area_uuid
				AND u2.sub_area_uuid = communes.sub_area_uuid
				AND u2.commune_uuid = communes.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = communes.country_uuid 
				AND p.province_uuid = communes.province_uuid
				AND p.area_uuid = communes.area_uuid
				AND p.sub_area_uuid = communes.sub_area_uuid
				AND p.commune_uuid = communes.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = communes.country_uuid
				AND ps.province_uuid = communes.province_uuid
				AND ps.area_uuid = communes.area_uuid
				AND ps.sub_area_uuid = communes.sub_area_uuid
				AND ps.commune_uuid = communes.uuid
			) AS total_posforms
		`).
		Offset(offset).
		Limit(limit).
		Order("communes.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		// Preload("Pos").
		// Preload("PosForms").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch communes",
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
		"message":    "Commnues retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// query data cyclo by Cyclo id
func GetPaginatedCommunesByCyclo(c *fiber.Ctx) error {
	db := database.DB

	commueUUID := c.Params("commune_uuid")

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

	var dataList []models.Commune
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Commune{}).
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.uuid = ?", commueUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("LEFT JOIN countries ON communes.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON communes.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON communes.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON communes.sub_area_uuid = sub_areas.uuid").
		Where("communes.uuid = ?", commueUUID).
		Where("communes.name ILIKE ? OR countries.name ILIKE ? OR provinces.name ILIKE ? OR areas.name ILIKE ? OR sub_areas.name ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Select(` 
			communes.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = communes.country_uuid
				AND u2.province_uuid = communes.province_uuid
				AND u2.area_uuid = communes.area_uuid
				AND u2.sub_area_uuid = communes.sub_area_uuid
				AND u2.commune_uuid = communes.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = communes.country_uuid 
				AND p.province_uuid = communes.province_uuid
				AND p.area_uuid = communes.area_uuid
				AND p.sub_area_uuid = communes.sub_area_uuid
				AND p.commune_uuid = communes.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps  
				WHERE ps.country_uuid = communes.country_uuid
				AND ps.province_uuid = communes.province_uuid
				AND ps.area_uuid = communes.area_uuid
				AND ps.sub_area_uuid = communes.sub_area_uuid
				AND ps.commune_uuid = communes.uuid
			) AS total_posforms
		`).
		Offset(offset).
		Limit(limit).
		Order("communes.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		// Preload("Pos").
		// Preload("PosForms").
		// Preload("Users").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch communes",
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
		"message":    "Commnues retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllCommunes(c *fiber.Ctx) error {
	db := database.DB
	var data []models.Commune
	db.Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Communes",
		"data":    data,
	})
}

// Get All data by SubArea id
func GetAllCommunesBySubAreaUUID(c *fiber.Ctx) error {
	db := database.DB

	subAreaUUID := c.Params("sub_area_uuid")

	var data []models.Commune
	db.Where("sub_area_uuid = ?", subAreaUUID).Find(&data).Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Communes",
		"data":    data,
	})
}

// query data
func GetCountryCommuneByID(c *fiber.Ctx) error {
	id := c.Params("id")
	db := database.DB
	var communes []models.Commune
	db.Where("country_uuid = ?", id).Find(&communes)

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Communes by id found",
		"data":    communes,
	})
}

// query data
func GetCommuneByID(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var communes []models.Commune
	db.Where("uuid = ?", uuid).Find(&communes)

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "country by id found",
		"data":    communes,
	})
}

// query data
func GetProvinceCommuneByID(c *fiber.Ctx) error {
	id := c.Params("id")
	db := database.DB
	var communes []models.Commune
	db.Where("province_uuid = ?", id).Find(&communes)

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "province by id found",
		"data":    communes,
	})
}

// Get one data
func GetCommune(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var commune models.Commune
	db.Where("uuid = ?", uuid).First(&commune)
	if commune.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Commune name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Commune found",
			"data":    commune,
		},
	)
}

// Create data
func CreateCommune(c *fiber.Ctx) error {
	p := &models.Commune{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Commune created successfully",
			"data":    p,
		},
	)
}

// Update data
func UpdateCommune(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		UUID        string `json:"uuid"`
		Name        string `gorm:"not null" json:"name"`
		SubAreaUUID string `json:"sub_area_uuid" gorm:"type:varchar(255);not null"`
		Signature   string `json:"signature"`
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

	commune := new(models.Commune)

	db.Where("uuid = ?", uuid).First(&commune)
	if commune.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Commune found",
				"data":    nil,
			},
		)
	}

	commune.Name = updateData.Name
	commune.SubAreaUUID = updateData.SubAreaUUID
	commune.Signature = updateData.Signature

	db.Save(&commune)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Commune updated successfully",
			"data":    commune,
		},
	)
}

// Delete data
func DeleteCommune(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var commune models.Commune
	db.Where("uuid = ?", uuid).First(&commune)
	if commune.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Commune name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&commune)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Commune deleted successfully",
			"data":    nil,
		},
	)
}

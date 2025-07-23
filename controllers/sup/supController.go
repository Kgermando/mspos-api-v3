package sup

import (
	"fmt"
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
)

// Paginate
func GetPaginatedSups(c *fiber.Ctx) error {
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

	var dataList []models.User
	var totalRecords int64

	// Count total records matching the search query
	countQuery := db.Model(&models.User{}).
		Where("role = ?", "Supervisor")

	if search != "" {
		countQuery = countQuery.Where(`
		fullname ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	countQuery.Count(&totalRecords)

	// Build the main query
	query := db.Where("role = ?", "Supervisor")

	if search != "" {
		query = query.Where(`
		fullname ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	err = query.
		Select(`
			users.*,
			(
				SELECT COUNT(DISTINCT u2.dr_uuid)
				FROM users u2
				WHERE u2.role = 'DR' AND u2.province_uuid = users.province_uuid
				AND u2.area_uuid = users.area_uuid
			) AS total_dr,
			(
				SELECT COUNT(DISTINCT u2.cyclo_uuid)
				FROM users u2
				WHERE u2.role = 'Cyclo' AND u2.province_uuid = users.province_uuid
				AND u2.area_uuid = users.area_uuid
			) AS total_cyclo,
			 (
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE users.province_uuid = p.province_uuid
				AND users.area_uuid = p.area_uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps 
				WHERE
				users.province_uuid = ps.province_uuid
				AND users.area_uuid = ps.area_uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("users.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch sups",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]any{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "Sups retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by Province ID
func GetPaginatedSupProvince(c *fiber.Ctx) error {
	db := database.DB

	UserUUID := c.Params("province_uuid")

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

	var dataList []models.User
	var totalRecords int64

	// Count total records matching the search query
	countQuery := db.Model(&models.User{}).
		Where("role = ?", "Supervisor").
		Where("users.asm_uuid = ?", UserUUID)

	if search != "" {
		countQuery = countQuery.Where(`
		fullname ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	countQuery.Count(&totalRecords)

	// Build the main query
	query := db.Where("role = ?", "Supervisor").
		Where("users.asm_uuid = ?", UserUUID)

	if search != "" {
		query = query.Where(`
		fullname ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	err = query.
		Select(`
			users.*,  
			(
				SELECT COUNT(DISTINCT u2.dr_uuid)
				FROM users u2
				WHERE u2.role = 'DR' AND u2.province_uuid = users.province_uuid
				AND u2.area_uuid = users.area_uuid
			) AS total_dr,
			(
				SELECT COUNT(DISTINCT u2.cyclo_uuid)
				FROM users u2
				WHERE u2.role = 'Cyclo' AND u2.province_uuid = users.province_uuid
				AND u2.area_uuid = users.area_uuid
			) AS total_cyclo,
			 (
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE users.province_uuid = p.province_uuid
				AND users.area_uuid = p.area_uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps 
				WHERE
				users.province_uuid = ps.province_uuid
				AND users.area_uuid = ps.area_uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("users.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch sups",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]any{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "Sups retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by Area ID
func GetPaginatedSupArea(c *fiber.Ctx) error {
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

	// Parse search query
	search := c.Query("search", "")
	fmt.Println("Search query:", search)

	var dataList []models.User
	var totalRecords int64

	// Count total records matching the search query
	countQuery := db.Model(&models.User{}).
		Where("role = ?", "Supervisor").
		Where("users.sup_uuid = ?", UserUUID)

	if search != "" {
		countQuery = countQuery.Where(`
		fullname ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	countQuery.Count(&totalRecords)

	// Build the main query
	query := db.Where("role = ?", "Supervisor").
		Where("users.sup_uuid = ?", UserUUID)

	if search != "" {
		query = query.Where(`
		fullname ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	err = query.
		Select(`
			users.*,  
			(
				SELECT COUNT(DISTINCT u2.dr_uuid)
				FROM users u2
				WHERE u2.role = 'DR' AND u2.province_uuid = users.province_uuid
				AND u2.area_uuid = users.area_uuid
			) AS total_dr,
			(
				SELECT COUNT(DISTINCT u2.cyclo_uuid)
				FROM users u2
				WHERE u2.role = 'Cyclo' AND u2.province_uuid = users.province_uuid
				AND u2.area_uuid = users.area_uuid
			) AS total_cyclo,
			 (
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p
				INNER JOIN users u2 ON p.user_uuid = u2.uuid
				WHERE u2.province_uuid = p.province_uuid
				AND u2.area_uuid = p.area_uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps 
				WHERE
				users.province_uuid = ps.province_uuid
				AND users.area_uuid = ps.area_uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("users.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch sups",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Prepare pagination metadata
	pagination := map[string]any{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "Sups retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}


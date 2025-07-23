package cyclo

import (
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
)

// Paginate
func GetPaginatedCyclo(c *fiber.Ctx) error {
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
		Where("role = ?", "Cyclo")

	if search != "" {
		countQuery = countQuery.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	countQuery.Count(&totalRecords)

	// Build the main query
	query := db.Where("role = ?", "Cyclo")

	if search != "" {
		query = query.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	err = query.
		Select(`
			users.*,
			  (
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE users.province_uuid = p.province_uuid
				AND users.area_uuid = p.area_uuid
				AND users.sub_area_uuid = p.sub_area_uuid
				AND users.commune_uuid = p.commune_uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps 
				WHERE
				users.province_uuid = ps.province_uuid
				AND users.area_uuid = ps.area_uuid
				AND users.sub_area_uuid = ps.sub_area_uuid
				AND users.commune_uuid = ps.commune_uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch cyclos",
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
		"message":    "Cyclo retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Province by ID
func GetPaginatedCycloProvinceByID(c *fiber.Ctx) error {
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

	var dataList []models.User
	var totalRecords int64

	// Count total records matching the search query
	countQuery := db.Model(&models.User{}).
		Where("role = ?", "Cyclo").
		Where("users.asm_uuid = ?", UserUUID)

	if search != "" {
		countQuery = countQuery.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	countQuery.Count(&totalRecords)

	// Build the main query
	query := db.Where("role = ?", "Cyclo").
		Where("users.asm_uuid = ?", UserUUID)

	if search != "" {
		query = query.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	err = query.
		Select(`
			users.*,
			  (
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE users.province_uuid = p.province_uuid
				AND users.area_uuid = p.area_uuid
				AND users.sub_area_uuid = p.sub_area_uuid
				AND users.commune_uuid = p.commune_uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps 
				WHERE
				users.province_uuid = ps.province_uuid
				AND users.area_uuid = ps.area_uuid
				AND users.sub_area_uuid = ps.sub_area_uuid
				AND users.commune_uuid = ps.commune_uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch cyclos",
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
		"message":    "Cyclo retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Area by ID
func GetPaginatedCycloByAreaUUID(c *fiber.Ctx) error {
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

	var dataList []models.User
	var totalRecords int64

	// Count total records matching the search query
	countQuery := db.Model(&models.User{}).
		Where("role = ?", "Cyclo").
		Where("users.sup_uuid = ?", UserUUID)

	if search != "" {
		countQuery = countQuery.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	countQuery.Count(&totalRecords)

	// Build the main query
	query := db.Where("role = ?", "Cyclo").
		Where("users.sup_uuid = ?", UserUUID)

	if search != "" {
		query = query.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	err = query.
		Select(`
			users.*,
			  (
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE users.province_uuid = p.province_uuid
				AND users.area_uuid = p.area_uuid
				AND users.sub_area_uuid = p.sub_area_uuid
				AND users.commune_uuid = p.commune_uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps 
				WHERE
				users.province_uuid = ps.province_uuid
				AND users.area_uuid = ps.area_uuid
				AND users.sub_area_uuid = ps.sub_area_uuid
				AND users.commune_uuid = ps.commune_uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch cyclos",
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
		"message":    "Cyclo retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by SubArea ID
func GetPaginatedSubAreaByID(c *fiber.Ctx) error {
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

	var dataList []models.User
	var totalRecords int64

	// Count total records matching the search query
	countQuery := db.Model(&models.User{}).
		Where("role = ?", "Cyclo").
		Where("users.dr_uuid = ?", UserUUID)

	if search != "" {
		countQuery = countQuery.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	countQuery.Count(&totalRecords)

	// Build the main query
	query := db.Where("role = ?", "Cyclo").
		Where("users.dr_uuid = ?", UserUUID)

	if search != "" {
		query = query.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	err = query.
		Select(`
			users.*,
			  (
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE users.province_uuid = p.province_uuid
				AND users.area_uuid = p.area_uuid
				AND users.sub_area_uuid = p.sub_area_uuid
				AND users.commune_uuid = p.commune_uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps 
				WHERE
				users.province_uuid = ps.province_uuid
				AND users.area_uuid = ps.area_uuid
				AND users.sub_area_uuid = ps.sub_area_uuid
				AND users.commune_uuid = ps.commune_uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch cyclos",
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
		"message":    "Cyclo retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate by Commune ID
func GetPaginatedCycloCommune(c *fiber.Ctx) error {
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

	var dataList []models.User
	var totalRecords int64

	// Count total records matching the search query
	countQuery := db.Model(&models.User{}).
		Where("role = ?", "Cyclo").
		Where("users.cyclo_uuid = ?", UserUUID)

	if search != "" {
		countQuery = countQuery.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	countQuery.Count(&totalRecords)

	// Build the main query
	query := db.Where("role = ?", "Cyclo").
		Where("users.cyclo_uuid = ?", UserUUID)

	if search != "" {
		query = query.Where(`
		title ILIKE ? OR 
		asm ILIKE ? OR 
		sup ILIKE ? OR 
		dr ILIKE ? OR 
		cyclo ILIKE ? OR EXISTS 
		(SELECT 1 FROM provinces WHERE users.province_uuid = provinces.uuid AND provinces.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM areas WHERE users.area_uuid = areas.uuid AND areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM sub_areas WHERE users.sub_area_uuid = sub_areas.uuid AND sub_areas.name ILIKE ?) OR EXISTS
		(SELECT 1 FROM communes WHERE users.commune_uuid = communes.uuid AND communes.name ILIKE ?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	err = query.
		Select(`
			users.*,
			  (
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE users.province_uuid = p.province_uuid
				AND users.area_uuid = p.area_uuid
				AND users.sub_area_uuid = p.sub_area_uuid
				AND users.commune_uuid = p.commune_uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps 
				WHERE
				users.province_uuid = ps.province_uuid
				AND users.area_uuid = ps.area_uuid
				AND users.sub_area_uuid = ps.sub_area_uuid
				AND users.commune_uuid = ps.commune_uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch cyclos",
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
		"message":    "Cyclo retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

package posform

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

// Paginate ALL data
func GetPaginatedPosForm(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch pos_forms",
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
		"message":    "POSFORM retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Query data province by UUID
func GetPaginatedPosFormCountryUUID(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	CountryUUID := c.Params("country_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.country_uuid = ?", CountryUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch posforms",
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
		"message":    "posforms retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Query data province by UUID
func GetPaginatedPosFormProvine(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	ProvinceUUID := c.Params("province_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.province_uuid = ?", ProvinceUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch posforms",
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
		"message":    "posforms retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Query data area by UUID
func GetPaginatedPosFormArea(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	AreaUUID := c.Params("area_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.area_uuid = ?", AreaUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch posforms",
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
		"message":    "posform retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Query data subarea by UUID
func GetPaginatedPosFormSubArea(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	SubAreaUUID := c.Params("sub_area_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.sub_area_uuid = ?", SubAreaUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch posforms",
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
		"message":    "posform retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Query data commune by UserUUID
func GetPaginatedPosFormCommune(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	UserUUID := c.Params("user_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.user_uuid = ?", UserUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch posforms",
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
		"message":    "posform retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Query data commune by UUID filter
func GetPaginatedPosFormCommuneFilter(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	CommuneUUID := c.Params("commune_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.commune_uuid = ?", CommuneUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch posforms",
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
		"message":    "posform retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Query data pos by UUID
func GetPaginatedPosFormByPOS(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	posUUID := c.Params("pos_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.pos_uuid = ?", posUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch posforms",
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
		"message":    "posforms retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

func GetPaginatedPosFormByUserUUID(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	// Provide default values if start_date or end_date are empty
	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z" // Default start date
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z" // Default end date
	}

	userUUID := c.Params("user_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosForm
	var totalRecords int64

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.user_uuid = ?", userUUID).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Apply filters
	query = applyAdvancedFilters(query, c)

	// Count total records
	query.Count(&totalRecords)

	// Fetch data with pagination
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
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch posforms",
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
		"message":    "posforms retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllPosforms(c *fiber.Ctx) error {
	db := database.DB
	var data []models.PosForm
	db.Preload("Pos").Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All PosForms",
		"data":    data,
	})
}

// Get one data
func GetPosForm(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var posform models.PosForm
	result := db.Where("uuid = ?", uuid).
		Preload("Pos").
		First(&posform)
	if result.Error != nil {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No posform name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "posform found",
			"data":    posform,
		},
	)
}

func CreatePosform(c *fiber.Ctx) error {
	p := &models.PosForm{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()

	// p.Sync = true
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
func UpdatePosform(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		Price   int    `json:"price"`
		Comment string `json:"comment"`
		PosUUID string `json:"pos_uuid"`

		Latitude  float64 `json:"latitude"`  // Latitude of the user
		Longitude float64 `json:"longitude"` // Longitude of the user
		Signature string  `json:"signature"`

		CountryUUID  string `json:"country_uuid"`
		ProvinceUUID string `json:"province_uuid"`
		AreaUUID     string `json:"area_uuid"`
		SubAreaUUID  string `json:"sub_area_uuid"`
		CommuneUUID  string `json:"commune_uuid"`

		AsmUUID   string `json:"asm_uuid"`
		Asm       string `json:"asm"`
		SupUUID   string `json:"sup_uuid"`
		Sup       string `json:"sup"`
		DrUUID    string `json:"dr_uuid"`
		Dr        string `json:"dr"`
		CycloUUID string `json:"cyclo_uuid"`
		Cyclo     string `json:"cyclo"`
		UserUUID  string `json:"user_uuid"`
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

	posform := new(models.PosForm)

	db.Where("uuid = ?", uuid).First(&posform)

	posform.Price = updateData.Price
	posform.Comment = updateData.Comment
	posform.PosUUID = updateData.PosUUID

	posform.Latitude = updateData.Latitude
	posform.Longitude = updateData.Longitude
	posform.Signature = updateData.Signature

	posform.CountryUUID = updateData.CountryUUID
	posform.ProvinceUUID = updateData.ProvinceUUID
	posform.AreaUUID = updateData.AreaUUID
	posform.SubAreaUUID = updateData.SubAreaUUID
	posform.CommuneUUID = updateData.CommuneUUID

	posform.AsmUUID = updateData.AsmUUID
	posform.Asm = updateData.Asm
	posform.SupUUID = updateData.SupUUID
	posform.Sup = updateData.Sup
	posform.DrUUID = updateData.DrUUID
	posform.Dr = updateData.Dr
	posform.CycloUUID = updateData.CycloUUID
	posform.Cyclo = updateData.Cyclo
	posform.UserUUID = updateData.UserUUID
	// posform.Sync = true

	db.Save(&posform)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "posform updated success",
			"data":    posform,
		},
	)

}

// Delete data
func DeletePosform(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var posform models.PosForm
	db.Where("uuid = ?", uuid).First(&posform)
	if posform.UUID == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No posform name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&posform)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "posform deleted success",
			"data":    nil,
		},
	)
}

// GetUniqueFilterValues retourne les valeurs uniques pour les filtres
func GetUniqueFilterValues(c *fiber.Ctx) error {
	db := database.DB

	type FilterValues struct {
		Countries   []string `json:"countries"`
		Provinces   []string `json:"provinces"`
		Areas       []string `json:"areas"`
		SubAreas    []string `json:"sub_areas"`
		Communes    []string `json:"communes"`
		Prices      []int    `json:"prices"`
		PosTypes    []string `json:"pos_types"`
		Asms        []string `json:"asms"`
		Supervisors []string `json:"supervisors"`
		Drs         []string `json:"drs"`
		Cyclos      []string `json:"cyclos"`
	}

	var filterValues FilterValues

	// Récupérer les pays uniques
	var countries []string
	db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Where("countries.name IS NOT NULL AND countries.name != ''").
		Pluck("DISTINCT countries.name", &countries)
	filterValues.Countries = countries

	// Récupérer les provinces uniques
	var provinces []string
	db.Model(&models.PosForm{}).
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Where("provinces.name IS NOT NULL AND provinces.name != ''").
		Pluck("DISTINCT provinces.name", &provinces)
	filterValues.Provinces = provinces

	// Récupérer les areas uniques
	var areas []string
	db.Model(&models.PosForm{}).
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Where("areas.name IS NOT NULL AND areas.name != ''").
		Pluck("DISTINCT areas.name", &areas)
	filterValues.Areas = areas

	// Récupérer les sub_areas uniques
	var subAreas []string
	db.Model(&models.PosForm{}).
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Where("sub_areas.name IS NOT NULL AND sub_areas.name != ''").
		Pluck("DISTINCT sub_areas.name", &subAreas)
	filterValues.SubAreas = subAreas

	// Récupérer les communes uniques
	var communes []string
	db.Model(&models.PosForm{}).
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Where("communes.name IS NOT NULL AND communes.name != ''").
		Pluck("DISTINCT communes.name", &communes)
	filterValues.Communes = communes

	// Récupérer les prix uniques
	var prices []int
	db.Model(&models.PosForm{}).
		Where("pos_forms.price > 0").
		Pluck("DISTINCT pos_forms.price", &prices)
	filterValues.Prices = prices

	// Récupérer les types de POS uniques
	var posTypes []string
	db.Model(&models.PosForm{}).
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos.type IS NOT NULL AND pos.type != ''").
		Pluck("DISTINCT pos.type", &posTypes)
	filterValues.PosTypes = posTypes

	// Récupérer les ASMs uniques
	var asms []string
	db.Model(&models.PosForm{}).
		Where("pos_forms.asm IS NOT NULL AND pos_forms.asm != ''").
		Pluck("DISTINCT pos_forms.asm", &asms)
	filterValues.Asms = asms

	// Récupérer les Supervisors uniques
	var supervisors []string
	db.Model(&models.PosForm{}).
		Where("pos_forms.sup IS NOT NULL AND pos_forms.sup != ''").
		Pluck("DISTINCT pos_forms.sup", &supervisors)
	filterValues.Supervisors = supervisors

	// Récupérer les DRs uniques
	var drs []string
	db.Model(&models.PosForm{}).
		Where("pos_forms.dr IS NOT NULL AND pos_forms.dr != ''").
		Pluck("DISTINCT pos_forms.dr", &drs)
	filterValues.Drs = drs

	// Récupérer les Cyclos uniques
	var cyclos []string
	db.Model(&models.PosForm{}).
		Where("pos_forms.cyclo IS NOT NULL AND pos_forms.cyclo != ''").
		Pluck("DISTINCT pos_forms.cyclo", &cyclos)
	filterValues.Cyclos = cyclos

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Filter values retrieved successfully",
		"data":    filterValues,
	})
}

// GetFilteredHierarchy retourne les valeurs filtrées de la hiérarchie commerciale basées sur la recherche
func GetFilteredHierarchy(c *fiber.Ctx) error {
	db := database.DB

	// Paramètres de recherche
	asmSearch := c.Query("asmSearch", "")
	supervisorSearch := c.Query("supervisorSearch", "")
	drSearch := c.Query("drSearch", "")
	cycloSearch := c.Query("cycloSearch", "")

	type HierarchyValues struct {
		FilteredAsms        []string `json:"filtered_asms"`
		FilteredSupervisors []string `json:"filtered_supervisors"`
		FilteredDrs         []string `json:"filtered_drs"`
		FilteredCyclos      []string `json:"filtered_cyclos"`
	}

	var hierarchyValues HierarchyValues

	// Filtrer les ASMs
	asmQuery := db.Model(&models.PosForm{}).
		Where("pos_forms.asm IS NOT NULL AND pos_forms.asm != ''")
	if asmSearch != "" {
		asmQuery = asmQuery.Where("pos_forms.asm ILIKE ?", "%"+asmSearch+"%")
	}
	var asms []string
	asmQuery.Pluck("DISTINCT pos_forms.asm", &asms)
	hierarchyValues.FilteredAsms = asms

	// Filtrer les Supervisors
	supQuery := db.Model(&models.PosForm{}).
		Where("pos_forms.sup IS NOT NULL AND pos_forms.sup != ''")
	if supervisorSearch != "" {
		supQuery = supQuery.Where("pos_forms.sup ILIKE ?", "%"+supervisorSearch+"%")
	}
	var supervisors []string
	supQuery.Pluck("DISTINCT pos_forms.sup", &supervisors)
	hierarchyValues.FilteredSupervisors = supervisors

	// Filtrer les DRs
	drQuery := db.Model(&models.PosForm{}).
		Where("pos_forms.dr IS NOT NULL AND pos_forms.dr != ''")
	if drSearch != "" {
		drQuery = drQuery.Where("pos_forms.dr ILIKE ?", "%"+drSearch+"%")
	}
	var drs []string
	drQuery.Pluck("DISTINCT pos_forms.dr", &drs)
	hierarchyValues.FilteredDrs = drs

	// Filtrer les Cyclos
	cycloQuery := db.Model(&models.PosForm{}).
		Where("pos_forms.cyclo IS NOT NULL AND pos_forms.cyclo != ''")
	if cycloSearch != "" {
		cycloQuery = cycloQuery.Where("pos_forms.cyclo ILIKE ?", "%"+cycloSearch+"%")
	}
	var cyclos []string
	cycloQuery.Pluck("DISTINCT pos_forms.cyclo", &cyclos)
	hierarchyValues.FilteredCyclos = cyclos

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Filtered hierarchy values retrieved successfully",
		"data":    hierarchyValues,
	})
}

// GetFilterStatistics retourne les statistiques des filtres appliqués
func GetFilterStatistics(c *fiber.Ctx) error {
	db := database.DB

	// Construire la requête de base avec les mêmes JOINs
	baseQuery := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid")

	// Appliquer les filtres
	filteredQuery := applyAdvancedFilters(baseQuery, c)

	type FilterStatistics struct {
		TotalRecords      int64   `json:"total_records"`
		FilteredRecords   int64   `json:"filtered_records"`
		CompleteReports   int64   `json:"complete_reports"`
		IncompleteReports int64   `json:"incomplete_reports"`
		TotalBrands       int64   `json:"total_brands"`
		AveragePrice      float64 `json:"average_price"`
	}

	var stats FilterStatistics

	// Total records sans filtre
	db.Model(&models.PosForm{}).Count(&stats.TotalRecords)

	// Records avec filtres
	filteredQuery.Count(&stats.FilteredRecords)

	// Rapports complets
	filteredQuery.Where("pos_forms.pos_uuid IS NOT NULL AND pos_forms.pos_uuid != ''").Count(&stats.CompleteReports)

	// Rapports incomplets
	filteredQuery.Where("(pos_forms.pos_uuid IS NULL OR pos_forms.pos_uuid = '')").Count(&stats.IncompleteReports)

	// Total des marques dans les rapports filtrés
	var brandSubQuery = db.Table("pos_form_items").
		Select("COUNT(*)").
		Where("pos_form_items.pos_form_uuid IN (?)",
			filteredQuery.Select("pos_forms.uuid"))
	db.Raw("SELECT COALESCE(SUM(brand_count), 0) FROM (?) as subquery", brandSubQuery).Scan(&stats.TotalBrands)

	// Prix moyen
	filteredQuery.Select("COALESCE(AVG(pos_forms.price), 0)").Scan(&stats.AveragePrice)

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Filter statistics retrieved successfully",
		"data":    stats,
	})
}

// TestAdvancedFilters endpoint pour tester tous les filtres
func TestAdvancedFilters(c *fiber.Ctx) error {
	db := database.DB

	// Construction de la requête de test
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid")

	// Appliquer tous les filtres
	query = applyAdvancedFilters(query, c)

	// Compter les résultats
	var totalRecords int64
	query.Count(&totalRecords)

	// Récupérer un échantillon de données
	var sampleData []models.PosForm
	query.Select("pos_forms.*").
		Limit(10).
		Order("pos_forms.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("Pos").
		Preload("PosFormItems").
		Find(&sampleData)

	// Préparer les détails des filtres appliqués
	appliedFilters := map[string]string{
		"search":           c.Query("search", ""),
		"country":          c.Query("country", ""),
		"province":         c.Query("province", ""),
		"area":             c.Query("area", ""),
		"subarea":          c.Query("subarea", ""),
		"commune":          c.Query("commune", ""),
		"price":            c.Query("price", ""),
		"status":           c.Query("status", ""),
		"brandCount":       c.Query("brandCount", ""),
		"posType":          c.Query("posType", ""),
		"posSearch":        c.Query("posSearch", ""),
		"asm":              c.Query("asm", ""),
		"asmSearch":        c.Query("asmSearch", ""),
		"supervisor":       c.Query("supervisor", ""),
		"supervisorSearch": c.Query("supervisorSearch", ""),
		"dr":               c.Query("dr", ""),
		"drSearch":         c.Query("drSearch", ""),
		"cyclo":            c.Query("cyclo", ""),
		"cycloSearch":      c.Query("cycloSearch", ""),
		"quickDate":        c.Query("quickDate", ""),
	}

	// Compter les filtres actifs
	activeFiltersCount := 0
	for _, value := range appliedFilters {
		if value != "" {
			activeFiltersCount++
		}
	}

	return c.JSON(fiber.Map{
		"status":               "success",
		"message":              "Advanced filters test completed",
		"total_records":        totalRecords,
		"active_filters":       appliedFilters,
		"active_filters_count": activeFiltersCount,
		"sample_data":          sampleData,
		"sample_count":         len(sampleData),
	})
}

// GetPaginatedWithAdvancedFilters - Version spécialisée avec tous les filtres
func GetPaginatedWithAdvancedFilters(c *fiber.Ctx) error {
	db := database.DB

	// Gestion des dates
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	if start_date == "" {
		start_date = "1970-01-01T00:00:00Z"
	}
	if end_date == "" {
		end_date = "2100-01-01T00:00:00Z"
	}

	// Gestion de la pagination
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

	// Construction de la requête avec tous les JOINs nécessaires
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date)

	// Appliquer TOUS les filtres avancés
	query = applyAdvancedFilters(query, c)

	// Compter le total des enregistrements
	query.Count(&totalRecords)

	// Récupérer les données avec pagination
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
		Preload("PosFormItems.Brand").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Erreur lors de la récupération des données",
			"error":   err.Error(),
		})
	}

	// Calculer le nombre total de pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	// Statistiques additionnelles
	var completeReports, incompleteReports int64
	db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Scopes(func(d *gorm.DB) *gorm.DB { return applyAdvancedFilters(d, c) }).
		Where("pos_forms.pos_uuid IS NOT NULL AND pos_forms.pos_uuid != ''").
		Count(&completeReports)

	db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Scopes(func(d *gorm.DB) *gorm.DB { return applyAdvancedFilters(d, c) }).
		Where("(pos_forms.pos_uuid IS NULL OR pos_forms.pos_uuid = '')").
		Count(&incompleteReports)

	// Métadonnées de pagination
	pagination := map[string]interface{}{
		"total_records":      totalRecords,
		"total_pages":        totalPages,
		"current_page":       page,
		"page_size":          limit,
		"complete_reports":   completeReports,
		"incomplete_reports": incompleteReports,
		"has_filters":        hasActiveFilters(c),
		"active_filters":     getActiveFiltersCount(c),
	}

	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "PosForm avec filtres avancés récupérés avec succès",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Helper functions pour les statistiques de filtres
func hasActiveFilters(c *fiber.Ctx) bool {
	filters := []string{
		"search", "country", "province", "area", "subarea", "commune",
		"price", "status", "brandCount", "posType", "posSearch",
		"asm", "asmSearch", "supervisor", "supervisorSearch",
		"dr", "drSearch", "cyclo", "cycloSearch", "quickDate",
	}

	for _, filter := range filters {
		if c.Query(filter, "") != "" {
			return true
		}
	}
	return false
}

func getActiveFiltersCount(c *fiber.Ctx) int {
	filters := []string{
		"search", "country", "province", "area", "subarea", "commune",
		"price", "status", "brandCount", "posType", "posSearch",
		"asm", "asmSearch", "supervisor", "supervisorSearch",
		"dr", "drSearch", "cyclo", "cycloSearch", "quickDate",
	}

	count := 0
	for _, filter := range filters {
		if c.Query(filter, "") != "" {
			count++
		}
	}
	return count
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

	// Filtres commerciaux
	price := c.Query("price", "")
	status := c.Query("status", "")
	brandCount := c.Query("brandCount", "")
	posType := c.Query("posType", "")
	posSearch := c.Query("posSearch", "")

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

	// 🔍 Recherche générale dans les commentaires
	if search != "" {
		query = query.Where("pos_forms.comment ILIKE ?", "%"+search+"%")
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

	// 💰 Filtre prix exact
	if price != "" {
		if priceInt, err := strconv.Atoi(price); err == nil {
			query = query.Where("pos_forms.price = ?", priceInt)
		}
	}

	// 📊 Filtre statut du rapport
	switch status {
	case "complete":
		query = query.Where("pos_forms.pos_uuid IS NOT NULL AND pos_forms.pos_uuid != ''")
	case "incomplete":
		query = query.Where("(pos_forms.pos_uuid IS NULL OR pos_forms.pos_uuid = '')")
	}

	// 🏪 Filtres point de vente
	if posType != "" {
		query = query.Where("pos.type = ?", posType)
	}
	if posSearch != "" {
		query = query.Where("(pos.name ILIKE ? OR pos.shop ILIKE ?)", "%"+posSearch+"%", "%"+posSearch+"%")
	}

	// 👔 Filtres hiérarchie commerciale avec recherche intégrée
	// ASM - support recherche intégrée
	if asm != "" {
		query = query.Where("pos_forms.asm = ?", asm)
	}
	if asmSearch != "" {
		query = query.Where("pos_forms.asm ILIKE ?", "%"+asmSearch+"%")
	}

	// Supervisor - support recherche intégrée
	if supervisor != "" {
		query = query.Where("pos_forms.sup = ?", supervisor)
	}
	if supervisorSearch != "" {
		query = query.Where("pos_forms.sup ILIKE ?", "%"+supervisorSearch+"%")
	}

	// DR - support recherche intégrée
	if dr != "" {
		query = query.Where("pos_forms.dr = ?", dr)
	}
	if drSearch != "" {
		query = query.Where("pos_forms.dr ILIKE ?", "%"+drSearch+"%")
	}

	// Cyclo - support recherche intégrée
	if cyclo != "" {
		query = query.Where("pos_forms.cyclo = ?", cyclo)
	}
	if cycloSearch != "" {
		query = query.Where("pos_forms.cyclo ILIKE ?", "%"+cycloSearch+"%")
	}

	// 🏷️ Filtre nombre de marques avec sous-requête sur pos_form_items
	if brandCount != "" {
		switch brandCount {
		case "0":
			// Aucune marque
			query = query.Where("NOT EXISTS (SELECT 1 FROM pos_form_items WHERE pos_form_items.pos_form_uuid = pos_forms.uuid)")
		case "5":
			// Exactement 1 marque
			query = query.Where("(SELECT COUNT(*) FROM pos_form_items WHERE pos_form_items.pos_form_uuid = pos_forms.uuid) = 5")
		case "5-10":
			// Entre 2 et 5 marques
			query = query.Where("(SELECT COUNT(*) FROM pos_form_items WHERE pos_form_items.pos_form_uuid = pos_forms.uuid) BETWEEN 5 AND 10")
		case "11+":
			// 6 marques ou plus
			query = query.Where("(SELECT COUNT(*) FROM pos_form_items WHERE pos_form_items.pos_form_uuid = pos_forms.uuid) >= 11")
		}
	}

	// 📅 Filtres rapides par date
	if quickDate != "" {
		switch quickDate {
		case "today":
			query = query.Where("DATE(pos_forms.created_at) = CURRENT_DATE")
		case "yesterday":
			query = query.Where("DATE(pos_forms.created_at) = CURRENT_DATE - INTERVAL '1 day'")
		case "last7days":
			query = query.Where("pos_forms.created_at >= CURRENT_DATE - INTERVAL '7 days'")
		case "last30days":
			query = query.Where("pos_forms.created_at >= CURRENT_DATE - INTERVAL '30 days'")
		}
	}

	return query
}

// applyAdvancedFiltersForExcel applies advanced filters including date range for Excel reports
func applyAdvancedFiltersForExcel(query *gorm.DB, c *fiber.Ctx) *gorm.DB {
	// Apply all standard filters first
	query = applyAdvancedFilters(query, c)

	// Additional filters specific to Excel reports - support multiple date parameter formats
	startDate := c.Query("startDate", "")
	endDate := c.Query("endDate", "")

	// Support legacy parameter names
	if startDate == "" {
		startDate = c.Query("start_date", "")
	}
	if endDate == "" {
		endDate = c.Query("end_date", "")
	}

	// 📅 Filtres par plage de dates personnalisée (uniquement pour Excel)
	if startDate != "" && endDate != "" {
		// Validation et parsing des dates avec support de multiples formats
		startTime, err := parseFlexibleDate(startDate)
		if err == nil {
			endTime, err := parseFlexibleDate(endDate)
			if err == nil {
				// Ajouter 23:59:59 à la date de fin pour inclure toute la journée
				endTime = endTime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
				query = query.Where("pos_forms.created_at >= ? AND pos_forms.created_at <= ?", startTime, endTime)
			}
		}
	} else if startDate != "" {
		// Filtre à partir d'une date de début seulement
		startTime, err := parseFlexibleDate(startDate)
		if err == nil {
			query = query.Where("pos_forms.created_at >= ?", startTime)
		}
	} else if endDate != "" {
		// Filtre jusqu'à une date de fin seulement
		endTime, err := parseFlexibleDate(endDate)
		if err == nil {
			// Ajouter 23:59:59 à la date de fin pour inclure toute la journée
			endTime = endTime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
			query = query.Where("pos_forms.created_at <= ?", endTime)
		}
	}

	return query
}

// parseFlexibleDate parses dates in multiple formats
func parseFlexibleDate(dateStr string) (time.Time, error) {
	// List of supported date formats
	formats := []string{
		"2006-01-02",                // ISO format
		"2006-01-02T15:04:05Z",      // ISO with time
		"2006-01-02T15:04:05Z07:00", // ISO with timezone
		"02/01/2006",                // DD/MM/YYYY
		"01/02/2006",                // MM/DD/YYYY
		"2006/01/02",                // YYYY/MM/DD
		"02-01-2006",                // DD-MM-YYYY
		"01-02-2006",                // MM-DD-YYYY
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", dateStr)
}

// GeneratePosFormExcelReport generates an Excel report for PosForm data
func GeneratePosFormExcelReport(c *fiber.Ctx) error {
	db := database.DB

	// Parse query parameters for filtering
	var dataList []models.PosForm
	var totalRecords int64

	// Get date parameters for display in report
	startDate := c.Query("startDate", "")
	endDate := c.Query("endDate", "")

	// Support legacy parameter names
	if startDate == "" {
		startDate = c.Query("start_date", "")
	}
	if endDate == "" {
		endDate = c.Query("end_date", "")
	}

	// Build query with joins for better filtering
	query := db.Model(&models.PosForm{}).
		Joins("LEFT JOIN countries ON pos_forms.country_uuid = countries.uuid").
		Joins("LEFT JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Joins("LEFT JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Joins("LEFT JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Joins("LEFT JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Joins("LEFT JOIN pos ON pos_forms.pos_uuid = pos.uuid").
		Joins("LEFT JOIN users ON pos_forms.user_uuid = users.uuid")

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
		Select("pos_forms.*").
		Order("pos_forms.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("Pos").
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Échec de la récupération des données PosForm pour le rapport Excel",
			"error":   err.Error(),
		})
	}

	// Create Excel file
	reportTitle := "Rapport des Formulaires POS"
	if startDate != "" && endDate != "" {
		reportTitle = fmt.Sprintf("Rapport des Formulaires POS (%s - %s)", startDate, endDate)
	} else if startDate != "" {
		reportTitle = fmt.Sprintf("Rapport des Formulaires POS (depuis %s)", startDate)
	} else if endDate != "" {
		reportTitle = fmt.Sprintf("Rapport des Formulaires POS (jusqu'au %s)", endDate)
	}

	config := utils.ExcelReportConfig{
		Title:       reportTitle,
		CompanyName: "MSPOS System",
		ReportDate:  time.Now(),
		Author:      "Système de Rapport Automatique",
	}

	f := utils.CreateExcelFile(config)
	sheetName := "Rapport PosForm"

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
		"Total des Formulaires":  totalRecords,
		"Formulaires Complets":   countCompleteForms(dataList),
		"Formulaires Incomplets": countIncompleteForms(dataList),
		"Total Provinces":        countUniqueProvincesForm(dataList),
		"Total Aires":            countUniqueAreasForm(dataList),
		"Total Sous-Aires":       countUniqueSubAreasForm(dataList),
		"Prix Total":             calculateTotalPrice(dataList),
		"Date de génération":     time.Now().Format("02/01/2006 15:04:05"),
	}

	// Add date filter information if filters are applied
	if startDate != "" && endDate != "" {
		summaryData["Période (Du - Au)"] = fmt.Sprintf("%s - %s", startDate, endDate)
	} else if startDate != "" {
		summaryData["Période (Depuis)"] = startDate
	} else if endDate != "" {
		summaryData["Période (Jusqu'au)"] = endDate
	} else {
		summaryData["Période"] = "Toutes les données"
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
		"UUID", "Prix", "Commentaire", "Latitude", "Longitude", "Signature",
		"Pays", "Province", "Aire", "Sous-Aire", "Commune", "Utilisateur",
		"POS Nom", "POS Shop", "ASM", "Superviseur", "DR", "Cyclo",
		"Nombre d'Articles", "Statut", "Date Création", "Date Modification",
	}

	// Start data table after summary (row 15)
	dataStartRow := 15

	// Add main data table title
	f.SetCellValue(sheetName, fmt.Sprintf("A%d", dataStartRow), "DONNÉES DÉTAILLÉES DES FORMULAIRES POS")
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
	for i, form := range dataList {
		row := dataStartRow + 1 + i

		// Convert status to readable format
		statusText := "Incomplet"
		if form.PosUUID != "" {
			statusText = "Complet"
		}

		// Get related data safely
		countryName := ""
		if form.Country.Name != "" {
			countryName = form.Country.Name
		}

		provinceName := ""
		if form.Province.Name != "" {
			provinceName = form.Province.Name
		}

		areaName := ""
		if form.Area.Name != "" {
			areaName = form.Area.Name
		}

		subAreaName := ""
		if form.SubArea.Name != "" {
			subAreaName = form.SubArea.Name
		}

		communeName := ""
		if form.Commune.Name != "" {
			communeName = form.Commune.Name
		}

		userName := ""
		if form.User.Fullname != "" {
			userName = form.User.Fullname
		}

		posName := ""
		posShop := ""
		if form.Pos.Name != "" {
			posName = form.Pos.Name
			posShop = form.Pos.Shop
		}

		// Count form items
		itemCount := len(form.PosFormItems)

		// Data array
		rowData := []interface{}{
			form.UUID,
			form.Price,
			form.Comment,
			form.Latitude,
			form.Longitude,
			form.Signature,
			countryName,
			provinceName,
			areaName,
			subAreaName,
			communeName,
			userName,
			posName,
			posShop,
			form.Asm,
			form.Sup,
			form.Dr,
			form.Cyclo,
			itemCount,
			statusText,
			form.CreatedAt.Format("02/01/2006 15:04:05"),
			form.UpdatedAt.Format("02/01/2006 15:04:05"),
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
			if j == 1 { // Prix column
				style = styles["number"]
			} else if j == 19 { // Status column
				if form.PosUUID != "" {
					style = styles["success"]
				} else {
					style = styles["warning"]
				}
			} else if j == 20 || j == 21 { // Date columns
				style = styles["date"]
			} else if j == 18 { // Nombre d'articles
				style = styles["number"]
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
	filename := fmt.Sprintf("rapport_posform_%s.xlsx", time.Now().Format("2006-01-02_15-04-05"))

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
func countCompleteForms(formList []models.PosForm) int {
	count := 0
	for _, form := range formList {
		if form.PosUUID != "" {
			count++
		}
	}
	return count
}

func countIncompleteForms(formList []models.PosForm) int {
	count := 0
	for _, form := range formList {
		if form.PosUUID == "" {
			count++
		}
	}
	return count
}

func countUniqueProvincesForm(formList []models.PosForm) int {
	provinces := make(map[string]bool)
	for _, form := range formList {
		if form.Province.Name != "" {
			provinces[form.Province.Name] = true
		}
	}
	return len(provinces)
}

func countUniqueAreasForm(formList []models.PosForm) int {
	areas := make(map[string]bool)
	for _, form := range formList {
		if form.Area.Name != "" {
			areas[form.Area.Name] = true
		}
	}
	return len(areas)
}

func countUniqueSubAreasForm(formList []models.PosForm) int {
	subAreas := make(map[string]bool)
	for _, form := range formList {
		if form.SubArea.Name != "" {
			subAreas[form.SubArea.Name] = true
		}
	}
	return len(subAreas)
}

func calculateTotalPrice(formList []models.PosForm) int {
	total := 0
	for _, form := range formList {
		total += form.Price
	}
	return total
}

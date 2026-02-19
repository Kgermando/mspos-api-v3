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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

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

	// Apply common filters (geographic and agent filters)
	query = utils.ApplyCommonFilters(query, c, "pos_forms", []string{"comment"})

	// Apply date range filters for Excel export
	if startDate != "" && endDate != "" {
		query = query.Where("pos_forms.created_at >= ? AND pos_forms.created_at <= ?", startDate, endDate)
	} else if startDate != "" {
		query = query.Where("pos_forms.created_at >= ?", startDate)
	} else if endDate != "" {
		query = query.Where("pos_forms.created_at <= ?", endDate)
	}

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

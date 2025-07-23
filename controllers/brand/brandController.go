package brand

import (
	"fmt"
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// Paginate
func GetPaginatedBrands(c *fiber.Ctx) error {
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

	var dataList []models.Brand
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Brand{}).
		Where("name ILIKE ?", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Where("name ILIKE ?", "%"+search+"%").
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("PosFormItems").
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
		"message":    "PosFormItems retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Brands by CountryUUID
func GetPaginatedBrandsByCountryUUID(c *fiber.Ctx) error {
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

	var dataList []models.Brand
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Brand{}).
		Where("country_uuid = ?", CountryUUID).
		Where("name ILIKE ?", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Where("country_uuid = ?", CountryUUID).
		Where("name ILIKE ?", "%"+search+"%").
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("PosFormItems").
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
		"message":    "brands retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Paginate Brands by ProvinceUUID
func GetPaginatedBrandsByProvinceUUID(c *fiber.Ctx) error {
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

	var dataList []models.Brand
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Brand{}).
		Where("province_uuid = ?", ProvinceUUID).
		Where("name ILIKE ?", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Where("province_uuid = ?", ProvinceUUID).
		Where("name ILIKE ?", "%"+search+"%").
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("PosFormItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch brands",
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
		"message":    "brands retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllBrands(c *fiber.Ctx) error {
	db := database.DB

	var data []models.Brand
	db.
		Preload("Province").
		Order("brands.updated_at DESC").
		Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Brands",
		"data":    data,
	})
}

// Get All data by ProvinceUUID
func GetAllBrandsByProvince(c *fiber.Ctx) error {
	db := database.DB

	ProvinceUUID := c.Params("province_uuid")

	var data []models.Brand
	db.
		Preload("Province").
		Order("brands.updated_at DESC").
		Where("province_uuid = ?", ProvinceUUID).
		Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Brands",
		"data":    data,
	})
}

// Get one data
func GetOneBrand(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	var brand models.Brand
	db.Where("uuid = ?", uuid).First(&brand)
	if brand.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No brand name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "brand found",
			"data":    brand,
		},
	)
}

// Create data
func CreateBrand(c *fiber.Ctx) error {
	p := &models.Brand{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "brand created success",
			"data":    p,
		},
	)
}

// Update data
func UpdateBrand(c *fiber.Ctx) error {
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

	brand := new(models.Brand)

	db.Where("uuid = ?", uuid).First(&brand)
	brand.Name = updateData.Name
	brand.CountryUUID = updateData.CountryUUID
	brand.ProvinceUUID = updateData.ProvinceUUID
	brand.Signature = updateData.Signature

	db.Save(&brand)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "brand updated success",
			"data":    brand,
		},
	)

}

// Delete data
func DeleteBrand(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var brand models.Brand
	db.Where("uuid = ?", uuid).First(&brand)
	if brand.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No product name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&brand)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "brand deleted success",
			"data":    nil,
		},
	)
}

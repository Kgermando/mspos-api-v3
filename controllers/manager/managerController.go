package manager

import (
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// Paginate
func GetPaginatedManager(c *fiber.Ctx) error {
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

	var dataList []models.Manager
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Manager{}).
		Where("title ILIKE ?", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Where("title ILIKE ?", "%"+search+"%").
		Offset(offset).

		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("User").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Manager to fetch provinces",
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
		"message":    "Managers retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllManagers(c *fiber.Ctx) error {
	db := database.DB
	var data []models.Manager
	db.Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Managers",
		"data":    data,
	})
}

// Get one data
func GetManager(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var manager models.Manager
	db.Where("uuid = ?", uuid).First(&manager)
	if manager.Title == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No manager  name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "manager  found",
			"data":    manager,
		},
	)
}

// Create data
func CreateManager(c *fiber.Ctx) error {
	p := &models.Manager{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "manager created success",
			"data":    p,
		},
	)
}

// Update data
func UpdateManager(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		Title       string `json:"title"` // Example Head of Sales, Support, Manager, etc
		CountryUUID string   `json:"country_uuid" gorm:"type:varchar(255);not null"`
		UserUUID    string   `json:"user_uuid"` // Corrected field name
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

	manager := new(models.Manager)

	db.Where("uuid = ?", uuid).First(&manager)
	manager.Title = updateData.Title
	manager.CountryUUID = updateData.CountryUUID
	manager.UserUUID = updateData.UserUUID
	manager.Signature = updateData.Signature

	db.Save(&manager)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "manager  updated success",
			"data":    manager,
		},
	)

}

// Delete data
func DeleteManager(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var manager models.Manager
	db.Where("uuid = ?", uuid).First(&manager)
	if manager.Title == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Manager name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&manager)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Manager deleted success",
			"data":    nil,
		},
	)
}

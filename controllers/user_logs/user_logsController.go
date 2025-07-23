package user_logs

import (
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// Paginate
func GetPaginatedUserLogs(c *fiber.Ctx) error {
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

	var dataList []models.UserLogs
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.UserLogs{}).Count(&totalRecords)

	err = db.
		Joins("JOIN users ON user_logs.user_uuid=users.uuid").
		Where("users.fullname ILIKE ? OR user_logs.name ILIKE ? OR users.title ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Offset(offset).
		Limit(limit).
		Order("user_logs.updated_at DESC").
		Preload("User").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch UsersLogs",
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
		"message":    "Log retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// query data
func GetUserLogByID(c *fiber.Ctx) error {
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

	var dataList []models.UserLogs
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Country{}).
		Joins("JOIN users ON user_logs.user_uuid=users.uuid").
		Where("user_logs.user_uuid = ?", UserUUID).
		Where("users.fullname ILIKE ? OR user_logs.name ILIKE ? OR users.title ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Joins("JOIN users ON user_logs.user_uuid=users.uuid").
		Where("user_logs.user_uuid = ?", UserUUID).
		Where("users.fullname ILIKE ? OR user_logs.name ILIKE ? OR users.title ILIKE ?", "%"+search+"%", "%"+search+"%", "%"+search+"%").
		Offset(offset).
		Limit(limit).
		Order("user_logs.updated_at DESC").
		Preload("User").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch usersLogs",
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
		"message":    "Countries retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetUserLogs(c *fiber.Ctx) error {

	db := database.DB
	var data []models.UserLogs
	db.Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All UserLogs",
		"data":    data,
	})
}

// Get one data
func GetUserLog(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var user_logs models.UserLogs
	db.Where("uuid = ?", uuid).First(&user_logs)
	if user_logs.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No user_logs  name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "user_logs  found",
			"data":    user_logs,
		},
	)
}

// Create data
func CreateUserLog(c *fiber.Ctx) error {
	p := &models.UserLogs{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "UserLog created success",
			"data":    p,
		},
	)
}

// Update data
func UpdateUserLog(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		UUID string `json:"uuid"`

		Name        string `gorm:"type:text;not null" json:"name"`
		UserUUID    string `json:"user_uuid"`
		Action      string `gorm:"type:text;not null" json:"action"`
		Description string `gorm:"type:text;not null" json:"description"`
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

	user_logs := new(models.UserLogs)

	db.Where("uuid = ?", uuid).First(&user_logs)
	user_logs.Name = updateData.Name
	user_logs.UserUUID = updateData.UserUUID
	user_logs.Action = updateData.Action
	user_logs.Description = updateData.Description
	user_logs.Signature = updateData.Signature

	db.Save(&user_logs)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "user_logs  updated success",
			"data":    user_logs,
		},
	)

}

// Delete data
func DeleteUserLog(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var user_logs models.UserLogs
	// db.First(&user_logs, id)
	db.Where("uuid = ?", uuid).First(&user_logs)
	if user_logs.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No user_logs  name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&user_logs)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "user_logs  deleted success",
			"data":    nil,
		},
	)
}

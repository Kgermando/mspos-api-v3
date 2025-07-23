package country

import (
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// Paginate
func GetPaginatedCountry(c *fiber.Ctx) error {
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

	var countries []models.Country
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.Country{}).
		Where("name ILIKE ?", "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Where("name ILIKE ?", "%"+search+"%").
		Select(` 
			countries.*, 
			(
				SELECT COUNT(DISTINCT u2.uuid)
				FROM users u2
				WHERE u2.country_uuid = countries.uuid
			) AS total_users,  
			(
				SELECT COUNT(DISTINCT p.uuid)
				FROM pos p 
				WHERE p.country_uuid = countries.uuid
			) AS total_pos, 
			(
				SELECT
				COUNT(DISTINCT ps.uuid)
				FROM
				pos_forms ps
				WHERE ps.country_uuid = countries.uuid
			) AS visites
		`).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Provinces").
		Preload("Areas").
		Preload("SubAreas").
		Preload("Communes").
		Preload("Brands").
		// Preload("Users").
		// Preload("PosForms").
		Find(&countries).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch countries",
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
		"data":       countries,
		"pagination": pagination,
	})
}

// Get All data
func GetAllCountry(c *fiber.Ctx) error {
	db := database.DB
	var data []models.Country
	db.Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Countries",
		"data":    data,
	})
}

// Get one data
func GetCountry(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var Country models.Country
	db.Where("uuid = ?", uuid).First(&Country)
	if Country.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Country name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Country found",
			"data":    Country,
		},
	)
}

// Create data
func CreateCountry(c *fiber.Ctx) error {
	p := &models.Country{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = uuid.New().String()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Country created success",
			"data":    p,
		},
	)
}

// Update data
func UpdateCountry(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		UUID      string `json:"uuid"`
		Name      string `json:"name"`
		Signature string `json:"signature"`
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

	country := new(models.Country)

	db.Where("uuid = ?", uuid).First(&country)
	country.Name = updateData.Name
	country.Signature = updateData.Signature

	db.Save(&country)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Country updated success",
			"data":    country,
		},
	)

}

// Delete data
func DeleteCountry(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var country models.Country
	db.Where("uuid = ?", uuid).First(&country)
	if country.Name == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Country name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&country)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "Country deleted success",
			"data":    nil,
		},
	)
}

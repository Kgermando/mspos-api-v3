package PosFormItem

import (
	"fmt"
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/danny19977/mspos-api-v3/utils"
	"github.com/gofiber/fiber/v2"
)

// Paginate
func GetPaginatedPosformItem(c *fiber.Ctx) error {
	db := database.DB
	PosFormUUID := c.Params("posform_uuid")

	page, err := strconv.Atoi(c.Query("page", "1"))
	if err != nil || page <= 0 {
		page = 1 // Default page number
	}
	limit, err := strconv.Atoi(c.Query("limit", "15"))
	if err != nil || limit <= 0 {
		limit = 15
	}
	offset := (page - 1) * limit

	var dataList []models.PosFormItems
	var length int64
	db.Model(dataList).Where("posform_uuid = ?", PosFormUUID).Count(&length)

	db.Where("posform_uuid = ?", PosFormUUID).
		Offset(offset).
		Limit(limit).
		Order("posform_items.created_at DESC").
		Preload("PosForm").
		Preload("Brand").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Find(&dataList)

	if err != nil {
		fmt.Println("error s'est produite: ", err)
		return c.Status(500).SendString(err.Error())
	}

	// Calculate total number of pages
	totalPages := len(dataList) / limit
	if remainder := len(dataList) % limit; remainder > 0 {
		totalPages++
	}
	pagination := map[string]interface{}{
		"total_pages": totalPages,
		"page":        page,
		"page_size":   limit,
		"length":      length,
	}

	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "All stocks",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllPosFormItems(c *fiber.Ctx) error {
	db := database.DB

	var data []models.PosFormItems
	result := db.Find(&data)
	if result.Error != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": result.Error.Error(),
			"data":    nil,
		})
	}
	if result.RowsAffected == 0 {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No PosFormItems found",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All PosFormItems",
		"data":    data,
	})
}

// Get All data
func GetAllPosFormItemsByUUID(c *fiber.Ctx) error {
	db := database.DB
	PosFormUUID := c.Params("pos_form_uuid")

	var data []models.PosFormItems
	db.
		Where("pos_form_uuid = ?", PosFormUUID).
		Preload("PosForm").
		Preload("Brand").
		Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All PosFormItemsByUUID",
		"data":    data,
	})
}

// Create data
func CreatePosformItem(c *fiber.Ctx) error {
	p := &models.PosFormItems{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = utils.GenerateUUID()
	database.DB.Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "posformitem created success",
			"data":    p,
		},
	)
}

// Update data
func UpdatePosformItem(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		UUID string `json:"uuid"`

		Sold        float64 `gorm:"default:0" json:"sold"`
		NumberFarde float64 `gorm:"not null" json:"number_farde"`                   // NUMBER Farde
		Counter     int     `gorm:"not null" json:"counter"`                        // Allows to calculate the Sum of the ND Dashboard
		PosFormUUID string  `json:"posform_uuid" gorm:"type:varchar(255);not null"` // Foreign key (belongs to), tag `index` will create index for this column
		BrandUUID   string  `json:"brand_id" gorm:"type:varchar(255);not null"`     // Foreign key (belongs to), tag `index` will create index for this column
		PosUUID     string  `json:"pos_uuid" gorm:"type:varchar(255);not null"`     // Foreign key (belongs to), tag `index` will create index for this column
		// Foreign key (belongs to), tag `index` will create index for this column
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

	posFormItem := new(models.PosFormItems)

	db.Where("uuid = ?", uuid).First(&posFormItem)
	posFormItem.Sold = updateData.Sold
	posFormItem.NumberFarde = updateData.NumberFarde
	posFormItem.PosFormUUID = updateData.PosFormUUID
	posFormItem.BrandUUID = updateData.BrandUUID

	db.Save(&posFormItem)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "stock updated success",
			"data":    posFormItem,
		},
	)

}

// Delete data
func DeletePosformItem(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var posFormItems models.PosFormItems
	db.Where("uuid = ?", uuid).First(&posFormItems)
	if posFormItems.PosFormUUID == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No stock name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&posFormItems)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "stock deleted success",
			"data":    nil,
		},
	)
}

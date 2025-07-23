package routeplan

import (
	"strconv"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/danny19977/mspos-api-v3/utils"
	"github.com/gofiber/fiber/v2"
)

// Paginate
func GetPaginatedRouteplan(c *fiber.Ctx) error {

	// Initialize database connection
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

	var dataList []models.RoutePlan
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.RoutePlan{}).
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		// Select(`
		// 	route_plans.*, 
		// 	COALESCE((
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid 
		// 		AND r.status = true
		// 	), 0) AS total_item_active,
		// 	COALESCE((
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid
		// 	), 0) AS total_item
		// `).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("RoutePlanItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces",
			"error":   err.Error(),
		})
	}

	/// Calculate total pages
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
		"message":    "Routeplan retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// GetPaginatedRouthplaByProvinceID
func GetPaginatedRouthplaByProvinceUUID(c *fiber.Ctx) error {

	provinceUUID := c.Params("province_uuid")

	// Initialize database connection
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

	var dataList []models.RoutePlan
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.RoutePlan{}).
		Where("route_plans.province_uuid = ?", provinceUUID).
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Where("route_plans.province_uuid = ?", provinceUUID).
		Where(`  
		 EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		// Select(`
		// 	route_plans.*, 
		// 	(
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid 
		// 		AND r.status = true
		// 	) AS total_item_active,
		// 	 (
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid
		// 	) AS total_item
		// `).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("RoutePlanItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces",
			"error":   err.Error(),
		})
	}

	/// Calculate total pages
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
		"message":    "Routeplan retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// GetPaginatedRouthplaByareaUUID
func GetPaginatedRouthplaByareaUUID(c *fiber.Ctx) error {

	areaUUID := c.Params("area_uuid")

	// Initialize database connection
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

	var dataList []models.RoutePlan
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.RoutePlan{}).
		Where("route_plans.area_uuid = ?", areaUUID).
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Where("route_plans.area_uuid = ?", areaUUID).
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		// Select(`
		// 	route_plans.*, 
		// 	(
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid 
		// 		AND r.status = true
		// 	) AS total_item_active,
		// 	 (
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid
		// 	) AS total_item
		// `).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("RoutePlanItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces",
			"error":   err.Error(),
		})
	}

	/// Calculate total pages
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
		"message":    "Routeplan retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// GetPaginatedRouthplaBysubareaUUID
func GetPaginatedRouthplaBySubareaUUID(c *fiber.Ctx) error {

	subareaUUID := c.Params("sub_area_uuid")

	// Initialize database connection
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

	var dataList []models.RoutePlan
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.RoutePlan{}).
		Where("route_plans.sub_area_uuid = ?", subareaUUID).
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Where("route_plans.sub_area_uuid = ?", subareaUUID).
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		// Select(`
		// 	route_plans.*, 
		// 	(
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid 
		// 		AND r.status = true
		// 	) AS total_item_active,
		// 	 (
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid
		// 	) AS total_item
		// `).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("RoutePlanItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces",
			"error":   err.Error(),
		})
	}

	/// Calculate total pages
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
		"message":    "Routeplan retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// GetPaginatedRouteplaBycommuneUUID
func GetPaginatedRouteplaBycommuneUUID(c *fiber.Ctx) error {

	UserUUID := c.Params("user_uuid")

	// Initialize database connection
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

	var dataList []models.RoutePlan
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.RoutePlan{}).
		Where("route_plans.user_uuid = ?", UserUUID).
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		Count(&totalRecords)

	// Fetch paginated data
	err = db.
		Where("route_plans.user_uuid = ?", UserUUID).
		Where(`  
		EXISTS(SELECT 1 FROM users WHERE route_plans.user_uuid = users.uuid AND users.fullname ILIKE ?)
		`, "%"+search+"%").
		// Select(`
		// 	route_plans.*, 
		// 	(
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid 
		// 		AND r.status = true
		// 	) AS total_item_active,
		// 	 (
		// 		SELECT
		// 		COUNT(DISTINCT r.uuid)
		// 		FROM
		// 		route_plan_items r 
		// 		WHERE
		// 		r.route_plan_uuid = route_plans.uuid
		// 	) AS total_item
		// `).
		Offset(offset).
		Limit(limit).
		Order("updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("User").
		Preload("RoutePlanItems").
		Find(&dataList).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces",
			"error":   err.Error(),
		})
	}

	/// Calculate total pages
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
		"message":    "Routeplan retrieved successfully",
		"data":       dataList,
		"pagination": pagination,
	})
}

// Get All data
func GetAllRouteplan(c *fiber.Ctx) error {
	db := database.DB

	var data []models.RoutePlan
	db.Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Routeplan",
		"data":    data,
	})
}

// Get All data by id
func GetAllRouteplanBySearch(c *fiber.Ctx) error {
	db := database.DB

	search := c.Query("search", "")

	var data []models.RoutePlan
	db.Where("name ILIKE ?", "%"+search+"%").
		Find(&data)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All Routeplan",
		"data":    data,
	})
}

// Get one data by user id
func GetRouteplanByUserUUID(c *fiber.Ctx) error {
	userUUID := c.Params("user_uuid")
	db := database.DB

	var routeplan models.RoutePlan   
	db.Where("user_uuid = ? AND DATE(created_at) = CURRENT_DATE", userUUID).
		Order("created_at DESC").
		Preload("RoutePlanItems").
		First(&routeplan)

	if routeplan.UUID == "00000000-0000-0000-0000-000000000000" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Routeplan name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "All Routeplan",
			"data":    routeplan,
		},
	)
}

// Get one data
func GetRouteplan(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	var Routeplan models.RoutePlan
	db.Where("uuid = ?", uuid).
		Preload("RoutePlanItems").
		First(&Routeplan)
	if Routeplan.UUID == "0000000-0000-0000-0000-000000000000" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No Routeplan name found",
				"data":    nil,
			},
		)
	}
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "RoutePlan found",
			"data":    Routeplan,
		},
	)
}

// Create data
func CreateRouteplan(c *fiber.Ctx) error {
	p := &models.RoutePlan{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	p.UUID = utils.GenerateUUID()
	// Omit the primary key field (ID) during creation
	database.DB.Omit("ID").Create(p)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "routeplan created success",
			"data":    p,
		},
	)
}

// Update data
func UpdateRouteplan(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateData struct {
		UUID string `json:"uuid"`

		UserUUID     string `json:"user_uuid"`
		ProvinceUUID string `json:"province_uuid"`
		AreaUUID     string `json:"area_uuid"`
		SubAreaUUID  string `json:"sub_area_uuid"`
		CommuneUUID  string `json:"commune_uuid"`

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

	RoutePlan := new(models.RoutePlan)

	db.Where("uuid = ?", uuid).First(&RoutePlan)
	RoutePlan.UserUUID = updateData.UserUUID
	RoutePlan.ProvinceUUID = updateData.ProvinceUUID
	RoutePlan.AreaUUID = updateData.AreaUUID
	RoutePlan.SubAreaUUID = updateData.SubAreaUUID
	RoutePlan.CommuneUUID = updateData.CommuneUUID
	RoutePlan.Signature = updateData.Signature

	db.Save(&RoutePlan)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "RoutePlan updated success",
			"data":    RoutePlan,
		},
	)

}

// Delete data
func DeleteRouteplan(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var routeplan models.RoutePlan
	db.Where("uuid = ?", uuid).First(&routeplan)
	if routeplan.UUID == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No routeplan name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&routeplan)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "RoutePlan deleted success",
			"data":    nil,
		},
	)
}

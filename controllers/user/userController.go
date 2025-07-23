package user

import (
	"strconv"
	"strings"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/danny19977/mspos-api-v3/models"
	"github.com/danny19977/mspos-api-v3/utils"
	"github.com/gofiber/fiber/v2"
)

// Paginate
func GetPaginatedUsers(c *fiber.Ctx) error {
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

	var users []models.User
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.User{}).
		Where("fullname ILIKE ? OR title ILIKE ?", "%"+search+"%", "%"+search+"%").
		Count(&totalRecords)

	err = db.
		Where("fullname ILIKE ? OR title ILIKE ?", "%"+search+"%", "%"+search+"%").
		Offset(offset).
		Limit(limit).
		Order("users.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Find(&users).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch Users",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	//  Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "Users retrieved successfully",
		"data":       users,
		"pagination": pagination,
	})
}

func GetPaginatedNoSerach(c *fiber.Ctx) error {
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
	var users []models.User
	var totalRecords int64

	// Count total records matching the search query
	db.Model(&models.User{}).
		Count(&totalRecords)

	err = db.
		Offset(offset).
		Limit(limit).
		Order("users.updated_at DESC").
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Find(&users).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch Users",
			"error":   err.Error(),
		})
	}

	// Calculate total pages
	totalPages := int((totalRecords + int64(limit) - 1) / int64(limit))

	//  Prepare pagination metadata
	pagination := map[string]interface{}{
		"total_records": totalRecords,
		"total_pages":   totalPages,
		"current_page":  page,
		"page_size":     limit,
	}

	// Return response
	return c.JSON(fiber.Map{
		"status":     "success",
		"message":    "Users retrieved successfully",
		"data":       users,
		"pagination": pagination,
	})
}

// query all data
func GetAllUsers(c *fiber.Ctx) error {
	db := database.DB
	var users []models.User
	db.Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All users",
		"data":    users,
	})
}

// query data
func GetUserByID(c *fiber.Ctx) error {
	ProvinceUUID := c.Params("uuid")
	db := database.DB
	var users []models.User
	db.Where("province_uuid = ?", ProvinceUUID).Where("role = ?", "DR").Where("status = ?", true).Find(&users)

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "users by id found",
		"data":    users,
	})
}

// Get one data
func GetUser(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB
	var user models.User
	db.Where("uuid = ?", uuid).
		Preload("Country").
		Preload("Province").
		Preload("Area").
		Preload("SubArea").
		Preload("Commune").
		Preload("Pos").
		Preload("PosForms").
		Preload("RoutePlan").
		First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No User name found",
				"data":    nil,
			},
		)
	}    
	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "User found",
			"data":    user,
		},
	)
}

// Create data
func CreateUser(c *fiber.Ctx) error {
	p := &models.User{}

	if err := c.BodyParser(&p); err != nil {
		return err
	}

	if p.Fullname == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "Form not complete",
				"data":    nil,
			},
		)
	}

	if p.Password != p.PasswordConfirm {
		c.Status(400)
		return c.JSON(fiber.Map{
			"message": "passwords do not match",
		})
	}

	user := &models.User{
		Fullname:     p.Fullname,
		Email:        p.Email,
		Title:        p.Title,
		Phone:        p.Phone,
		Role:         p.Role,
		Permission:   p.Permission,
		Image:        p.Image,
		Status:       p.Status,
		Signature:    p.Signature,
		CountryUUID:  p.CountryUUID,
		ProvinceUUID: p.ProvinceUUID,
		AreaUUID:     p.AreaUUID,
		SubAreaUUID:  p.SubAreaUUID,
		CommuneUUID:  p.CommuneUUID,
		Support:      p.Support,
		SupportUUID:  p.SupportUUID,
		ManagerUUID:  p.ManagerUUID,
		Manager:      p.Manager,
		AsmUUID:      p.AsmUUID,
		Asm:          p.Asm,
		SupUUID:      p.SupUUID,
		Sup:          p.Sup,
		DrUUID:       p.DrUUID,
		Dr:           p.Dr,
		CycloUUID:    p.CycloUUID,
		Cyclo:        p.Cyclo,
	}

	user.UUID = utils.GenerateUUID()

	user.SetPassword(p.Password)

	if err := utils.ValidateStruct(*user); err != nil {
		c.Status(400)
		return c.JSON(err)
	}

	// Check if user with the same email already exists
	var existingUser models.User
	if err := database.DB.Where("email = ?", user.Email).First(&existingUser).Error; err == nil {
		return c.Status(400).JSON(fiber.Map{
			"status":  "error",
			"message": "A user with this email already exists",
			"data":    nil,
		})
	}

	// Ensure UUID is unique before creating the user
	for {
		var uuidUser models.User
		if err := database.DB.Where("uuid = ?", user.UUID).First(&uuidUser).Error; err != nil {
			// Not found, so UUID is unique
			break
		}
		// Duplicate found, regenerate UUID
		user.UUID = utils.GenerateUUID()
	}

	if err := database.DB.Create(user).Error; err != nil {
		c.Status(500)
		sm := strings.Split(err.Error(), ":")
		m := strings.TrimSpace(sm[len(sm)-1])

		return c.JSON(fiber.Map{
			"message": m,
		})
	}

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "User Created success",
			"data":    user,
		},
	)
}

// Update data
func UpdateUser(c *fiber.Ctx) error {
	uuid := c.Params("uuid")
	db := database.DB

	type UpdateDataInput struct {
		UUID            string `json:"uuid"`
		Fullname        string `json:"fullname"`
		Email           string `json:"email"`
		Title           string `json:"title"`
		Phone           string `json:"phone"`
		Password        string `json:"password" validate:"required"`
		PasswordConfirm string `json:"password_confirm"`
		Role            string `json:"role"`
		Permission      string `json:"permission"`
		Image           string `json:"image"`
		Status          bool   `json:"status"`
		CountryUUID     string `json:"country_uuid"`
		ProvinceUUID    string `json:"province_uuid"`
		AreaUUID        string `json:"area_uuid"`
		SubAreaUUID     string `json:"sub_area_uuid"`
		CommuneUUID     string `json:"commune_uuid"`
		ManagerUUID     string `json:"manager_uuid"`
		Manager         string `json:"manager" gorm:"default:''"`
		SupportUUID     string `json:"support_uuid"`
		Support         string `json:"support" gorm:"default:''"`
		AsmUUID         string `json:"asm_uuid"`
		Asm             string `json:"asm" gorm:"default:''"`
		SupUUID         string `json:"sup_uuid"`
		Sup             string `json:"sup" gorm:"default:''"`
		DrUUID          string `json:"dr_uuid"`
		Dr              string `json:"dr" gorm:"default:''"`
		CycloUUID       string `json:"cyclo_uuid"`
		Cyclo           string `json:"cyclo" gorm:"default:''"`
		Signature       string `json:"signature"`
	}
	var updateData UpdateDataInput

	if err := c.BodyParser(&updateData); err != nil {
		return c.Status(500).JSON(
			fiber.Map{
				"status":  "error",
				"message": "Review your input",
				"data":    nil,
			},
		)
	}

	user := new(models.User)

	db.Where("uuid = ?", uuid).First(&user)
	user.Fullname = updateData.Fullname
	user.Email = updateData.Email
	user.Title = updateData.Title
	user.Phone = updateData.Phone
	user.Role = updateData.Role
	user.Permission = updateData.Permission
	user.Image = updateData.Image
	user.Status = updateData.Status
	user.CountryUUID = updateData.CountryUUID
	user.ProvinceUUID = updateData.ProvinceUUID
	user.AreaUUID = updateData.AreaUUID
	user.SubAreaUUID = updateData.SubAreaUUID
	user.CommuneUUID = updateData.CommuneUUID
	user.SupportUUID = updateData.SupportUUID
	user.Support = updateData.Support
	user.ManagerUUID = updateData.ManagerUUID
	user.Manager = updateData.Manager
	user.AsmUUID = updateData.AsmUUID
	user.Asm = updateData.Asm
	user.SupUUID = updateData.SupUUID
	user.Sup = updateData.Sup
	user.DrUUID = updateData.DrUUID
	user.Dr = updateData.Dr
	user.CycloUUID = updateData.CycloUUID
	user.Cyclo = updateData.Cyclo
	user.Signature = updateData.Signature

	db.Save(&user)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "User updated success",
			"data":    user,
		},
	)
}

// Delete data
func DeleteUser(c *fiber.Ctx) error {
	uuid := c.Params("uuid")

	db := database.DB

	var User models.User
	db.Where("uuid = ?", uuid).First(&User)
	if User.Fullname == "" {
		return c.Status(404).JSON(
			fiber.Map{
				"status":  "error",
				"message": "No User name found",
				"data":    nil,
			},
		)
	}

	db.Delete(&User)

	return c.JSON(
		fiber.Map{
			"status":  "success",
			"message": "User deleted success",
			"data":    nil,
		},
	)
}

// Get users by CountryUUID
func GetUsersByCountryUUID(c *fiber.Ctx) error {
	countryUUID := c.Params("country_uuid")
	db := database.DB
	var users []models.User
	db.Where("country_uuid = ?", countryUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by country_uuid found",
		"data":    users,
	})
}

// Get single user by CountryUUID
func GetUserByCountryUUID(c *fiber.Ctx) error {
	countryUUID := c.Params("country_uuid")
	db := database.DB
	var user models.User
	db.Where("country_uuid = ?", countryUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this country_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by country_uuid found",
		"data":    user,
	})
}

// Get users by ProvinceUUID
func GetUsersByProvinceUUID(c *fiber.Ctx) error {
	provinceUUID := c.Params("province_uuid")
	db := database.DB
	var users []models.User
	db.Where("province_uuid = ?", provinceUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by province_uuid found",
		"data":    users,
	})
}

// Get single user by ProvinceUUID
func GetUserByProvinceUUID(c *fiber.Ctx) error {
	provinceUUID := c.Params("province_uuid")
	db := database.DB
	var user models.User
	db.Where("province_uuid = ?", provinceUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this province_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by province_uuid found",
		"data":    user,
	})
}

// Get users by AreaUUID
func GetUsersByAreaUUID(c *fiber.Ctx) error {
	areaUUID := c.Params("area_uuid")
	db := database.DB
	var users []models.User
	db.Where("area_uuid = ?", areaUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by area_uuid found",
		"data":    users,
	})
}

// Get single user by AreaUUID
func GetUserByAreaUUID(c *fiber.Ctx) error {
	areaUUID := c.Params("area_uuid")
	db := database.DB
	var user models.User
	db.Where("area_uuid = ?", areaUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this area_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by area_uuid found",
		"data":    user,
	})
}

// Get users by SubAreaUUID
func GetUsersBySubAreaUUID(c *fiber.Ctx) error {
	subAreaUUID := c.Params("sub_area_uuid")
	db := database.DB
	var users []models.User
	db.Where("sub_area_uuid = ?", subAreaUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by sub_area_uuid found",
		"data":    users,
	})
}

// Get single user by SubAreaUUID
func GetUserBySubAreaUUID(c *fiber.Ctx) error {
	subAreaUUID := c.Params("sub_area_uuid")
	db := database.DB
	var user models.User
	db.Where("sub_area_uuid = ?", subAreaUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this sub_area_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by sub_area_uuid found",
		"data":    user,
	})
}

// Get users by CommuneUUID
func GetUsersByCommuneUUID(c *fiber.Ctx) error {
	communeUUID := c.Params("commune_uuid")
	db := database.DB
	var users []models.User
	db.Where("commune_uuid = ?", communeUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by commune_uuid found",
		"data":    users,
	})
}

// Get single user by CommuneUUID
func GetUserByCommuneUUID(c *fiber.Ctx) error {
	communeUUID := c.Params("commune_uuid")
	db := database.DB
	var user models.User
	db.Where("commune_uuid = ?", communeUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this commune_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by commune_uuid found",
		"data":    user,
	})
}

// Get users by SupportUUID
func GetUsersBySupportUUID(c *fiber.Ctx) error {
	supportUUID := c.Params("support_uuid")
	db := database.DB
	var users []models.User
	db.Where("support_uuid = ?", supportUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by support_uuid found",
		"data":    users,
	})
}

// Get single user by SupportUUID
func GetUserBySupportUUID(c *fiber.Ctx) error {
	supportUUID := c.Params("support_uuid")
	db := database.DB
	var user models.User
	db.Where("support_uuid = ?", supportUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this support_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by support_uuid found",
		"data":    user,
	})
}

// Get users by ManagerUUID
func GetUsersByManagerUUID(c *fiber.Ctx) error {
	managerUUID := c.Params("manager_uuid")
	db := database.DB
	var users []models.User
	db.Where("manager_uuid = ?", managerUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by manager_uuid found",
		"data":    users,
	})
}

// Get single user by ManagerUUID
func GetUserByManagerUUID(c *fiber.Ctx) error {
	managerUUID := c.Params("manager_uuid")
	db := database.DB
	var user models.User
	db.Where("manager_uuid = ?", managerUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this manager_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by manager_uuid found",
		"data":    user,
	})
}

// Get users by AsmUUID
func GetUsersByAsmUUID(c *fiber.Ctx) error {
	asmUUID := c.Params("asm_uuid")
	db := database.DB
	var users []models.User
	db.Where("asm_uuid = ?", asmUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by asm_uuid found",
		"data":    users,
	})
}

// Get single user by AsmUUID
func GetUserByAsmUUID(c *fiber.Ctx) error {
	asmUUID := c.Params("asm_uuid")
	db := database.DB
	var user models.User
	db.Where("asm_uuid = ?", asmUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this asm_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by asm_uuid found",
		"data":    user,
	})
}

// Get users by SupUUID
func GetUsersBySupUUID(c *fiber.Ctx) error {
	supUUID := c.Params("sup_uuid")
	db := database.DB
	var users []models.User
	db.Where("sup_uuid = ?", supUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by sup_uuid found",
		"data":    users,
	})
}

// Get single user by SupUUID
func GetUserBySupUUID(c *fiber.Ctx) error {
	supUUID := c.Params("sup_uuid")
	db := database.DB
	var user models.User
	db.Where("sup_uuid = ?", supUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this sup_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by sup_uuid found",
		"data":    user,
	})
}

// Get users by DrUUID
func GetUsersByDrUUID(c *fiber.Ctx) error {
	drUUID := c.Params("dr_uuid")
	db := database.DB
	var users []models.User
	db.Where("dr_uuid = ?", drUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by dr_uuid found",
		"data":    users,
	})
}

// Get single user by DrUUID
func GetUserByDrUUID(c *fiber.Ctx) error {
	drUUID := c.Params("dr_uuid")
	db := database.DB
	var user models.User
	db.Where("dr_uuid = ?", drUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this dr_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by dr_uuid found",
		"data":    user,
	})
}

// Get users by CycloUUID
func GetUsersByCycloUUID(c *fiber.Ctx) error {
	cycloUUID := c.Params("cyclo_uuid")
	db := database.DB
	var users []models.User
	db.Where("cyclo_uuid = ?", cycloUUID).Find(&users)
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Users by cyclo_uuid found",
		"data":    users,
	})
}

// Get single user by CycloUUID
func GetUserByCycloUUID(c *fiber.Ctx) error {
	cycloUUID := c.Params("cyclo_uuid")
	db := database.DB
	var user models.User
	db.Where("cyclo_uuid = ?", cycloUUID).First(&user)
	if user.Fullname == "" {
		return c.Status(404).JSON(fiber.Map{
			"status":  "error",
			"message": "No user found for this cyclo_uuid",
			"data":    nil,
		})
	}
	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "User by cyclo_uuid found",
		"data":    user,
	})
}

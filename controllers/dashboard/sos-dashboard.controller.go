package dashboard

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

func SosTableViewProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name             string  `json:"name"`
		UUID             string  `json:"uuid"`
		BrandName        string  `json:"brand_name"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
		provinces.name AS name,
		provinces.uuid AS uuid,
		brands.name AS brand_name, 
		ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
		(SELECT SUM(pos_form_items.number_farde) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
		) AS total_global_farde,
		ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?))::numeric, 2) AS percentage,
		(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
		 ) AS total_pos
	`, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date).
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Group("provinces.name, provinces.uuid, brands.name").
		Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

// Bar chart for SOS by Province with aggregated data
func SosBarChartProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name   string `json:"name"`
		UUID   string `json:"uuid"`
		Brands []struct {
			BrandName  string  `json:"brand_name"`
			TotalFarde float64 `json:"total_farde"`
			Percentage float64 `json:"percentage"`
		} `json:"brands"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		TotalPos         int64   `json:"total_pos"`
	}

	// Query to get brand data for provinces (same logic as SosTableViewProvince)
	var rawResults []struct {
		Name             string  `json:"name"`
		UUID             string  `json:"uuid"`
		BrandName        string  `json:"brand_name"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
		provinces.name AS name,
		provinces.uuid AS uuid,
		brands.name AS brand_name, 
		ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
		(SELECT SUM(pos_form_items.number_farde) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
		) AS total_global_farde,
		ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?))::numeric, 2) AS percentage,
		(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
		 ) AS total_pos
	`, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date).
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN provinces ON pos_forms.province_uuid = provinces.uuid").
		Group("provinces.name, provinces.uuid, brands.name").
		Scan(&rawResults).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	// Map to store province data
	provinceMap := make(map[string]struct {
		Name             string
		UUID             string
		TotalGlobalFarde float64
		TotalPos         int64
		Brands           []struct {
			BrandName  string  `json:"brand_name"`
			TotalFarde float64 `json:"total_farde"`
			Percentage float64 `json:"percentage"`
		}
	})

	// Process the data
	for _, result := range rawResults {
		// Initialize province if not exists
		if province, exists := provinceMap[result.Name]; !exists {
			provinceMap[result.Name] = struct {
				Name             string
				UUID             string
				TotalGlobalFarde float64
				TotalPos         int64
				Brands           []struct {
					BrandName  string  `json:"brand_name"`
					TotalFarde float64 `json:"total_farde"`
					Percentage float64 `json:"percentage"`
				}
			}{
				Name:             result.Name,
				UUID:             result.UUID,
				TotalGlobalFarde: result.TotalGlobalFarde,
				TotalPos:         result.TotalPos,
				Brands: []struct {
					BrandName  string  `json:"brand_name"`
					TotalFarde float64 `json:"total_farde"`
					Percentage float64 `json:"percentage"`
				}{
					{
						BrandName:  result.BrandName,
						TotalFarde: result.TotalFarde,
						Percentage: result.Percentage,
					},
				},
			}
		} else {
			// Add brand to existing province
			province.Brands = append(province.Brands, struct {
				BrandName  string  `json:"brand_name"`
				TotalFarde float64 `json:"total_farde"`
				Percentage float64 `json:"percentage"`
			}{
				BrandName:  result.BrandName,
				TotalFarde: result.TotalFarde,
				Percentage: result.Percentage,
			})
			provinceMap[result.Name] = province
		}
	}

	// Convert map to slice for response
	for _, province := range provinceMap {
		results = append(results, struct {
			Name   string `json:"name"`
			UUID   string `json:"uuid"`
			Brands []struct {
				BrandName  string  `json:"brand_name"`
				TotalFarde float64 `json:"total_farde"`
				Percentage float64 `json:"percentage"`
			} `json:"brands"`
			TotalGlobalFarde float64 `json:"total_global_farde"`
			TotalPos         int64   `json:"total_pos"`
		}{
			Name:             province.Name,
			UUID:             province.UUID,
			Brands:           province.Brands,
			TotalGlobalFarde: province.TotalGlobalFarde,
			TotalPos:         province.TotalPos,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Bar chart data for SOS by Province",
		"data":    results,
	})
}

func SosTableViewArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name             string  `json:"name"`
		UUID             string  `json:"uuid"`
		BrandName        string  `json:"brand_name"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
			areas.name AS name,
			areas.uuid AS uuid,
			brands.name AS brand_name, 
			ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
			(SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = areas.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_global_farde,
			ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			  AND pos_forms.area_uuid = areas.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?))::numeric, 2) AS percentage,
			(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
			FROM pos_form_items 
			INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_pos
		`, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date).
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Group("areas.name, areas.uuid, brands.name").
		Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

// Bar chart for SOS by Area with aggregated data
func SosBarChartArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name   string `json:"name"`
		UUID   string `json:"uuid"`
		Brands []struct {
			BrandName  string  `json:"brand_name"`
			TotalFarde float64 `json:"total_farde"`
			Percentage float64 `json:"percentage"`
		} `json:"brands"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		TotalPos         int64   `json:"total_pos"`
	}

	// Query to get brand data for areas (same logic as SosTableViewArea)
	var rawResults []struct {
		Name             string  `json:"name"`
		UUID             string  `json:"uuid"`
		BrandName        string  `json:"brand_name"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
			areas.name AS name,
			areas.uuid AS uuid,
			brands.name AS brand_name, 
			ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
			(SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = areas.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_global_farde,
			ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			  AND pos_forms.area_uuid = areas.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?))::numeric, 2) AS percentage,
			(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
			FROM pos_form_items 
			INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_pos
		`, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date).
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?", country_uuid, province_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN areas ON pos_forms.area_uuid = areas.uuid").
		Group("areas.name, areas.uuid, brands.name").
		Scan(&rawResults).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	// Map to store area data
	areaMap := make(map[string]struct {
		Name             string
		UUID             string
		TotalGlobalFarde float64
		TotalPos         int64
		Brands           []struct {
			BrandName  string  `json:"brand_name"`
			TotalFarde float64 `json:"total_farde"`
			Percentage float64 `json:"percentage"`
		}
	})

	// Process the data
	for _, result := range rawResults {
		// Initialize area if not exists
		if area, exists := areaMap[result.Name]; !exists {
			areaMap[result.Name] = struct {
				Name             string
				UUID             string
				TotalGlobalFarde float64
				TotalPos         int64
				Brands           []struct {
					BrandName  string  `json:"brand_name"`
					TotalFarde float64 `json:"total_farde"`
					Percentage float64 `json:"percentage"`
				}
			}{
				Name:             result.Name,
				UUID:             result.UUID,
				TotalGlobalFarde: result.TotalGlobalFarde,
				TotalPos:         result.TotalPos,
				Brands: []struct {
					BrandName  string  `json:"brand_name"`
					TotalFarde float64 `json:"total_farde"`
					Percentage float64 `json:"percentage"`
				}{
					{
						BrandName:  result.BrandName,
						TotalFarde: result.TotalFarde,
						Percentage: result.Percentage,
					},
				},
			}
		} else {
			// Add brand to existing area
			area.Brands = append(area.Brands, struct {
				BrandName  string  `json:"brand_name"`
				TotalFarde float64 `json:"total_farde"`
				Percentage float64 `json:"percentage"`
			}{
				BrandName:  result.BrandName,
				TotalFarde: result.TotalFarde,
				Percentage: result.Percentage,
			})
			areaMap[result.Name] = area
		}
	}

	// Convert map to slice for response
	for _, area := range areaMap {
		results = append(results, struct {
			Name   string `json:"name"`
			UUID   string `json:"uuid"`
			Brands []struct {
				BrandName  string  `json:"brand_name"`
				TotalFarde float64 `json:"total_farde"`
				Percentage float64 `json:"percentage"`
			} `json:"brands"`
			TotalGlobalFarde float64 `json:"total_global_farde"`
			TotalPos         int64   `json:"total_pos"`
		}{
			Name:             area.Name,
			UUID:             area.UUID,
			Brands:           area.Brands,
			TotalGlobalFarde: area.TotalGlobalFarde,
			TotalPos:         area.TotalPos,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Bar chart data for SOS by Area",
		"data":    results,
	})
}

func SosTableViewSubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name             string  `json:"name"`
		UUID             string  `json:"uuid"`
		BrandName        string  `json:"brand_name"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
			sub_areas.name AS name,
			sub_areas.uuid AS uuid,
			brands.name AS brand_name, 
			ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
			(SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = ? 
			 AND pos_forms.sub_area_uuid = sub_areas.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_global_farde,
			ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = ?
			 AND pos_forms.sub_area_uuid = sub_areas.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?))::numeric, 2) AS percentage,
			(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
			FROM pos_form_items 
			INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_pos
		`, country_uuid, province_uuid, area_uuid, start_date, end_date, country_uuid, province_uuid, area_uuid, start_date, end_date, country_uuid, province_uuid, area_uuid, start_date, end_date).
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ?", country_uuid, province_uuid, area_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Group("sub_areas.name, sub_areas.uuid, brands.name").
		Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

// Bar chart for SOS by SubArea with aggregated data
func SosBarChartSubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name   string `json:"name"`
		UUID   string `json:"uuid"`
		Brands []struct {
			BrandName  string  `json:"brand_name"`
			TotalFarde float64 `json:"total_farde"`
			Percentage float64 `json:"percentage"`
		} `json:"brands"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		TotalPos         int64   `json:"total_pos"`
	}

	// Query to get brand data for sub areas (same logic as SosTableViewSubArea)
	var rawResults []struct {
		Name             string  `json:"name"`
		UUID             string  `json:"uuid"`
		BrandName        string  `json:"brand_name"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
			sub_areas.name AS name,
			sub_areas.uuid AS uuid,
			brands.name AS brand_name, 
			ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
			(SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = ? 
			 AND pos_forms.sub_area_uuid = sub_areas.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_global_farde,
			ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = ?
			 AND pos_forms.sub_area_uuid = sub_areas.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?))::numeric, 2) AS percentage,
			(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
			FROM pos_form_items 
			INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_pos
		`, country_uuid, province_uuid, area_uuid, start_date, end_date, country_uuid, province_uuid, area_uuid, start_date, end_date, country_uuid, province_uuid, area_uuid, start_date, end_date).
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ?", country_uuid, province_uuid, area_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid").
		Group("sub_areas.name, sub_areas.uuid, brands.name").
		Scan(&rawResults).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	// Map to store sub area data
	subAreaMap := make(map[string]struct {
		Name             string
		UUID             string
		TotalGlobalFarde float64
		TotalPos         int64
		Brands           []struct {
			BrandName  string  `json:"brand_name"`
			TotalFarde float64 `json:"total_farde"`
			Percentage float64 `json:"percentage"`
		}
	})

	// Process the data
	for _, result := range rawResults {
		// Initialize sub area if not exists
		if subArea, exists := subAreaMap[result.Name]; !exists {
			subAreaMap[result.Name] = struct {
				Name             string
				UUID             string
				TotalGlobalFarde float64
				TotalPos         int64
				Brands           []struct {
					BrandName  string  `json:"brand_name"`
					TotalFarde float64 `json:"total_farde"`
					Percentage float64 `json:"percentage"`
				}
			}{
				Name:             result.Name,
				UUID:             result.UUID,
				TotalGlobalFarde: result.TotalGlobalFarde,
				TotalPos:         result.TotalPos,
				Brands: []struct {
					BrandName  string  `json:"brand_name"`
					TotalFarde float64 `json:"total_farde"`
					Percentage float64 `json:"percentage"`
				}{
					{
						BrandName:  result.BrandName,
						TotalFarde: result.TotalFarde,
						Percentage: result.Percentage,
					},
				},
			}
		} else {
			// Add brand to existing sub area
			subArea.Brands = append(subArea.Brands, struct {
				BrandName  string  `json:"brand_name"`
				TotalFarde float64 `json:"total_farde"`
				Percentage float64 `json:"percentage"`
			}{
				BrandName:  result.BrandName,
				TotalFarde: result.TotalFarde,
				Percentage: result.Percentage,
			})
			subAreaMap[result.Name] = subArea
		}
	}

	// Convert map to slice for response
	for _, subArea := range subAreaMap {
		results = append(results, struct {
			Name   string `json:"name"`
			UUID   string `json:"uuid"`
			Brands []struct {
				BrandName  string  `json:"brand_name"`
				TotalFarde float64 `json:"total_farde"`
				Percentage float64 `json:"percentage"`
			} `json:"brands"`
			TotalGlobalFarde float64 `json:"total_global_farde"`
			TotalPos         int64   `json:"total_pos"`
		}{
			Name:             subArea.Name,
			UUID:             subArea.UUID,
			Brands:           subArea.Brands,
			TotalGlobalFarde: subArea.TotalGlobalFarde,
			TotalPos:         subArea.TotalPos,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Bar chart data for SOS by SubArea",
		"data":    results,
	})
}

func SosTableViewCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name             string  `json:"name"`
		UUID             string  `json:"uuid"`
		BrandName        string  `json:"brand_name"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
			communes.name AS name,
			communes.uuid AS uuid,
			brands.name AS brand_name, 
			ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
			(SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = ? 
			 AND pos_forms.sub_area_uuid = ? 
			 AND pos_forms.commune_uuid = communes.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_global_farde,
			ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = ? 
			 AND pos_forms.sub_area_uuid = ? 
			AND pos_forms.commune_uuid = communes.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?))::numeric, 2) AS percentage,
			(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
			FROM pos_form_items 
			INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.sub_area_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_pos
		`, country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date, country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date, country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date).
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.sub_area_uuid = ?", country_uuid, province_uuid, area_uuid, sub_area_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Group("communes.name, communes.uuid, brands.name").
		Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

// Bar chart for SOS by Commune with aggregated data
func SosBarChartCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name   string `json:"name"`
		UUID   string `json:"uuid"`
		Brands []struct {
			BrandName  string  `json:"brand_name"`
			TotalFarde float64 `json:"total_farde"`
			Percentage float64 `json:"percentage"`
		} `json:"brands"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		TotalPos         int64   `json:"total_pos"`
	}

	// Query to get brand data for communes (same logic as SosTableViewCommune)
	var rawResults []struct {
		Name             string  `json:"name"`
		UUID             string  `json:"uuid"`
		BrandName        string  `json:"brand_name"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
			communes.name AS name,
			communes.uuid AS uuid,
			brands.name AS brand_name, 
			ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
			(SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = ? 
			 AND pos_forms.sub_area_uuid = ? 
			 AND pos_forms.commune_uuid = communes.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_global_farde,
			ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
			 FROM pos_form_items 
			 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			 WHERE pos_form_items.deleted_at IS NULL 
			 AND pos_forms.country_uuid = ? 
			 AND pos_forms.province_uuid = ? 
			 AND pos_forms.area_uuid = ? 
			 AND pos_forms.sub_area_uuid = ? 
			AND pos_forms.commune_uuid = communes.uuid
			 AND pos_forms.created_at BETWEEN ? AND ?))::numeric, 2) AS percentage,
			(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
			FROM pos_form_items 
			INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
			WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.sub_area_uuid = ? AND pos_forms.created_at BETWEEN ? AND ?
			) AS total_pos
		`, country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date, country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date, country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date).
		Where("pos_forms.country_uuid = ? AND pos_forms.province_uuid = ? AND pos_forms.area_uuid = ? AND pos_forms.sub_area_uuid = ?", country_uuid, province_uuid, area_uuid, sub_area_uuid).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL").
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Joins("INNER JOIN communes ON pos_forms.commune_uuid = communes.uuid").
		Group("communes.name, communes.uuid, brands.name").
		Scan(&rawResults).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	// Map to store commune data
	communeMap := make(map[string]struct {
		Name             string
		UUID             string
		TotalGlobalFarde float64
		TotalPos         int64
		Brands           []struct {
			BrandName  string  `json:"brand_name"`
			TotalFarde float64 `json:"total_farde"`
			Percentage float64 `json:"percentage"`
		}
	})

	// Process the data
	for _, result := range rawResults {
		// Initialize commune if not exists
		if commune, exists := communeMap[result.Name]; !exists {
			communeMap[result.Name] = struct {
				Name             string
				UUID             string
				TotalGlobalFarde float64
				TotalPos         int64
				Brands           []struct {
					BrandName  string  `json:"brand_name"`
					TotalFarde float64 `json:"total_farde"`
					Percentage float64 `json:"percentage"`
				}
			}{
				Name:             result.Name,
				UUID:             result.UUID,
				TotalGlobalFarde: result.TotalGlobalFarde,
				TotalPos:         result.TotalPos,
				Brands: []struct {
					BrandName  string  `json:"brand_name"`
					TotalFarde float64 `json:"total_farde"`
					Percentage float64 `json:"percentage"`
				}{
					{
						BrandName:  result.BrandName,
						TotalFarde: result.TotalFarde,
						Percentage: result.Percentage,
					},
				},
			}
		} else {
			// Add brand to existing commune
			commune.Brands = append(commune.Brands, struct {
				BrandName  string  `json:"brand_name"`
				TotalFarde float64 `json:"total_farde"`
				Percentage float64 `json:"percentage"`
			}{
				BrandName:  result.BrandName,
				TotalFarde: result.TotalFarde,
				Percentage: result.Percentage,
			})
			communeMap[result.Name] = commune
		}
	}

	// Convert map to slice for response
	for _, commune := range communeMap {
		results = append(results, struct {
			Name   string `json:"name"`
			UUID   string `json:"uuid"`
			Brands []struct {
				BrandName  string  `json:"brand_name"`
				TotalFarde float64 `json:"total_farde"`
				Percentage float64 `json:"percentage"`
			} `json:"brands"`
			TotalGlobalFarde float64 `json:"total_global_farde"`
			TotalPos         int64   `json:"total_pos"`
		}{
			Name:             commune.Name,
			UUID:             commune.UUID,
			Brands:           commune.Brands,
			TotalGlobalFarde: commune.TotalGlobalFarde,
			TotalPos:         commune.TotalPos,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Bar chart data for SOS by Commune",
		"data":    results,
	})
}

func SosTotalByBrandByMonth(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	year := c.Query("year")

	var results []struct {
		BrandName        string  `json:"brand_name"`
		Month            int     `json:"month"`
		TotalFarde       float64 `json:"total_farde"`
		TotalGlobalFarde float64 `json:"total_global_farde"`
		Percentage       float64 `json:"percentage"`
		TotalPos         int64   `json:"total_pos"`
	}

	err := db.Table("pos_form_items").
		Select(`
		brands.name AS brand_name,
		EXTRACT(MONTH FROM pos_forms.created_at) AS month, 
		ROUND(SUM(pos_form_items.number_farde)::numeric, 2) AS total_farde,
		(SELECT SUM(pos_form_items.number_farde) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND EXTRACT(YEAR FROM pos_forms.created_at) = ?
		) AS total_global_farde,
		ROUND((SUM(pos_form_items.number_farde) * 100.0 / (SELECT SUM(pos_form_items.number_farde) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND EXTRACT(YEAR FROM pos_forms.created_at) = ?))::numeric, 2) AS percentage,
		(SELECT COUNT(DISTINCT pos_forms.pos_uuid) 
		 FROM pos_form_items 
		 INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		 WHERE pos_form_items.deleted_at IS NULL AND pos_forms.country_uuid = ? AND EXTRACT(YEAR FROM pos_forms.created_at) = ?
		 ) AS total_pos
	`, country_uuid, year, country_uuid, year, country_uuid, year).
		Joins("INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid").
		Joins("INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid").
		Where("pos_forms.country_uuid = ? AND EXTRACT(YEAR FROM pos_forms.created_at) = ?", country_uuid, year).
		Where("pos_forms.deleted_at IS NULL").
		Group("brands.name, month").
		Order("brands.name, month ASC").
		Scan(&results).Error

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "results data",
		"data":    results,
	})
}

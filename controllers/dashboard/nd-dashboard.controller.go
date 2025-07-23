package dashboard

import (
	"encoding/json"
	"fmt"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

// calculate the ND by Country and Province
func NdTableViewProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name     string  `json:"name"`
		UUID     string  `json:"uuid"`
		Brand    string  `json:"brand"`
		Presence int     `json:"presence"`
		Visits   int     `json:"visits"`
		Pourcent float64 `json:"pourcent"`
	}

	sqlQuery := `
	   
		SELECT 
		provinces.name AS name,
		provinces.uuid AS uuid,
		brands.name AS brand,

		COUNT(pos_form_items.uuid) AS presence,

		(SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		) AS visits,

		(COUNT(pos_form_items.uuid) * 100 / (
		SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		)) AS pourcent
		
		FROM pos_form_items 
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid
		INNER JOIN provinces ON pos_forms.province_uuid = provinces.uuid
		WHERE pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY provinces.name, provinces.uuid, brands.name
		ORDER BY pourcent DESC;
	`
	rows, err := db.Raw(sqlQuery, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}
	defer rows.Close()

	for rows.Next() {
		var name, uuid, brand string
		var presence, visits int
		var pourcent float64
		if err := rows.Scan(&name, &uuid, &brand, &presence, &visits, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan data",
				"error":   err.Error(),
			})
		}
		results = append(results, struct {
			Name     string  `json:"name"`
			UUID     string  `json:"uuid"`
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Visits   int     `json:"visits"`
			Pourcent float64 `json:"pourcent"`
		}{
			Name:     name,
			UUID:     uuid,
			Brand:    brand,
			Presence: presence,
			Visits:   visits,
			Pourcent: pourcent,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

// calculate the ND by Area Found here
func NdTableViewArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name     string  `json:"name"`
		UUID     string  `json:"uuid"`
		Brand    string  `json:"brand"`
		Presence int     `json:"presence"`
		Visits   int     `json:"visits"`
		Pourcent float64 `json:"pourcent"`
	}

	sqlQuery := `
		SELECT  
		areas.name AS name,
		areas.uuid AS uuid,
		brands.name AS brand,

		COUNT(pos_form_items.uuid) AS presence,

		(SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? AND 
		pos_forms.province_uuid = ? AND
		pos_forms.area_uuid = areas.uuid 
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		) AS visits,

		(COUNT(pos_form_items.uuid) * 100 / (
		SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? AND 
		pos_forms.province_uuid = ?
		AND pos_forms.area_uuid = areas.uuid
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		)) AS pourcent
		
		FROM pos_form_items
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid
		INNER JOIN areas ON pos_forms.area_uuid = areas.uuid
		WHERE pos_forms.country_uuid = ?
		AND pos_forms.province_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY areas.name, areas.uuid, brands.name
		ORDER BY pourcent DESC;
	`
	rows, err := db.Raw(sqlQuery, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date, country_uuid, province_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}
	defer rows.Close()

	for rows.Next() {
		var name, uuid, brand string
		var presence, visits int
		var pourcent float64
		if err := rows.Scan(&name, &uuid, &brand, &presence, &visits, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan data",
				"error":   err.Error(),
			})
		}
		results = append(results, struct {
			Name     string  `json:"name"`
			UUID     string  `json:"uuid"`
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Visits   int     `json:"visits"`
			Pourcent float64 `json:"pourcent"`
		}{
			Name:     name,
			UUID:     uuid,
			Brand:    brand,
			Presence: presence,
			Visits:   visits,
			Pourcent: pourcent,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

// calculate the ND by Subarea Found here
func NdTableViewSubArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name     string  `json:"name"`
		UUID     string  `json:"uuid"`
		Brand    string  `json:"brand"`
		Presence int     `json:"presence"`
		Visits   int     `json:"visits"`
		Pourcent float64 `json:"pourcent"`
	}

	sqlQuery := `
	   
		SELECT  
		sub_areas.name AS name,
		sub_areas.uuid AS uuid,
		brands.name AS brand,

		COUNT(pos_form_items.uuid) AS presence,

		(SELECT COUNT(pos_forms.uuid) FROM pos_forms 
			WHERE pos_forms.country_uuid = ? 
			AND pos_forms.province_uuid = ? 
			AND pos_forms.area_uuid = ?
			AND pos_forms.sub_area_uuid = sub_areas.uuid
			AND pos_forms.created_at BETWEEN ? AND ?
			AND pos_forms.deleted_at IS NULL
		) AS visits,

		(COUNT(pos_form_items.uuid) * 100 / (
		SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ?
		AND pos_forms.sub_area_uuid = sub_areas.uuid
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		)) AS pourcent
		FROM pos_form_items 
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid 
		INNER JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY sub_areas.name, sub_areas.uuid, brands.name
		ORDER BY pourcent DESC;
	`

	rows, err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}
	defer rows.Close()

	for rows.Next() {
		var name, uuid, brand string
		var presence, visits int
		var pourcent float64
		if err := rows.Scan(&name, &uuid, &brand, &presence, &visits, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan data",
				"error":   err.Error(),
			})
		}
		results = append(results, struct {
			Name     string  `json:"name"`
			UUID     string  `json:"uuid"`
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Visits   int     `json:"visits"`
			Pourcent float64 `json:"pourcent"`
		}{
			Name:     name,
			UUID:     uuid,
			Brand:    brand,
			Presence: presence,
			Visits:   visits,
			Pourcent: pourcent,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

// calculate the ND by Commune Found here
func NdTableViewCommune(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	area_uuid := c.Query("area_uuid")
	sub_area_uuid := c.Query("sub_area_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name     string  `json:"name"`
		UUID     string  `json:"uuid"`
		Brand    string  `json:"brand"`
		Presence int     `json:"presence"`
		Visits   int     `json:"visits"`
		Pourcent float64 `json:"pourcent"`
	}

	fmt.Println("country_uuid:", country_uuid)
	fmt.Println("province_uuid:", province_uuid)
	fmt.Println("area_uuid:", area_uuid)
	fmt.Println("sub_area_uuid:", sub_area_uuid)
	fmt.Println("start_date:", start_date)
	fmt.Println("end_date:", end_date)

	sqlQuery := `
		SELECT  
		communes.name AS name,
		communes.uuid AS uuid,
		brands.name AS brand,
		COUNT(pos_form_items.uuid) AS presence,
		(SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ? 
		AND pos_forms.sub_area_uuid = ?
		AND pos_forms.commune_uuid = communes.uuid
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		) AS visits,
		(COUNT(pos_form_items.uuid) * 100.0 / (
		SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ? 
		AND pos_forms.sub_area_uuid = ?
		AND pos_forms.commune_uuid = communes.uuid
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		)) AS pourcent
		FROM pos_form_items 
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid 
		INNER JOIN communes ON pos_forms.commune_uuid = communes.uuid 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ? 
		AND pos_forms.sub_area_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY communes.name, communes.uuid, brands.name
		ORDER BY pourcent DESC;
	`

	rows, err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}
	defer rows.Close()

	for rows.Next() {
		var name, uuid, brand string
		var presence, visits int
		var pourcent float64
		if err := rows.Scan(&name, &uuid, &brand, &presence, &visits, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan data",
				"error":   err.Error(),
			})
		}
		results = append(results, struct {
			Name     string  `json:"name"`
			UUID     string  `json:"uuid"`
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Visits   int     `json:"visits"`
			Pourcent float64 `json:"pourcent"`
		}{
			Name:     name,
			UUID:     uuid,
			Brand:    brand,
			Presence: presence,
			Visits:   visits,
			Pourcent: pourcent,
		})
	}

	json, err := json.Marshal(results)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to marshal results",
			"error":   err.Error(),
		})
	}
	fmt.Println("JSON Results:", string(json))

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "chartData data",
		"data":    results,
	})
}

// Line chart for sum brand by month
func NdTotalByBrandByMonth(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	year := c.Query("year")

	var results []struct {
		Brand    string  `json:"brand"`
		Month    int     `json:"month"`
		Presence int     `json:"presence"`
		Pourcent float64 `json:"pourcent"`
	}

	sqlQuery := `
		SELECT
			brands.name AS brand,
			EXTRACT(MONTH FROM pos_forms.created_at) AS month,
			COUNT(brands.name) AS presence,
			(COUNT(brands.name) * 100 / (
				SELECT COUNT(pos_forms.uuid) FROM pos_forms 
				WHERE pos_forms.country_uuid = ? 
				AND EXTRACT(YEAR FROM pos_forms.created_at) = ?
				AND pos_forms.deleted_at IS NULL
			)) AS pourcent
		FROM pos_form_items 
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid
		INNER JOIN provinces ON pos_forms.province_uuid = provinces.uuid
		WHERE pos_forms.country_uuid = ? AND EXTRACT(YEAR FROM pos_forms.created_at) = ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY brands.name, month
		ORDER BY brands.name, month ASC;
	`
	rows, err := db.Raw(sqlQuery, country_uuid, year, country_uuid, year).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}
	defer rows.Close()
	for rows.Next() {
		var brand string
		var month, presence int
		var pourcent float64
		if err := rows.Scan(&brand, &month, &presence, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan data",
				"error":   err.Error(),
			})
		}
		results = append(results, struct {
			Brand    string  `json:"brand"`
			Month    int     `json:"month"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		}{
			Brand:    brand,
			Month:    month,
			Presence: presence,
			Pourcent: pourcent,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Total count by brand grouped by month for the year",
		"data":    results,
	})
}

// Bar chart for ND by Province with aggregated data
func NdBarChartProvince(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name   string `json:"name"`
		Brands []struct {
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		} `json:"brands"`
		TotalVisits int `json:"total_visits"`
	}

	// First query to get provinces and their total visits
	provincesQuery := `
		SELECT DISTINCT
			provinces.name AS name,
			provinces.uuid AS uuid,
			COUNT(DISTINCT pos_forms.uuid) AS total_visits
		FROM pos_forms
		INNER JOIN provinces ON pos_forms.province_uuid = provinces.uuid
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY provinces.name, provinces.uuid
		ORDER BY provinces.name;
	`

	provinceRows, err := db.Raw(provincesQuery, country_uuid, province_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch provinces data",
			"error":   err.Error(),
		})
	}
	defer provinceRows.Close()

	// Map to store province data
	provinceMap := make(map[string]struct {
		Name        string
		TotalVisits int
		Brands      []struct {
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		}
	})

	// Process provinces
	for provinceRows.Next() {
		var name, uuid string
		var totalVisits int
		if err := provinceRows.Scan(&name, &uuid, &totalVisits); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan provinces data",
				"error":   err.Error(),
			})
		}
		provinceMap[name] = struct {
			Name        string
			TotalVisits int
			Brands      []struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			}
		}{
			Name:        name,
			TotalVisits: totalVisits,
			Brands: []struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			}{},
		}
	}

	// Second query to get brand data for each province
	brandsQuery := `
		SELECT 
			provinces.name AS province_name,
			brands.name AS brand,
			COUNT(pos_form_items.uuid) AS presence,
			(COUNT(pos_form_items.uuid) * 100.0 / (
				SELECT COUNT(pos_forms.uuid) FROM pos_forms 
				WHERE pos_forms.country_uuid = ? 
				AND pos_forms.province_uuid = ?
				AND pos_forms.created_at BETWEEN ? AND ?
				AND pos_forms.deleted_at IS NULL
			)) AS pourcent
		FROM pos_form_items 
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid
		INNER JOIN provinces ON pos_forms.province_uuid = provinces.uuid
		WHERE pos_forms.country_uuid = ? AND pos_forms.province_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY provinces.name, brands.name
		ORDER BY provinces.name, pourcent DESC;
	`

	brandRows, err := db.Raw(brandsQuery,
		country_uuid, province_uuid, start_date, end_date,
		country_uuid, province_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch brands data",
			"error":   err.Error(),
		})
	}
	defer brandRows.Close()

	// Process brands data
	for brandRows.Next() {
		var provinceName, brand string
		var presence int
		var pourcent float64
		if err := brandRows.Scan(&provinceName, &brand, &presence, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan brands data",
				"error":   err.Error(),
			})
		}

		if province, exists := provinceMap[provinceName]; exists {
			province.Brands = append(province.Brands, struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			}{
				Brand:    brand,
				Presence: presence,
				Pourcent: pourcent,
			})
			provinceMap[provinceName] = province
		}
	}

	// Convert map to slice for response
	for _, province := range provinceMap {
		results = append(results, struct {
			Name   string `json:"name"`
			Brands []struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			} `json:"brands"`
			TotalVisits int `json:"total_visits"`
		}{
			Name:        province.Name,
			Brands:      province.Brands,
			TotalVisits: province.TotalVisits,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Bar chart data for ND by Province",
		"data":    results,
	})
}

// Bar chart for ND by Area with aggregated data - shows ALL areas within a province
func NdBarChartArea(c *fiber.Ctx) error {
	db := database.DB

	country_uuid := c.Query("country_uuid")
	province_uuid := c.Query("province_uuid")
	start_date := c.Query("start_date")
	end_date := c.Query("end_date")

	var results []struct {
		Name   string `json:"name"`
		UUID   string `json:"uuid"`
		Brands []struct {
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		} `json:"brands"`
		TotalVisits int `json:"total_visits"`
	}

	// Query to get brand data for ALL areas within the province (same logic as NdTableViewArea)
	sqlQuery := `
		SELECT  
		areas.name AS name,
		areas.uuid AS uuid,
		brands.name AS brand,
		COUNT(pos_form_items.uuid) AS presence,
		(SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? AND 
		pos_forms.province_uuid = ? AND
		pos_forms.area_uuid = areas.uuid 
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		) AS visits,
		(COUNT(pos_form_items.uuid) * 100.0 / (
		SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? AND 
		pos_forms.province_uuid = ?
		AND pos_forms.area_uuid = areas.uuid
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		)) AS pourcent
		FROM pos_form_items
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid
		INNER JOIN areas ON pos_forms.area_uuid = areas.uuid
		WHERE pos_forms.country_uuid = ?
		AND pos_forms.province_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY areas.name, areas.uuid, brands.name
		ORDER BY areas.name, pourcent DESC;
	`

	rows, err := db.Raw(sqlQuery,
		country_uuid, province_uuid, start_date, end_date,
		country_uuid, province_uuid, start_date, end_date,
		country_uuid, province_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}
	defer rows.Close()

	// Map to store area data
	areaMap := make(map[string]struct {
		Name        string
		UUID        string
		TotalVisits int
		Brands      []struct {
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		}
	})

	// Process the data
	for rows.Next() {
		var name, uuid, brand string
		var presence, visits int
		var pourcent float64
		if err := rows.Scan(&name, &uuid, &brand, &presence, &visits, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan data",
				"error":   err.Error(),
			})
		}

		// Initialize area if not exists
		if area, exists := areaMap[name]; !exists {
			areaMap[name] = struct {
				Name        string
				UUID        string
				TotalVisits int
				Brands      []struct {
					Brand    string  `json:"brand"`
					Presence int     `json:"presence"`
					Pourcent float64 `json:"pourcent"`
				}
			}{
				Name:        name,
				UUID:        uuid,
				TotalVisits: visits,
				Brands: []struct {
					Brand    string  `json:"brand"`
					Presence int     `json:"presence"`
					Pourcent float64 `json:"pourcent"`
				}{
					{
						Brand:    brand,
						Presence: presence,
						Pourcent: pourcent,
					},
				},
			}
		} else {
			// Add brand to existing area
			area.Brands = append(area.Brands, struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			}{
				Brand:    brand,
				Presence: presence,
				Pourcent: pourcent,
			})
			areaMap[name] = area
		}
	}

	// Convert map to slice for response
	for _, area := range areaMap {
		results = append(results, struct {
			Name   string `json:"name"`
			UUID   string `json:"uuid"`
			Brands []struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			} `json:"brands"`
			TotalVisits int `json:"total_visits"`
		}{
			Name:        area.Name,
			UUID:        area.UUID,
			Brands:      area.Brands,
			TotalVisits: area.TotalVisits,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Bar chart data for ND by Area",
		"data":    results,
	})
}

// Bar chart for ND by SubArea with aggregated data - shows ALL sub areas within an area
func NdBarChartSubArea(c *fiber.Ctx) error {
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
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		} `json:"brands"`
		TotalVisits int `json:"total_visits"`
	}

	// Query to get brand data for ALL sub areas within the area (same logic as NdTableViewSubArea)
	sqlQuery := `
		SELECT  
		sub_areas.name AS name,
		sub_areas.uuid AS uuid,
		brands.name AS brand,
		COUNT(pos_form_items.uuid) AS presence,
		(SELECT COUNT(pos_forms.uuid) FROM pos_forms 
			WHERE pos_forms.country_uuid = ? 
			AND pos_forms.province_uuid = ? 
			AND pos_forms.area_uuid = ?
			AND pos_forms.sub_area_uuid = sub_areas.uuid
			AND pos_forms.created_at BETWEEN ? AND ?
			AND pos_forms.deleted_at IS NULL
		) AS visits,
		(COUNT(pos_form_items.uuid) * 100.0 / (
		SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ?
		AND pos_forms.sub_area_uuid = sub_areas.uuid
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		)) AS pourcent
		FROM pos_form_items 
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid 
		INNER JOIN sub_areas ON pos_forms.sub_area_uuid = sub_areas.uuid 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY sub_areas.name, sub_areas.uuid, brands.name
		ORDER BY sub_areas.name, pourcent DESC;
	`

	rows, err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}
	defer rows.Close()

	// Map to store sub area data
	subAreaMap := make(map[string]struct {
		Name        string
		UUID        string
		TotalVisits int
		Brands      []struct {
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		}
	})

	// Process the data
	for rows.Next() {
		var name, uuid, brand string
		var presence, visits int
		var pourcent float64
		if err := rows.Scan(&name, &uuid, &brand, &presence, &visits, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan data",
				"error":   err.Error(),
			})
		}

		// Initialize sub area if not exists
		if subArea, exists := subAreaMap[name]; !exists {
			subAreaMap[name] = struct {
				Name        string
				UUID        string
				TotalVisits int
				Brands      []struct {
					Brand    string  `json:"brand"`
					Presence int     `json:"presence"`
					Pourcent float64 `json:"pourcent"`
				}
			}{
				Name:        name,
				UUID:        uuid,
				TotalVisits: visits,
				Brands: []struct {
					Brand    string  `json:"brand"`
					Presence int     `json:"presence"`
					Pourcent float64 `json:"pourcent"`
				}{
					{
						Brand:    brand,
						Presence: presence,
						Pourcent: pourcent,
					},
				},
			}
		} else {
			// Add brand to existing sub area
			subArea.Brands = append(subArea.Brands, struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			}{
				Brand:    brand,
				Presence: presence,
				Pourcent: pourcent,
			})
			subAreaMap[name] = subArea
		}
	}

	// Convert map to slice for response
	for _, subArea := range subAreaMap {
		results = append(results, struct {
			Name   string `json:"name"`
			UUID   string `json:"uuid"`
			Brands []struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			} `json:"brands"`
			TotalVisits int `json:"total_visits"`
		}{
			Name:        subArea.Name,
			UUID:        subArea.UUID,
			Brands:      subArea.Brands,
			TotalVisits: subArea.TotalVisits,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Bar chart data for ND by SubArea",
		"data":    results,
	})
}

// Bar chart for ND by Commune with aggregated data - shows ALL communes within a sub area
func NdBarChartCommune(c *fiber.Ctx) error {
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
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		} `json:"brands"`
		TotalVisits int `json:"total_visits"`
	}

	// Query to get brand data for ALL communes within the sub area (same logic as NdTableViewCommune)
	sqlQuery := `
		SELECT  
		communes.name AS name,
		communes.uuid AS uuid,
		brands.name AS brand,
		COUNT(pos_form_items.uuid) AS presence,
		(SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ? 
		AND pos_forms.sub_area_uuid = ?
		AND pos_forms.commune_uuid = communes.uuid
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		) AS visits,
		(COUNT(pos_form_items.uuid) * 100.0 / (
		SELECT COUNT(pos_forms.uuid) FROM pos_forms 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ? 
		AND pos_forms.sub_area_uuid = ?
		AND pos_forms.commune_uuid = communes.uuid
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		)) AS pourcent
		FROM pos_form_items 
		INNER JOIN pos_forms ON pos_form_items.pos_form_uuid = pos_forms.uuid
		INNER JOIN brands ON pos_form_items.brand_uuid = brands.uuid 
		INNER JOIN communes ON pos_forms.commune_uuid = communes.uuid 
		WHERE pos_forms.country_uuid = ? 
		AND pos_forms.province_uuid = ? 
		AND pos_forms.area_uuid = ? 
		AND pos_forms.sub_area_uuid = ?
		AND pos_forms.created_at BETWEEN ? AND ?
		AND pos_forms.deleted_at IS NULL
		GROUP BY communes.name, communes.uuid, brands.name
		ORDER BY communes.name, pourcent DESC;
	`

	rows, err := db.Raw(sqlQuery,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date,
		country_uuid, province_uuid, area_uuid, sub_area_uuid, start_date, end_date).Rows()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Failed to fetch data",
			"error":   err.Error(),
		})
	}
	defer rows.Close()

	// Map to store commune data
	communeMap := make(map[string]struct {
		Name        string
		UUID        string
		TotalVisits int
		Brands      []struct {
			Brand    string  `json:"brand"`
			Presence int     `json:"presence"`
			Pourcent float64 `json:"pourcent"`
		}
	})

	// Process the data
	for rows.Next() {
		var name, uuid, brand string
		var presence, visits int
		var pourcent float64
		if err := rows.Scan(&name, &uuid, &brand, &presence, &visits, &pourcent); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to scan data",
				"error":   err.Error(),
			})
		}

		// Initialize commune if not exists
		if commune, exists := communeMap[name]; !exists {
			communeMap[name] = struct {
				Name        string
				UUID        string
				TotalVisits int
				Brands      []struct {
					Brand    string  `json:"brand"`
					Presence int     `json:"presence"`
					Pourcent float64 `json:"pourcent"`
				}
			}{
				Name:        name,
				UUID:        uuid,
				TotalVisits: visits,
				Brands: []struct {
					Brand    string  `json:"brand"`
					Presence int     `json:"presence"`
					Pourcent float64 `json:"pourcent"`
				}{
					{
						Brand:    brand,
						Presence: presence,
						Pourcent: pourcent,
					},
				},
			}
		} else {
			// Add brand to existing commune
			commune.Brands = append(commune.Brands, struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			}{
				Brand:    brand,
				Presence: presence,
				Pourcent: pourcent,
			})
			communeMap[name] = commune
		}
	}

	// Convert map to slice for response
	for _, commune := range communeMap {
		results = append(results, struct {
			Name   string `json:"name"`
			UUID   string `json:"uuid"`
			Brands []struct {
				Brand    string  `json:"brand"`
				Presence int     `json:"presence"`
				Pourcent float64 `json:"pourcent"`
			} `json:"brands"`
			TotalVisits int `json:"total_visits"`
		}{
			Name:        commune.Name,
			UUID:        commune.UUID,
			Brands:      commune.Brands,
			TotalVisits: commune.TotalVisits,
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Bar chart data for ND by Commune",
		"data":    results,
	})
}

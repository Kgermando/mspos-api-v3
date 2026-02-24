package dashboard

import (
	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
)

func GoogleMaps(c *fiber.Ctx) error {
	db := database.DB

	start_date := c.Query("start_date")
	end_date := c.Query("end_date")
	search := c.Query("search")               // recherche par nom d'utilisateur
	province_uuid := c.Query("province_uuid") // filtre par province (menu déroulant)
	user_type := c.Query("user_type")         // filtre par type : asm | supervisor | dr | cyclo

	var results []struct {
		Latitude     float64 `json:"latitude"`  // Latitude du marqueur
		Longitude    float64 `json:"longitude"` // Longitude du marqueur
		Signature    string  `json:"signature"`
		PosName      string  `json:"pos_name"`      // Nom du POS
		PosUUID      string  `json:"pos_uuid"`      // UUID du POS
		Postype      string  `json:"postype"`       // Type de POS
		Asm          string  `json:"asm"`           // Nom de l'ASM
		Sup          string  `json:"sup"`           // Nom du Superviseur
		Dr           string  `json:"dr"`            // Nom du DR
		Cyclo        string  `json:"cyclo"`         // Nom du Cyclo
		Role         string  `json:"role"`          // asm | supervisor | dr | cyclo
		ProvinceUUID string  `json:"province_uuid"` // UUID de la province
		ProvinceName string  `json:"province_name"` // Nom de la province
		CreatedAt    string  `json:"created_at"`    // Date de création du formulaire
	}

	query := db.Table("pos_forms").
		Joins("JOIN pos ON pos.uuid = pos_forms.pos_uuid").
		Joins("LEFT JOIN provinces ON provinces.uuid = pos_forms.province_uuid").
		Select(`
			pos_forms.latitude AS latitude,
			pos_forms.longitude AS longitude,
			pos_forms.signature AS signature,
			pos_forms.created_at AS created_at,
			pos_forms.province_uuid AS province_uuid,
			COALESCE(provinces.name, '') AS province_name,
			pos.name AS pos_name,
			pos.uuid AS pos_uuid,
			pos.postype AS postype,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				ELSE pos_forms.asm 
			END AS asm,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN '' 
				ELSE pos_forms.sup 
			END AS sup,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				WHEN pos_forms.signature = pos_forms.sup THEN '' 
				ELSE pos_forms.dr 
			END AS dr,
			CASE 
				WHEN pos_forms.signature = pos_forms.asm THEN ''
				WHEN pos_forms.signature = pos_forms.sup THEN ''
				WHEN pos_forms.signature = pos_forms.dr THEN '' 
				ELSE pos_forms.cyclo 
			END AS cyclo,
			CASE
				WHEN pos_forms.signature = pos_forms.asm  THEN 'asm'
				WHEN pos_forms.signature = pos_forms.sup  THEN 'supervisor'
				WHEN pos_forms.signature = pos_forms.dr   THEN 'dr'
				WHEN pos_forms.signature = pos_forms.cyclo THEN 'cyclo'
				ELSE 'unknown'
			END AS role
		`).
		Where("pos_forms.created_at BETWEEN ? AND ?", start_date, end_date).
		Where("pos_forms.deleted_at IS NULL")

	// Filtre par province (menu déroulant)
	if province_uuid != "" {
		query = query.Where("pos_forms.province_uuid = ?", province_uuid)
	}

	// Barre de recherche : filtre par nom d'utilisateur (tous rôles confondus)
	if search != "" {
		query = query.Where(`
			LOWER(pos_forms.asm) LIKE LOWER(?) OR 
			LOWER(pos_forms.sup) LIKE LOWER(?) OR 
			LOWER(pos_forms.dr) LIKE LOWER(?) OR 
			LOWER(pos_forms.cyclo) LIKE LOWER(?)
		`, "%"+search+"%", "%"+search+"%", "%"+search+"%", "%"+search+"%")
	}

	// Filtre par type d'utilisateur pour n'afficher qu'une catégorie de marqueurs
	switch user_type {
	case "asm":
		query = query.Where("pos_forms.signature = pos_forms.asm")
	case "supervisor":
		query = query.Where("pos_forms.signature = pos_forms.sup")
	case "dr":
		query = query.Where(
			"pos_forms.signature = pos_forms.dr AND pos_forms.signature != pos_forms.asm AND pos_forms.signature != pos_forms.sup",
		)
	case "cyclo":
		query = query.Where(
			"pos_forms.signature = pos_forms.cyclo AND pos_forms.signature != pos_forms.asm AND pos_forms.signature != pos_forms.sup AND pos_forms.signature != pos_forms.dr",
		)
	}

	err := query.Scan(&results).Error
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

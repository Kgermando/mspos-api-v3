package utils

import (
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// ApplyGeographicFilters applique les filtres géographiques communs pour pos et pos_forms
// tableName: "pos" ou "pos_forms" selon le contexte
func ApplyGeographicFilters(query *gorm.DB, c *fiber.Ctx, tableName string) *gorm.DB {
	// Filtres géographiques des dropdowns dépendants les uns des autres
	country := c.Query("country", "")
	province := c.Query("province", "")
	area := c.Query("area", "")
	subarea := c.Query("subarea", "")
	commune := c.Query("commune", "")

	// 🌍 Filtres géographiques en cascade (dépendants les uns des autres)
	if country != "" {
		query = query.Where("countries.name ILIKE ?", "%"+country+"%")
	}
	if province != "" {
		query = query.Where("provinces.name ILIKE ?", "%"+province+"%")
	}
	if area != "" {
		query = query.Where("areas.name ILIKE ?", "%"+area+"%")
	}
	if subarea != "" {
		query = query.Where("sub_areas.name ILIKE ?", "%"+subarea+"%")
	}
	if commune != "" {
		query = query.Where("communes.name ILIKE ?", "%"+commune+"%")
	}

	return query
}

// ApplyAgentFilters applique les filtres de hiérarchie commerciale (ASM, Supervisor, DR, Cyclo)
// tableName: "pos" ou "pos_forms" selon le contexte
func ApplyAgentFilters(query *gorm.DB, c *fiber.Ctx, tableName string) *gorm.DB {
	// Filtre pour les niveaux (ASM, Supervisor, DR, Cyclo) avec support de recherche intégrée
	agent := c.Query("agent", "")

	// 👤 Filtre pour les agents (recherche dans ASM, Supervisor, DR, Cyclo)
	if agent != "" {
		query = query.Where(
			tableName+".asm ILIKE ? OR "+tableName+".sup ILIKE ? OR "+tableName+".dr ILIKE ? OR "+tableName+".cyclo ILIKE ?",
			"%"+agent+"%", "%"+agent+"%", "%"+agent+"%", "%"+agent+"%",
		)
	}

	return query
}

// ApplySearchFilter applique un filtre de recherche générale
// tableName: "pos" ou "pos_forms" selon le contexte
// searchFields: liste des champs dans lesquels effectuer la recherche
func ApplySearchFilter(query *gorm.DB, c *fiber.Ctx, tableName string, searchFields []string) *gorm.DB {
	search := c.Query("search", "")

	if search != "" && len(searchFields) > 0 {
		// Construire la condition WHERE pour tous les champs de recherche
		whereClause := ""
		args := []interface{}{}

		for i, field := range searchFields {
			if i > 0 {
				whereClause += " OR "
			}
			whereClause += tableName + "." + field + " ILIKE ?"
			args = append(args, "%"+search+"%")
		}

		query = query.Where(whereClause, args...)
	}

	return query
}

// ApplyCommonFilters applique tous les filtres communs (géographiques + agents + recherche)
// C'est une fonction de commodité qui combine tous les filtres
func ApplyCommonFilters(query *gorm.DB, c *fiber.Ctx, tableName string, searchFields []string) *gorm.DB {
	// Appliquer les filtres géographiques
	query = ApplyGeographicFilters(query, c, tableName)

	// Appliquer les filtres d'agents
	query = ApplyAgentFilters(query, c, tableName)

	// Appliquer le filtre de recherche
	query = ApplySearchFilter(query, c, tableName, searchFields)

	return query
}

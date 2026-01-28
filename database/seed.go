package database

import (
	"fmt"

	"github.com/danny19977/mspos-api-v3/models"
	"github.com/google/uuid"
)

// InitializeSupportUser crée automatiquement un utilisateur Support s'il n'existe pas
func InitializeSupportUser() {
	var existingUser models.User
	result := DB.Where("role = ?", "Support").First(&existingUser)

	// Si aucun utilisateur Support n'existe, on le crée
	if result.Error != nil {
		supportUser := &models.User{
			UUID:         uuid.New().String(),
			Fullname:     "Support Admin",
			Email:        "support@mspos.com",
			Phone:        "+243000000000",
			Title:        "Support Administrator",
			Role:         "Support",
			Permission:   "all",
			Status:       true,
			CountryUUID:  "",
			ProvinceUUID: "",
			AreaUUID:     "",
			SubAreaUUID:  "",
			CommuneUUID:  "",
		}

		// Définir le mot de passe
		supportUser.SetPassword("Support@2026")

		// Enregistrer dans la base de données
		if err := DB.Create(supportUser).Error; err != nil {
			fmt.Println("⚠️ Erreur lors de la création de l'utilisateur Support:", err)
		} else {
			fmt.Println("✅ Utilisateur Support créé avec succès!")
			fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			fmt.Printf("   Email: %s\n", supportUser.Email)
			fmt.Printf("   Téléphone: %s\n", supportUser.Phone)
			fmt.Printf("   Mot de passe: Support@2026\n")
			fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		}
	} else {
		fmt.Println("ℹ️ Utilisateur Support déjà existant")
	}
}

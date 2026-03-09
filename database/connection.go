package database

import (
	"fmt"
	"log"
	"strconv"

	"github.com/danny19977/mspos-api-v3/models"
	"github.com/danny19977/mspos-api-v3/utils"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

func Connect() {
	p := utils.Env("DB_PORT")
	port, err := strconv.ParseUint(p, 10, 32)
	if err != nil {
		panic("failed to parse database port 😵!")
	}

	DNS := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", utils.Env("DB_HOST"), port, utils.Env("DB_USER"), utils.Env("DB_PASSWORD"), utils.Env("DB_NAME"))
	connection, err := gorm.Open(postgres.Open(DNS), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		panic("Could not connect to the database 😰!")
	}

	DB = connection
	fmt.Println("Database Connected 🎉!")

	migrateModel := func(model interface{}) {
		if err := connection.AutoMigrate(model); err != nil {
			log.Printf("AutoMigrate failed for %T: %v\n", model, err)
		}
	}

	migrateModel(&models.Country{})
	migrateModel(&models.Province{})
	migrateModel(&models.Area{})
	migrateModel(&models.SubArea{})
	migrateModel(&models.Commune{})
	migrateModel(&models.User{})
	migrateModel(&models.UserLogs{})
	migrateModel(&models.Manager{})
	migrateModel(&models.Pos{})
	migrateModel(&models.PosEquipment{})
	migrateModel(&models.PosForm{})
	migrateModel(&models.PosFormItems{})
	migrateModel(&models.RoutePlan{})
	migrateModel(&models.RoutePlanItem{})
	migrateModel(&models.Brand{})

	// Initialiser le premier utilisateur Support s'il n'existe pas
	InitializeSupportUser()
}

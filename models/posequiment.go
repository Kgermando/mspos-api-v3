package models

import (
	"time"

	"gorm.io/gorm"
)

type PosEquipment struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	PosUUID string `json:"pos_uuid" gorm:"type:varchar(255);not null"`
	Pos     Pos    `gorm:"foreignKey:PosUUID;references:UUID"` // Status d'equipements  Casser, Vieux, Bien

	Parasol       string `json:"parasol"`                        // drop down brand  create a line "other"
	ParasolStatus string `gorm:"not null" json:"parasol_status"` // Status d'equipements  Casser, Vieux, Bien

	Stand       string `json:"stand"`                        // drope down brand create a line "other"
	StandStatus string `gorm:"not null" json:"stand_status"` // Status d'equipements  Casser, Vieux, Bien

	Kiosk       string `json:"kiosk"`                        //Drope down brand create a line "other"
	KioskStatus string `gorm:"not null" json:"kiosk_status"` // Status d'equipements  Casser, Vieux, Bien

	Signature string `json:"signature"`
}

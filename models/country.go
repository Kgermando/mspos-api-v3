package models

import (
	"time"

	"gorm.io/gorm"
)

type Country struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Name      string `gorm:"not null" json:"name"`
	Signature string `json:"signature"`

	Provinces []Province `gorm:"foreignKey:CountryUUID;references:UUID"`
	Areas     []Area     `gorm:"foreignKey:CountryUUID;references:UUID"`
	SubAreas  []SubArea  `gorm:"foreignKey:CountryUUID;references:UUID"`
	Communes  []Commune  `gorm:"foreignKey:CountryUUID;references:UUID"`

	Managers []Manager `gorm:"foreignKey:CountryUUID;references:UUID"`

	Brands []Brand `gorm:"foreignKey:CountryUUID;references:UUID"`
	// Pos      []Pos     `gorm:"foreignKey:CountryUUID;references:UUID"`
	// PosForms []PosForm `gorm:"foreignKey:CountryUUID;references:UUID"`

	// Users      []User      `gorm:"foreignKey:CountryUUID;references:UUID"`
	// RoutePlans []RoutePlan `gorm:"foreignKey:CountryUUID;references:UUID"`

	TotalUsers int64 `json:"total_users"`
	TotalPos   int64 `json:"total_pos"`
	Visites    int64 `json:"visites"`
}

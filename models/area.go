package models

import (
	// "github.com/lib/pq"
	"time"

	"gorm.io/gorm"
)

type Area struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Name string `gorm:"not null" json:"name"`

	CountryUUID  string   `json:"country_uuid" gorm:"type:varchar(255);not null"`
	Country      Country  `gorm:"foreignKey:CountryUUID;references:UUID"`
	ProvinceUUID string   `json:"province_uuid" gorm:"type:varchar(255);not null"`
	Province     Province `gorm:"foreignKey:ProvinceUUID;references:UUID"`

	Signature string `json:"signature"`

	SubAreas []SubArea `gorm:"foreignKey:AreaUUID;references:UUID"`
	Communes []Commune `gorm:"foreignKey:AreaUUID;references:UUID"`

	// Pos      []Pos     `gorm:"foreignKey:AreaUUID;references:UUID"`
	// PosForms []PosForm `gorm:"foreignKey:AreaUUID;references:UUID"`

	// RoutePlans []RoutePlan `gorm:"foreignKey:AreaUUID;references:UUID"`

	// Users []User `gorm:"foreignKey:AreaUUID;references:UUID"`

	TotalUsers int64 `json:"total_users"`
	TotalPos   int64 `json:"total_pos"`
	Visites    int64 `json:"visites"`
}

package models

import (
	"time"

	"gorm.io/gorm"
)

type PosFormItems struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	PosFormUUID string `json:"posform_uuid" gorm:"type:varchar(255);not null"` // Foreign key (belongs to), tag `index` will create index for this column
	BrandUUID   string `json:"brand_uuid" gorm:"type:varchar(255);not null"`   // Foreign key (belongs to), tag `index` will create index for this column

	NumberFarde float64 `gorm:"not null" json:"number_farde"` // NUMBER Farde
	Counter     int     `gorm:"not null" json:"counter"`      // Allows to calculate the Sum of the ND Dashboard
	Sold        float64 `gorm:"default:0" json:"sold"`        // Sold quantity of the item

	PosForm PosForm `gorm:"foreignKey:PosFormUUID;references:UUID"` // POS Form of the POS
	Brand   Brand   `gorm:"foreignKey:BrandUUID;references:UUID"`   // Brand of the POS

}

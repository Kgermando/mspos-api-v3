package models

import (
	"time"

	"gorm.io/gorm"
)

type PosForm struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Price   int    `gorm:"default:0" json:"price"`
	Comment string `json:"comment"`

	Latitude  float64 `json:"latitude"`  // Latitude of the user
	Longitude float64 `json:"longitude"` // Longitude of the user
	Signature string  `json:"signature"`

	PosUUID string `json:"pos_uuid" gorm:"type:varchar(255);not null;default:''"`
	Pos Pos `gorm:"foreignKey:PosUUID;references:UUID"`

	CountryUUID  string `json:"country_uuid" gorm:"type:varchar(255);not null;default:''"`
	ProvinceUUID string `json:"province_uuid" gorm:"type:varchar(255);not null;default:''"`
	AreaUUID     string `json:"area_uuid" gorm:"type:varchar(255);not null;default:''"`
	SubAreaUUID  string `json:"sub_area_uuid" gorm:"type:varchar(255);not null;default:''"`
	CommuneUUID  string `json:"commune_uuid" gorm:"type:varchar(255);not null;default:''"`

	UserUUID string `json:"user_uuid" gorm:"type:varchar(255);not null;default:''"`
	User     User   `gorm:"foreignKey:UserUUID;references:UUID"`

	AsmUUID   string `json:"asm_uuid" gorm:"type:varchar(255);not null"`
	Asm       string `json:"asm" gorm:"default:''"`
	SupUUID   string `json:"sup_uuid" gorm:"type:varchar(255);not null"`
	Sup       string `json:"sup" gorm:"default:''"`
	DrUUID    string `json:"dr_uuid" gorm:"type:varchar(255);not null"`
	Dr        string `json:"dr" gorm:"default:''"`
	CycloUUID string `json:"cyclo_uuid" gorm:"type:varchar(255);not null"`
	Cyclo     string `json:"cyclo" gorm:"default:''"`

	Country  Country  `gorm:"foreignKey:CountryUUID;references:UUID"`
	Province Province `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	Area     Area     `gorm:"foreignKey:AreaUUID;references:UUID"`
	SubArea  SubArea  `gorm:"foreignKey:SubAreaUUID;references:UUID"`
	Commune  Commune  `gorm:"foreignKey:CommuneUUID;references:UUID"`

 

	Sync bool `json:"sync" gorm:"default:false"`

	PosFormItems []PosFormItems `gorm:"foreignKey:PosFormUUID;references:UUID"`
}

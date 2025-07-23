package models

type Team struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	Fullname string `gorm:"not null;default:''" json:"fullname"`

	// CountryUUID  string `json:"country_uuid" gorm:"type:varchar(255);not null;default:''"`
	// ProvinceUUID string `json:"province_uuid" gorm:"type:varchar(255);not null;default:''"`
	// AreaUUID     string `json:"area_uuid" gorm:"type:varchar(255);not null;default:''"`
	// SubAreaUUID  string `json:"sub_area_uuid" gorm:"type:varchar(255);not null;default:''"`
	// CommuneUUID  string `json:"commune_uuid" gorm:"type:varchar(255);not null;default:''"`

	// Country  Country  `gorm:"foreignKey:CountryUUID;references:UUID"`
	// Province Province `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	// Area     Area     `gorm:"foreignKey:AreaUUID;references:UUID"`
	// SubArea  SubArea  `gorm:"foreignKey:SubAreaUUID;references:UUID"`
	// Commune  Commune  `gorm:"foreignKey:CommuneUUID;references:UUID"`

	AsmUUID string `json:"asm_uuid" gorm:"type:varchar(255);not null"`
	Asm     string `json:"asm" gorm:"default:''"`

	TotalSup   int64 `json:"total_sup"`
	TotalDr    int64 `json:"total_dr"`
	TotalCyclo int64 `json:"total_cyclo"`
	TotalPos   int64 `json:"total_pos"`
	Visites    int64 `json:"visites"`
}

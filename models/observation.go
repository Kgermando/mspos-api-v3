package models

import "time"

// ObservationResponse is data transfer object (DTO) used to return
// observations (non-empty comments) from visit forms.
// It is NOT a DB table — it is built from PosForm records.
type ObservationResponse struct {
	UUID      string    `json:"uuid"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Observation text
	Comment string `json:"comment"`

	// POS info
	PosUUID string `json:"pos_uuid"`
	PosName string `json:"pos_name"`

	// Territory hierarchy
	CountryUUID  string `json:"country_uuid"`
	CountryName  string `json:"country_name"`
	ProvinceUUID string `json:"province_uuid"`
	ProvinceName string `json:"province_name"`
	AreaUUID     string `json:"area_uuid"`
	AreaName     string `json:"area_name"`
	SubAreaUUID  string `json:"sub_area_uuid"`
	SubAreaName  string `json:"sub_area_name"`
	CommuneUUID  string `json:"commune_uuid"`
	CommuneName  string `json:"commune_name"`

	// Agent hierarchy
	AsmUUID   string `json:"asm_uuid"`
	Asm       string `json:"asm"`
	SupUUID   string `json:"sup_uuid"`
	Sup       string `json:"sup"`
	DrUUID    string `json:"dr_uuid"`
	Dr        string `json:"dr"`
	CycloUUID string `json:"cyclo_uuid"`
	Cyclo     string `json:"cyclo"`

	// Visit author
	UserUUID string `json:"user_uuid"`
	UserName string `json:"user_fullname"`
	UserRole string `json:"user_role"`
}

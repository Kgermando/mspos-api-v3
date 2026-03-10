package dashboard

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/danny19977/mspos-api-v3/database"
	"github.com/gofiber/fiber/v2"
	"github.com/xuri/excelize/v2"
)

// ═══════════════════════════════════════════════════════════════════════════
// KPI EXCEL EXPORT — rapport complet multi-onglets
// GET /dashboard/kpi/export-excel
// Query params: country_uuid, province_uuid, area_uuid, sub_area_uuid,
//               commune_uuid, start_date, end_date, title
// ═══════════════════════════════════════════════════════════════════════════

// ExportKPIExcel génère un rapport Excel complet avec tous les KPIs
func ExportKPIExcel(c *fiber.Ctx) error {
	db := database.DB

	// ── paramètres ────────────────────────────────────────────────────────────
	countryUUID := c.Query("country_uuid")
	provinceUUID := c.Query("province_uuid")
	areaUUID := c.Query("area_uuid")
	subAreaUUID := c.Query("sub_area_uuid")
	communeUUID := c.Query("commune_uuid")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	titleFilter := c.Query("title")

	// Valeurs par défaut
	now := time.Now()
	if startDate == "" {
		startDate = now.AddDate(0, 0, -30).Format("2006-01-02")
	}
	if endDate == "" {
		endDate = now.Format("2006-01-02")
	}
	start, _ := time.Parse("2006-01-02", startDate)
	end, _ := time.Parse("2006-01-02", endDate)
	days := int(end.Sub(start).Hours()/24) + 1

	// ── fichier Excel ─────────────────────────────────────────────────────────
	f := excelize.NewFile()
	defer f.Close()

	// ── styles ────────────────────────────────────────────────────────────────
	titleStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true, Size: 16, Color: "1F4E79", Family: "Calibri"}, Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"D6E4F0"}, Pattern: 1}})
	headerStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true, Size: 11, Color: "FFFFFF", Family: "Calibri"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"2E75B6"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center", WrapText: true}, Border: []excelize.Border{{Type: "left", Color: "FFFFFF", Style: 1}, {Type: "right", Color: "FFFFFF", Style: 1}, {Type: "top", Color: "FFFFFF", Style: 1}, {Type: "bottom", Color: "FFFFFF", Style: 1}}})
	subHeaderStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true, Size: 10, Color: "FFFFFF", Family: "Calibri"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"5B9BD5"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"}})
	dataStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri"}, Alignment: &excelize.Alignment{Horizontal: "left", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}})
	numStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri"}, Alignment: &excelize.Alignment{Horizontal: "right", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}, NumFmt: 4})
	pctStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri"}, Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}})
	greenStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri", Bold: true, Color: "00703C"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"E2EFDA"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}})
	orangeStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri", Bold: true, Color: "9C5700"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"FFEB9C"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}})
	redStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri", Bold: true, Color: "9C0006"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"FFC7CE"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}})
	altRowStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"F2F7FB"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "left", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}})
	altNumStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"F2F7FB"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "right", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}, NumFmt: 4})
	altPctStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 10, Family: "Calibri"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"F2F7FB"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "BDD7EE", Style: 1}, {Type: "right", Color: "BDD7EE", Style: 1}, {Type: "top", Color: "BDD7EE", Style: 1}, {Type: "bottom", Color: "BDD7EE", Style: 1}}})
	infoStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true, Size: 11, Color: "2E75B6", Family: "Calibri"}, Alignment: &excelize.Alignment{Horizontal: "left", Vertical: "center"}})
	infoValStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Size: 11, Family: "Calibri"}, Alignment: &excelize.Alignment{Horizontal: "left", Vertical: "center"}})
	boldTotalStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true, Size: 10, Family: "Calibri", Color: "1F4E79"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"D6E4F0"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "right", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "2E75B6", Style: 2}, {Type: "right", Color: "2E75B6", Style: 2}, {Type: "top", Color: "2E75B6", Style: 2}, {Type: "bottom", Color: "2E75B6", Style: 2}}})
	boldTotalLblStyle, _ := f.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true, Size: 10, Family: "Calibri", Color: "1F4E79"}, Fill: excelize.Fill{Type: "pattern", Color: []string{"D6E4F0"}, Pattern: 1}, Alignment: &excelize.Alignment{Horizontal: "left", Vertical: "center"}, Border: []excelize.Border{{Type: "left", Color: "2E75B6", Style: 2}, {Type: "right", Color: "2E75B6", Style: 2}, {Type: "top", Color: "2E75B6", Style: 2}, {Type: "bottom", Color: "2E75B6", Style: 2}}})

	styles := map[string]int{
		"title": titleStyle, "header": headerStyle, "subHeader": subHeaderStyle,
		"data": dataStyle, "num": numStyle, "pct": pctStyle,
		"green": greenStyle, "orange": orangeStyle, "red": redStyle,
		"altRow": altRowStyle, "altNum": altNumStyle, "altPct": altPctStyle,
		"info": infoStyle, "infoVal": infoValStyle,
		"boldTotal": boldTotalStyle, "boldTotalLbl": boldTotalLblStyle,
	}

	// ═══════════════════════════════════════════════════════════════════════
	// ONGLET 1 — Résumé exécutif
	// ═══════════════════════════════════════════════════════════════════════
	sheetSummary := "Résumé Exécutif"
	f.SetSheetName("Sheet1", sheetSummary)

	addSheetTitle(f, sheetSummary, "RAPPORT KPI — RÉSUMÉ EXÉCUTIF", "A1", "F1", styles["title"])
	f.SetRowHeight(sheetSummary, 1, 35)

	row := 3
	setInfoRow(f, sheetSummary, row, "Période :", fmt.Sprintf("%s → %s (%d jours)", start.Format("02/01/2006"), end.Format("02/01/2006"), days), styles)
	row++
	setInfoRow(f, sheetSummary, row, "Généré le :", now.Format("02/01/2006 15:04:05"), styles)
	row += 2

	// KPIs globaux
	type GlobalKPI struct {
		TotalVisits  int64
		TotalAgents  int64
		TotalPOS     int64
		UniquePOS    int64
		SyncRate     float64
		AvgVisitsDay float64
	}

	var gkpi GlobalKPI
	gkpiQuery := db.Table("pos_forms pf").
		Joins("LEFT JOIN users u ON pf.user_uuid = u.uuid").
		Where("pf.created_at BETWEEN ? AND ?", start, end).
		Where("pf.deleted_at IS NULL").
		Select(`
			COUNT(DISTINCT pf.uuid) AS total_visits,
			COUNT(DISTINCT pf.user_uuid) AS total_agents,
			COUNT(DISTINCT pf.pos_uuid) AS unique_pos,
			ROUND(100.0 * COUNT(CASE WHEN pf.sync = true THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS sync_rate
		`)

	if countryUUID != "" {
		gkpiQuery = gkpiQuery.Where("pf.country_uuid = ?", countryUUID)
	}
	if provinceUUID != "" {
		gkpiQuery = gkpiQuery.Where("pf.province_uuid = ?", provinceUUID)
	}
	if areaUUID != "" {
		gkpiQuery = gkpiQuery.Where("pf.area_uuid = ?", areaUUID)
	}
	if subAreaUUID != "" {
		gkpiQuery = gkpiQuery.Where("pf.sub_area_uuid = ?", subAreaUUID)
	}
	if communeUUID != "" {
		gkpiQuery = gkpiQuery.Where("pf.commune_uuid = ?", communeUUID)
	}
	if titleFilter != "" {
		gkpiQuery = gkpiQuery.Where("u.title = ?", titleFilter)
	}
	gkpiQuery.Scan(&gkpi)

	if days > 0 {
		gkpi.AvgVisitsDay = math.Round(float64(gkpi.TotalVisits)/float64(days)*100) / 100
	}

	summaryKPIs := [][]interface{}{
		{"Indicateur", "Valeur"},
		{"Total Visites (période)", gkpi.TotalVisits},
		{"Agents Actifs", gkpi.TotalAgents},
		{"POS Uniques Visités", gkpi.UniquePOS},
		{"Taux de Sync (%)", fmt.Sprintf("%.2f%%", gkpi.SyncRate)},
		{"Moyenne Visites/Jour", fmt.Sprintf("%.2f", gkpi.AvgVisitsDay)},
		{"Durée (jours)", days},
	}

	f.MergeCell(sheetSummary, fmt.Sprintf("A%d", row), fmt.Sprintf("B%d", row))
	setCell(f, sheetSummary, fmt.Sprintf("A%d", row), "INDICATEURS GLOBAUX", styles["header"])
	f.SetColWidth(sheetSummary, "A", "A", 35)
	f.SetColWidth(sheetSummary, "B", "B", 25)
	row++

	for _, kpiRow := range summaryKPIs[1:] {
		setCell(f, sheetSummary, fmt.Sprintf("A%d", row), kpiRow[0], styles["infoVal"])
		setCell(f, sheetSummary, fmt.Sprintf("B%d", row), kpiRow[1], styles["infoVal"])
		row++
	}

	// ═══════════════════════════════════════════════════════════════════════
	// ONGLET 2 — Performance par Agent
	// ═══════════════════════════════════════════════════════════════════════
	sheetAgent := "Performance Agents"
	f.NewSheet(sheetAgent)

	addSheetTitle(f, sheetAgent, "PERFORMANCE PAR AGENT", "A1", "M1", styles["title"])
	f.SetRowHeight(sheetAgent, 1, 35)
	f.SetRowHeight(sheetAgent, 2, 40)

	agentHeaders := []string{
		"N°", "Agent", "Titre", "Province", "Area", "Sub-Area", "Commune",
		"Visites (Période)", "Objectif (Période)", "% Réalisation",
		"Visites Jour", "Objectif Jour", "% Jour",
		"Visites Mois", "Objectif Mois", "% Mois",
		"Statut",
	}
	agentCols := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"}
	agentColWidths := []float64{5, 30, 15, 20, 20, 20, 20, 18, 18, 15, 14, 14, 10, 14, 14, 10, 15}

	writeHeaderRow(f, sheetAgent, 2, agentHeaders, agentCols, agentColWidths, styles["header"])

	type AgentKPI struct {
		UserUUID      string  `json:"user_uuid"`
		Fullname      string  `json:"fullname"`
		Title         string  `json:"title"`
		ProvinceName  string  `json:"province_name"`
		AreaName      string  `json:"area_name"`
		SubAreaName   string  `json:"sub_area_name"`
		CommuneName   string  `json:"commune_name"`
		TotalVisits   int64   `json:"total_visits"`
		RangeTarget   int64   `json:"range_target"`
		RangePct      float64 `json:"range_pct"`
		DailyVisits   int64   `json:"daily_visits"`
		DailyTarget   int64   `json:"daily_target"`
		DailyPct      float64 `json:"daily_pct"`
		MonthlyVisits int64   `json:"monthly_visits"`
		MonthlyTarget int64   `json:"monthly_target"`
		MonthlyPct    float64 `json:"monthly_pct"`
	}

	var agentResults []AgentKPI

	agentQuery := db.Table("pos_forms pf").
		Joins("LEFT JOIN users u ON pf.user_uuid = u.uuid").
		Joins("LEFT JOIN provinces pr ON pf.province_uuid = pr.uuid").
		Joins("LEFT JOIN areas a ON pf.area_uuid = a.uuid").
		Joins("LEFT JOIN sub_areas sa ON pf.sub_area_uuid = sa.uuid").
		Joins("LEFT JOIN communes com ON pf.commune_uuid = com.uuid").
		Where("pf.created_at BETWEEN ? AND ?", start, end).
		Where("pf.deleted_at IS NULL").
		Select(`
			u.uuid AS user_uuid,
			u.fullname,
			u.title,
			pr.name AS province_name,
			a.name AS area_name,
			sa.name AS sub_area_name,
			com.name AS commune_name,
			COUNT(DISTINCT pf.uuid) FILTER (WHERE pf.created_at BETWEEN ?::date AND ?::date) AS total_visits,
			(CASE
				WHEN u.title = 'ASM'           THEN 10
				WHEN u.title = 'Supervisor'    THEN 20
				WHEN u.title IN ('DR','Cyclo') THEN 40
				ELSE 0
			END * (?::int)) AS range_target,
			ROUND(
				COUNT(DISTINCT pf.uuid) FILTER (WHERE pf.created_at BETWEEN ?::date AND ?::date)::numeric
				/ NULLIF(CASE WHEN u.title='ASM' THEN 10 WHEN u.title='Supervisor' THEN 20 WHEN u.title IN('DR','Cyclo') THEN 40 ELSE 1 END * ?::int, 0) * 100
			, 2) AS range_pct,
			COUNT(pf.uuid) FILTER (WHERE DATE(pf.created_at) = CURRENT_DATE) AS daily_visits,
			(CASE WHEN u.title='ASM' THEN 10 WHEN u.title='Supervisor' THEN 20 WHEN u.title IN('DR','Cyclo') THEN 40 ELSE 0 END) AS daily_target,
			ROUND(
				COUNT(pf.uuid) FILTER (WHERE DATE(pf.created_at) = CURRENT_DATE)::numeric
				/ NULLIF(CASE WHEN u.title='ASM' THEN 10 WHEN u.title='Supervisor' THEN 20 WHEN u.title IN('DR','Cyclo') THEN 40 ELSE 1 END, 0) * 100
			, 2) AS daily_pct,
			COUNT(pf.uuid) FILTER (WHERE DATE_TRUNC('month', pf.created_at) = DATE_TRUNC('month', CURRENT_DATE)) AS monthly_visits,
			(CASE WHEN u.title='ASM' THEN 10 WHEN u.title='Supervisor' THEN 20 WHEN u.title IN('DR','Cyclo') THEN 40 ELSE 0 END
			 * EXTRACT(DAY FROM (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day'))::int) AS monthly_target,
			ROUND(
				COUNT(pf.uuid) FILTER (WHERE DATE_TRUNC('month', pf.created_at) = DATE_TRUNC('month', CURRENT_DATE))::numeric
				/ NULLIF(
					(CASE WHEN u.title='ASM' THEN 10 WHEN u.title='Supervisor' THEN 20 WHEN u.title IN('DR','Cyclo') THEN 40 ELSE 1 END)
					* EXTRACT(DAY FROM (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day'))
				, 0) * 100
			, 2) AS monthly_pct
		`, start, end, days, start, end, days)

	if countryUUID != "" {
		agentQuery = agentQuery.Where("pf.country_uuid = ?", countryUUID)
	}
	if provinceUUID != "" {
		agentQuery = agentQuery.Where("pf.province_uuid = ?", provinceUUID)
	}
	if areaUUID != "" {
		agentQuery = agentQuery.Where("pf.area_uuid = ?", areaUUID)
	}
	if subAreaUUID != "" {
		agentQuery = agentQuery.Where("pf.sub_area_uuid = ?", subAreaUUID)
	}
	if communeUUID != "" {
		agentQuery = agentQuery.Where("pf.commune_uuid = ?", communeUUID)
	}
	if titleFilter != "" {
		agentQuery = agentQuery.Where("u.title = ?", titleFilter)
	}

	agentQuery.Group("u.uuid, u.fullname, u.title, pr.name, a.name, sa.name, com.name").
		Order("u.title, u.fullname").
		Scan(&agentResults)

	var totalVisitsPeriod, totalTarget int64
	for idx, ar := range agentResults {
		rowNum := idx + 3
		isAlt := idx%2 == 1
		ds, ns, ps := rowStyles(isAlt, styles)
		pctUsed := ps

		status, sStyle := pctToStatus(ar.RangePct, styles)

		f.SetCellValue(sheetAgent, fmt.Sprintf("A%d", rowNum), idx+1)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("A%d", rowNum), fmt.Sprintf("A%d", rowNum), ns)
		f.SetCellValue(sheetAgent, fmt.Sprintf("B%d", rowNum), ar.Fullname)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("B%d", rowNum), fmt.Sprintf("B%d", rowNum), ds)
		f.SetCellValue(sheetAgent, fmt.Sprintf("C%d", rowNum), ar.Title)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("C%d", rowNum), fmt.Sprintf("C%d", rowNum), ds)
		f.SetCellValue(sheetAgent, fmt.Sprintf("D%d", rowNum), ar.ProvinceName)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("D%d", rowNum), fmt.Sprintf("D%d", rowNum), ds)
		f.SetCellValue(sheetAgent, fmt.Sprintf("E%d", rowNum), ar.AreaName)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("E%d", rowNum), ds)
		f.SetCellValue(sheetAgent, fmt.Sprintf("F%d", rowNum), ar.SubAreaName)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("F%d", rowNum), fmt.Sprintf("F%d", rowNum), ds)
		f.SetCellValue(sheetAgent, fmt.Sprintf("G%d", rowNum), ar.CommuneName)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("G%d", rowNum), fmt.Sprintf("G%d", rowNum), ds)
		f.SetCellValue(sheetAgent, fmt.Sprintf("H%d", rowNum), ar.TotalVisits)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("H%d", rowNum), fmt.Sprintf("H%d", rowNum), ns)
		f.SetCellValue(sheetAgent, fmt.Sprintf("I%d", rowNum), ar.RangeTarget)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("I%d", rowNum), fmt.Sprintf("I%d", rowNum), ns)
		f.SetCellValue(sheetAgent, fmt.Sprintf("J%d", rowNum), fmt.Sprintf("%.2f%%", ar.RangePct))
		f.SetCellStyle(sheetAgent, fmt.Sprintf("J%d", rowNum), fmt.Sprintf("J%d", rowNum), pctUsed)
		f.SetCellValue(sheetAgent, fmt.Sprintf("K%d", rowNum), ar.DailyVisits)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("K%d", rowNum), fmt.Sprintf("K%d", rowNum), ns)
		f.SetCellValue(sheetAgent, fmt.Sprintf("L%d", rowNum), ar.DailyTarget)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("L%d", rowNum), fmt.Sprintf("L%d", rowNum), ns)
		f.SetCellValue(sheetAgent, fmt.Sprintf("M%d", rowNum), fmt.Sprintf("%.2f%%", ar.DailyPct))
		f.SetCellStyle(sheetAgent, fmt.Sprintf("M%d", rowNum), fmt.Sprintf("M%d", rowNum), pctUsed)
		f.SetCellValue(sheetAgent, fmt.Sprintf("N%d", rowNum), ar.MonthlyVisits)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("N%d", rowNum), fmt.Sprintf("N%d", rowNum), ns)
		f.SetCellValue(sheetAgent, fmt.Sprintf("O%d", rowNum), ar.MonthlyTarget)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("O%d", rowNum), fmt.Sprintf("O%d", rowNum), ns)
		f.SetCellValue(sheetAgent, fmt.Sprintf("P%d", rowNum), fmt.Sprintf("%.2f%%", ar.MonthlyPct))
		f.SetCellStyle(sheetAgent, fmt.Sprintf("P%d", rowNum), fmt.Sprintf("P%d", rowNum), pctUsed)
		f.SetCellValue(sheetAgent, fmt.Sprintf("Q%d", rowNum), status)
		f.SetCellStyle(sheetAgent, fmt.Sprintf("Q%d", rowNum), fmt.Sprintf("Q%d", rowNum), sStyle)

		totalVisitsPeriod += ar.TotalVisits
		totalTarget += ar.RangeTarget
	}

	// Ligne total
	totalRow := len(agentResults) + 3
	f.SetCellValue(sheetAgent, fmt.Sprintf("A%d", totalRow), "TOTAL")
	f.SetCellStyle(sheetAgent, fmt.Sprintf("A%d", totalRow), fmt.Sprintf("G%d", totalRow), styles["boldTotalLbl"])
	f.MergeCell(sheetAgent, fmt.Sprintf("A%d", totalRow), fmt.Sprintf("G%d", totalRow))
	f.SetCellValue(sheetAgent, fmt.Sprintf("H%d", totalRow), totalVisitsPeriod)
	f.SetCellStyle(sheetAgent, fmt.Sprintf("H%d", totalRow), fmt.Sprintf("H%d", totalRow), styles["boldTotal"])
	f.SetCellValue(sheetAgent, fmt.Sprintf("I%d", totalRow), totalTarget)
	f.SetCellStyle(sheetAgent, fmt.Sprintf("I%d", totalRow), fmt.Sprintf("I%d", totalRow), styles["boldTotal"])
	if totalTarget > 0 {
		totalPct := float64(totalVisitsPeriod) / float64(totalTarget) * 100
		f.SetCellValue(sheetAgent, fmt.Sprintf("J%d", totalRow), fmt.Sprintf("%.2f%%", totalPct))
	}
	f.SetCellStyle(sheetAgent, fmt.Sprintf("J%d", totalRow), fmt.Sprintf("Q%d", totalRow), styles["boldTotal"])

	// ═══════════════════════════════════════════════════════════════════════
	// ONGLET 3 — Visites par Territoire
	// ═══════════════════════════════════════════════════════════════════════
	sheetTerr := "Visites par Territoire"
	f.NewSheet(sheetTerr)

	addSheetTitle(f, sheetTerr, "VISITES PAR TERRITOIRE", "A1", "H1", styles["title"])
	f.SetRowHeight(sheetTerr, 1, 35)
	f.SetRowHeight(sheetTerr, 2, 40)

	terrHeaders := []string{"N°", "Territoire", "Niveau", "Agent", "Titre", "Visites", "Objectif", "% Réalisation", "Statut"}
	terrCols := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I"}
	terrColWidths := []float64{5, 30, 15, 30, 15, 15, 15, 16, 15}
	writeHeaderRow(f, sheetTerr, 2, terrHeaders, terrCols, terrColWidths, styles["header"])

	type TerrRow struct {
		Name        string `json:"name"`
		Level       string
		Signature   string  `json:"signature"`
		Title       string  `json:"title"`
		TotalVisits int     `json:"total_visits"`
		Target      int     `json:"target"`
		Objectif    float64 `json:"objectif"`
	}

	var terrRows []TerrRow

	// Province
	var pvRes []struct {
		Name      string  `json:"name"`
		Signature string  `json:"signature"`
		Title     string  `json:"title"`
		Visits    int     `json:"total_visits"`
		Target    int     `json:"target"`
		Objectif  float64 `json:"objectif"`
	}
	pvQ := db.Table("pos_forms").
		Select(`
			provinces.name AS name,
			users.fullname AS signature,
			users.title AS title,
			COUNT(pos_forms.uuid) AS total_visits,
			(CASE WHEN users.title='ASM' THEN 10 WHEN users.title='Supervisor' THEN 20 WHEN users.title IN('DR','Cyclo') THEN 40 ELSE 0 END * ?) AS target,
			ROUND(COUNT(pos_forms.uuid)::numeric / NULLIF((CASE WHEN users.title='ASM' THEN 10 WHEN users.title='Supervisor' THEN 20 WHEN users.title IN('DR','Cyclo') THEN 40 ELSE 1 END * ?), 0) * 100, 2) AS objectif
		`, days, days).
		Joins("JOIN users ON users.uuid = pos_forms.user_uuid").
		Joins("JOIN provinces ON provinces.uuid = pos_forms.province_uuid").
		Where("pos_forms.created_at BETWEEN ? AND ?", start, end).
		Where("pos_forms.deleted_at IS NULL")

	if countryUUID != "" {
		pvQ = pvQ.Where("pos_forms.country_uuid = ?", countryUUID)
	}
	if provinceUUID != "" {
		pvQ = pvQ.Where("pos_forms.province_uuid = ?", provinceUUID)
	}
	if titleFilter != "" {
		pvQ = pvQ.Where("users.title = ?", titleFilter)
	}
	pvQ.Group("provinces.name, users.fullname, users.title").Order("provinces.name, users.fullname").Scan(&pvRes)
	for _, r := range pvRes {
		terrRows = append(terrRows, TerrRow{Name: r.Name, Level: "Province", Signature: r.Signature, Title: r.Title, TotalVisits: r.Visits, Target: r.Target, Objectif: r.Objectif})
	}

	// Area
	var arRes []struct {
		Name      string  `json:"name"`
		Signature string  `json:"signature"`
		Title     string  `json:"title"`
		Visits    int     `json:"total_visits"`
		Target    int     `json:"target"`
		Objectif  float64 `json:"objectif"`
	}
	arQ := db.Table("pos_forms").
		Select(`
			areas.name AS name,
			users.fullname AS signature,
			users.title AS title,
			COUNT(pos_forms.uuid) AS total_visits,
			(CASE WHEN users.title='ASM' THEN 10 WHEN users.title='Supervisor' THEN 20 WHEN users.title IN('DR','Cyclo') THEN 40 ELSE 0 END * ?) AS target,
			ROUND(COUNT(pos_forms.uuid)::numeric / NULLIF((CASE WHEN users.title='ASM' THEN 10 WHEN users.title='Supervisor' THEN 20 WHEN users.title IN('DR','Cyclo') THEN 40 ELSE 1 END * ?), 0) * 100, 2) AS objectif
		`, days, days).
		Joins("JOIN users ON users.uuid = pos_forms.user_uuid").
		Joins("JOIN areas ON areas.uuid = pos_forms.area_uuid").
		Where("pos_forms.created_at BETWEEN ? AND ?", start, end).
		Where("pos_forms.deleted_at IS NULL")

	if countryUUID != "" {
		arQ = arQ.Where("pos_forms.country_uuid = ?", countryUUID)
	}
	if areaUUID != "" {
		arQ = arQ.Where("pos_forms.area_uuid = ?", areaUUID)
	}
	if titleFilter != "" {
		arQ = arQ.Where("users.title = ?", titleFilter)
	}
	arQ.Group("areas.name, users.fullname, users.title").Order("areas.name, users.fullname").Scan(&arRes)
	for _, r := range arRes {
		terrRows = append(terrRows, TerrRow{Name: r.Name, Level: "Area", Signature: r.Signature, Title: r.Title, TotalVisits: r.Visits, Target: r.Target, Objectif: r.Objectif})
	}

	for idx, tr := range terrRows {
		rowNum := idx + 3
		isAlt := idx%2 == 1
		ds, ns, ps := rowStyles(isAlt, styles)
		status, sStyle := pctToStatus(tr.Objectif, styles)
		f.SetCellValue(sheetTerr, fmt.Sprintf("A%d", rowNum), idx+1)
		f.SetCellStyle(sheetTerr, fmt.Sprintf("A%d", rowNum), fmt.Sprintf("A%d", rowNum), ns)
		f.SetCellValue(sheetTerr, fmt.Sprintf("B%d", rowNum), tr.Name)
		f.SetCellStyle(sheetTerr, fmt.Sprintf("B%d", rowNum), fmt.Sprintf("B%d", rowNum), ds)
		f.SetCellValue(sheetTerr, fmt.Sprintf("C%d", rowNum), tr.Level)
		f.SetCellStyle(sheetTerr, fmt.Sprintf("C%d", rowNum), fmt.Sprintf("C%d", rowNum), ds)
		f.SetCellValue(sheetTerr, fmt.Sprintf("D%d", rowNum), tr.Signature)
		f.SetCellStyle(sheetTerr, fmt.Sprintf("D%d", rowNum), fmt.Sprintf("D%d", rowNum), ds)
		f.SetCellValue(sheetTerr, fmt.Sprintf("E%d", rowNum), tr.Title)
		f.SetCellStyle(sheetTerr, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("E%d", rowNum), ds)
		f.SetCellValue(sheetTerr, fmt.Sprintf("F%d", rowNum), tr.TotalVisits)
		f.SetCellStyle(sheetTerr, fmt.Sprintf("F%d", rowNum), fmt.Sprintf("F%d", rowNum), ns)
		f.SetCellValue(sheetTerr, fmt.Sprintf("G%d", rowNum), tr.Target)
		f.SetCellStyle(sheetTerr, fmt.Sprintf("G%d", rowNum), fmt.Sprintf("G%d", rowNum), ns)
		f.SetCellValue(sheetTerr, fmt.Sprintf("H%d", rowNum), fmt.Sprintf("%.2f%%", tr.Objectif))
		f.SetCellStyle(sheetTerr, fmt.Sprintf("H%d", rowNum), fmt.Sprintf("H%d", rowNum), ps)
		f.SetCellValue(sheetTerr, fmt.Sprintf("I%d", rowNum), status)
		f.SetCellStyle(sheetTerr, fmt.Sprintf("I%d", rowNum), fmt.Sprintf("I%d", rowNum), sStyle)
	}

	// ═══════════════════════════════════════════════════════════════════════
	// ONGLET 4 — Analyse des Absences
	// ═══════════════════════════════════════════════════════════════════════
	sheetAbs := "Analyse Absences"
	f.NewSheet(sheetAbs)

	addSheetTitle(f, sheetAbs, "ANALYSE DES ABSENCES — AGENTS INACTIFS", "A1", "F1", styles["title"])
	f.SetRowHeight(sheetAbs, 1, 35)
	f.SetRowHeight(sheetAbs, 2, 40)

	absHeaders := []string{"N°", "Agent", "Titre", "Dernière Visite", "Jours Inactif", "Niveau d'Alerte"}
	absCols := []string{"A", "B", "C", "D", "E", "F"}
	absColWidths := []float64{5, 35, 15, 22, 15, 20}
	writeHeaderRow(f, sheetAbs, 2, absHeaders, absCols, absColWidths, styles["header"])

	type AbsRow struct {
		AgentUUID    string     `json:"agent_uuid"`
		AgentName    string     `json:"agent_name"`
		AgentTitle   string     `json:"agent_title"`
		LastVisit    *time.Time `json:"last_visit"`
		DaysInactive int64      `json:"days_inactive"`
		AlertLevel   string     `json:"alert_level"`
	}

	var absResults []AbsRow
	absQuery := db.Table("users u").
		Joins("LEFT JOIN pos_forms pf ON u.uuid = pf.user_uuid AND pf.deleted_at IS NULL").
		Where("u.title IN ?", []string{"ASM", "Supervisor", "DR", "Cyclo"}).
		Where("u.status = true").
		Select(`
			u.uuid AS agent_uuid,
			u.fullname AS agent_name,
			u.title AS agent_title,
			MAX(pf.created_at) AS last_visit,
			COALESCE(ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(pf.created_at))) / 86400)::BIGINT, 999) AS days_inactive
		`).
		Group("u.uuid, u.fullname, u.title").
		Order("days_inactive DESC")

	if countryUUID != "" {
		absQuery = absQuery.Where("u.country_uuid = ?", countryUUID)
	}
	absQuery.Scan(&absResults)

	for i := range absResults {
		switch {
		case absResults[i].DaysInactive >= 999:
			absResults[i].AlertLevel = "JAMAIS VISITÉ"
		case absResults[i].DaysInactive > 14:
			absResults[i].AlertLevel = "CRITIQUE"
		case absResults[i].DaysInactive > 7:
			absResults[i].AlertLevel = "AVERTISSEMENT"
		default:
			absResults[i].AlertLevel = "OK"
		}
	}

	for idx, ar := range absResults {
		rowNum := idx + 3
		isAlt := idx%2 == 1
		ds, ns, _ := rowStyles(isAlt, styles)

		var alertStyle int
		switch ar.AlertLevel {
		case "CRITIQUE", "JAMAIS VISITÉ":
			alertStyle = styles["red"]
		case "AVERTISSEMENT":
			alertStyle = styles["orange"]
		default:
			alertStyle = styles["green"]
		}

		f.SetCellValue(sheetAbs, fmt.Sprintf("A%d", rowNum), idx+1)
		f.SetCellStyle(sheetAbs, fmt.Sprintf("A%d", rowNum), fmt.Sprintf("A%d", rowNum), ns)
		f.SetCellValue(sheetAbs, fmt.Sprintf("B%d", rowNum), ar.AgentName)
		f.SetCellStyle(sheetAbs, fmt.Sprintf("B%d", rowNum), fmt.Sprintf("B%d", rowNum), ds)
		f.SetCellValue(sheetAbs, fmt.Sprintf("C%d", rowNum), ar.AgentTitle)
		f.SetCellStyle(sheetAbs, fmt.Sprintf("C%d", rowNum), fmt.Sprintf("C%d", rowNum), ds)
		if ar.LastVisit != nil && !ar.LastVisit.IsZero() {
			f.SetCellValue(sheetAbs, fmt.Sprintf("D%d", rowNum), ar.LastVisit.Format("02/01/2006"))
		} else {
			f.SetCellValue(sheetAbs, fmt.Sprintf("D%d", rowNum), "—")
		}
		f.SetCellStyle(sheetAbs, fmt.Sprintf("D%d", rowNum), fmt.Sprintf("D%d", rowNum), ds)
		f.SetCellValue(sheetAbs, fmt.Sprintf("E%d", rowNum), ar.DaysInactive)
		f.SetCellStyle(sheetAbs, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("E%d", rowNum), ns)
		f.SetCellValue(sheetAbs, fmt.Sprintf("F%d", rowNum), ar.AlertLevel)
		f.SetCellStyle(sheetAbs, fmt.Sprintf("F%d", rowNum), fmt.Sprintf("F%d", rowNum), alertStyle)
	}

	// ═══════════════════════════════════════════════════════════════════════
	// ONGLET 5 — Comparaison Périodique (3 derniers mois)
	// ═══════════════════════════════════════════════════════════════════════
	sheetPeriod := "Comparaison Périodes"
	f.NewSheet(sheetPeriod)

	addSheetTitle(f, sheetPeriod, "COMPARAISON PÉRIODIQUE — 3 DERNIERS MOIS", "A1", "F1", styles["title"])
	f.SetRowHeight(sheetPeriod, 1, 35)
	f.SetRowHeight(sheetPeriod, 2, 40)

	periodHeaders := []string{"Période", "Visites", "POS Uniques", "Taux Sync (%)", "Évolution Visites", "Évolution POS"}
	periodCols := []string{"A", "B", "C", "D", "E", "F"}
	periodColWidths := []float64{18, 15, 15, 18, 20, 15}
	writeHeaderRow(f, sheetPeriod, 2, periodHeaders, periodCols, periodColWidths, styles["header"])

	type PeriodRow struct {
		Label     string
		Visits    int64
		POSUnique int64
		SyncRate  float64
	}

	var periodRows []PeriodRow
	for i := 2; i >= 0; i-- {
		mStart := time.Date(now.Year(), now.Month()-time.Month(i), 1, 0, 0, 0, 0, now.Location())
		mEnd := mStart.AddDate(0, 1, -1)
		var pd PeriodRow
		db.Table("pos_forms pf").
			Where("pf.created_at BETWEEN ? AND ?", mStart, mEnd).
			Where("pf.deleted_at IS NULL").
			Select(`
				COUNT(DISTINCT pf.uuid) AS visits,
				COUNT(DISTINCT pf.pos_uuid) AS pos_unique,
				ROUND(100.0 * COUNT(CASE WHEN pf.sync = true THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS sync_rate
			`).Scan(&pd)
		pd.Label = mStart.Format("Jan 2006")
		periodRows = append(periodRows, pd)
	}

	for idx, pr := range periodRows {
		rowNum := idx + 3
		isAlt := idx%2 == 1
		ds, ns, ps := rowStyles(isAlt, styles)
		f.SetCellValue(sheetPeriod, fmt.Sprintf("A%d", rowNum), pr.Label)
		f.SetCellStyle(sheetPeriod, fmt.Sprintf("A%d", rowNum), fmt.Sprintf("A%d", rowNum), ds)
		f.SetCellValue(sheetPeriod, fmt.Sprintf("B%d", rowNum), pr.Visits)
		f.SetCellStyle(sheetPeriod, fmt.Sprintf("B%d", rowNum), fmt.Sprintf("B%d", rowNum), ns)
		f.SetCellValue(sheetPeriod, fmt.Sprintf("C%d", rowNum), pr.POSUnique)
		f.SetCellStyle(sheetPeriod, fmt.Sprintf("C%d", rowNum), fmt.Sprintf("C%d", rowNum), ns)
		f.SetCellValue(sheetPeriod, fmt.Sprintf("D%d", rowNum), fmt.Sprintf("%.2f%%", pr.SyncRate))
		f.SetCellStyle(sheetPeriod, fmt.Sprintf("D%d", rowNum), fmt.Sprintf("D%d", rowNum), ps)

		if idx > 0 && periodRows[idx-1].Visits > 0 {
			evVisits := (float64(pr.Visits) - float64(periodRows[idx-1].Visits)) / float64(periodRows[idx-1].Visits) * 100
			evPOS := float64(0)
			if periodRows[idx-1].POSUnique > 0 {
				evPOS = (float64(pr.POSUnique) - float64(periodRows[idx-1].POSUnique)) / float64(periodRows[idx-1].POSUnique) * 100
			}
			f.SetCellValue(sheetPeriod, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("%+.2f%%", evVisits))
			f.SetCellValue(sheetPeriod, fmt.Sprintf("F%d", rowNum), fmt.Sprintf("%+.2f%%", evPOS))
			if evVisits >= 0 {
				f.SetCellStyle(sheetPeriod, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("E%d", rowNum), styles["green"])
			} else {
				f.SetCellStyle(sheetPeriod, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("E%d", rowNum), styles["red"])
			}
			if evPOS >= 0 {
				f.SetCellStyle(sheetPeriod, fmt.Sprintf("F%d", rowNum), fmt.Sprintf("F%d", rowNum), styles["green"])
			} else {
				f.SetCellStyle(sheetPeriod, fmt.Sprintf("F%d", rowNum), fmt.Sprintf("F%d", rowNum), styles["red"])
			}
		} else {
			f.SetCellValue(sheetPeriod, fmt.Sprintf("E%d", rowNum), "—")
			f.SetCellValue(sheetPeriod, fmt.Sprintf("F%d", rowNum), "—")
			f.SetCellStyle(sheetPeriod, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("F%d", rowNum), ds)
		}
		_ = ns
		_ = ps
	}

	// ═══════════════════════════════════════════════════════════════════════
	// ONGLET 6 — Objectifs vs Réalisé par Territoire
	// ═══════════════════════════════════════════════════════════════════════
	sheetTarget := "Objectifs vs Réalisé"
	f.NewSheet(sheetTarget)

	addSheetTitle(f, sheetTarget, "OBJECTIFS VS RÉALISÉ PAR TERRITOIRE", "A1", "F1", styles["title"])
	f.SetRowHeight(sheetTarget, 1, 35)
	f.SetRowHeight(sheetTarget, 2, 40)

	targetHeaders := []string{"N°", "Territoire (Area)", "Visites Réelles", "Objectif (Cible)", "% Réalisation", "Statut Risque"}
	targetCols := []string{"A", "B", "C", "D", "E", "F"}
	targetColWidths := []float64{5, 30, 18, 18, 18, 18}
	writeHeaderRow(f, sheetTarget, 2, targetHeaders, targetCols, targetColWidths, styles["header"])

	type TargetRow struct {
		Territory string  `json:"territory"`
		Actual    int64   `json:"actual_visits"`
		Target    int64   `json:"target_visits"`
		Pct       float64 `json:"achievement_percentage"`
	}

	var targetResults []TargetRow
	db.Table("pos_forms pf").
		Joins("LEFT JOIN users u ON pf.user_uuid = u.uuid").
		Joins("LEFT JOIN areas a ON pf.area_uuid = a.uuid").
		Where("pf.created_at BETWEEN ? AND ?", start, end).
		Where("pf.deleted_at IS NULL").
		Select(fmt.Sprintf(`
			a.name AS territory,
			COUNT(DISTINCT pf.uuid) AS actual_visits,
			CAST(40 * %d AS BIGINT) AS target_visits,
			ROUND(100.0 * COUNT(DISTINCT pf.uuid) / NULLIF(40 * %d, 0), 2) AS achievement_percentage
		`, days, days)).
		Group("a.name").
		Order("achievement_percentage ASC").
		Scan(&targetResults)

	for idx, tr := range targetResults {
		rowNum := idx + 3
		isAlt := idx%2 == 1
		ds, ns, ps := rowStyles(isAlt, styles)
		status, sStyle := pctToStatus(tr.Pct, styles)
		f.SetCellValue(sheetTarget, fmt.Sprintf("A%d", rowNum), idx+1)
		f.SetCellStyle(sheetTarget, fmt.Sprintf("A%d", rowNum), fmt.Sprintf("A%d", rowNum), ns)
		f.SetCellValue(sheetTarget, fmt.Sprintf("B%d", rowNum), tr.Territory)
		f.SetCellStyle(sheetTarget, fmt.Sprintf("B%d", rowNum), fmt.Sprintf("B%d", rowNum), ds)
		f.SetCellValue(sheetTarget, fmt.Sprintf("C%d", rowNum), tr.Actual)
		f.SetCellStyle(sheetTarget, fmt.Sprintf("C%d", rowNum), fmt.Sprintf("C%d", rowNum), ns)
		f.SetCellValue(sheetTarget, fmt.Sprintf("D%d", rowNum), tr.Target)
		f.SetCellStyle(sheetTarget, fmt.Sprintf("D%d", rowNum), fmt.Sprintf("D%d", rowNum), ns)
		f.SetCellValue(sheetTarget, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("%.2f%%", tr.Pct))
		f.SetCellStyle(sheetTarget, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("E%d", rowNum), ps)
		f.SetCellValue(sheetTarget, fmt.Sprintf("F%d", rowNum), status)
		f.SetCellStyle(sheetTarget, fmt.Sprintf("F%d", rowNum), fmt.Sprintf("F%d", rowNum), sStyle)
	}

	// ═══════════════════════════════════════════════════════════════════════
	// ONGLET 7 — Détail POS
	// ═══════════════════════════════════════════════════════════════════════
	sheetPOS := "Détail POS"
	f.NewSheet(sheetPOS)

	addSheetTitle(f, sheetPOS, "DÉTAIL COUVERTURE POS", "A1", "H1", styles["title"])
	f.SetRowHeight(sheetPOS, 1, 35)
	f.SetRowHeight(sheetPOS, 2, 40)

	posHeaders := []string{"N°", "Nom POS", "Shop", "Type (postype)", "Commune", "Nb Visites", "Agents Uniques", "Dernière Visite", "Statut Couverture"}
	posCols := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I"}
	posColWidths := []float64{5, 15, 30, 15, 20, 12, 15, 20, 20}
	writeHeaderRow(f, sheetPOS, 2, posHeaders, posCols, posColWidths, styles["header"])

	type POSRow struct {
		POSUUID      string     `json:"pos_uuid"`
		Name         string     `json:"name"`
		Shop         string     `json:"shop"`
		Postype      string     `json:"postype"`
		CommuneName  string     `json:"commune_name"`
		VisitsCount  int64      `json:"visits_count"`
		UniqueAgents int64      `json:"unique_agents"`
		LastVisit    *time.Time `json:"last_visit"`
		DaysSince    int64      `json:"days_since"`
	}

	var posRows []POSRow
	posQ := db.Table("pos p").
		Joins("LEFT JOIN communes c ON p.commune_uuid = c.uuid").
		Joins(fmt.Sprintf("LEFT JOIN pos_forms pf ON p.uuid = pf.pos_uuid AND pf.created_at BETWEEN '%s' AND '%s' AND pf.deleted_at IS NULL", start.Format("2006-01-02"), end.Format("2006-01-02"))).
		Select(`
			p.uuid AS pos_uuid,
			p.name,
			p.shop,
			p.postype,
			c.name AS commune_name,
			COUNT(DISTINCT pf.uuid) AS visits_count,
			COUNT(DISTINCT pf.user_uuid) AS unique_agents,
			MAX(pf.created_at) AS last_visit,
			COALESCE(ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(pf.created_at))) / 86400)::BIGINT, 9999) AS days_since
		`).
		Group("p.uuid, p.name, p.shop, p.postype, c.name").
		Order("visits_count DESC")

	if communeUUID != "" {
		posQ = posQ.Where("p.commune_uuid = ?", communeUUID)
	} else if subAreaUUID != "" {
		posQ = posQ.Where("p.sub_area_uuid = ?", subAreaUUID)
	} else if areaUUID != "" {
		posQ = posQ.Where("p.area_uuid = ?", areaUUID)
	} else if provinceUUID != "" {
		posQ = posQ.Where("p.province_uuid = ?", provinceUUID)
	} else if countryUUID != "" {
		posQ = posQ.Where("p.country_uuid = ?", countryUUID)
	}

	posQ.Scan(&posRows)

	for idx, pr := range posRows {
		rowNum := idx + 3
		isAlt := idx%2 == 1
		ds, ns, _ := rowStyles(isAlt, styles)

		var coverageStatus string
		var coverageStyle int
		switch {
		case pr.VisitsCount == 0:
			coverageStatus = "NON VISITÉ"
			coverageStyle = styles["red"]
		case pr.DaysSince > 21:
			coverageStatus = "À SURVEILLER"
			coverageStyle = styles["orange"]
		case pr.DaysSince > 14:
			coverageStatus = "ATTENTION"
			coverageStyle = styles["orange"]
		default:
			coverageStatus = "BON"
			coverageStyle = styles["green"]
		}

		f.SetCellValue(sheetPOS, fmt.Sprintf("A%d", rowNum), idx+1)
		f.SetCellStyle(sheetPOS, fmt.Sprintf("A%d", rowNum), fmt.Sprintf("A%d", rowNum), ns)
		f.SetCellValue(sheetPOS, fmt.Sprintf("B%d", rowNum), pr.Name)
		f.SetCellStyle(sheetPOS, fmt.Sprintf("B%d", rowNum), fmt.Sprintf("B%d", rowNum), ds)
		f.SetCellValue(sheetPOS, fmt.Sprintf("C%d", rowNum), pr.Shop)
		f.SetCellStyle(sheetPOS, fmt.Sprintf("C%d", rowNum), fmt.Sprintf("C%d", rowNum), ds)
		f.SetCellValue(sheetPOS, fmt.Sprintf("D%d", rowNum), pr.Postype)
		f.SetCellStyle(sheetPOS, fmt.Sprintf("D%d", rowNum), fmt.Sprintf("D%d", rowNum), ds)
		f.SetCellValue(sheetPOS, fmt.Sprintf("E%d", rowNum), pr.CommuneName)
		f.SetCellStyle(sheetPOS, fmt.Sprintf("E%d", rowNum), fmt.Sprintf("E%d", rowNum), ds)
		f.SetCellValue(sheetPOS, fmt.Sprintf("F%d", rowNum), pr.VisitsCount)
		f.SetCellStyle(sheetPOS, fmt.Sprintf("F%d", rowNum), fmt.Sprintf("F%d", rowNum), ns)
		f.SetCellValue(sheetPOS, fmt.Sprintf("G%d", rowNum), pr.UniqueAgents)
		f.SetCellStyle(sheetPOS, fmt.Sprintf("G%d", rowNum), fmt.Sprintf("G%d", rowNum), ns)
		if pr.LastVisit != nil && !pr.LastVisit.IsZero() {
			f.SetCellValue(sheetPOS, fmt.Sprintf("H%d", rowNum), pr.LastVisit.Format("02/01/2006"))
		} else {
			f.SetCellValue(sheetPOS, fmt.Sprintf("H%d", rowNum), "—")
		}
		f.SetCellStyle(sheetPOS, fmt.Sprintf("H%d", rowNum), fmt.Sprintf("H%d", rowNum), ds)
		f.SetCellValue(sheetPOS, fmt.Sprintf("I%d", rowNum), coverageStatus)
		f.SetCellStyle(sheetPOS, fmt.Sprintf("I%d", rowNum), fmt.Sprintf("I%d", rowNum), coverageStyle)
	}

	// ═══════════════════════════════════════════════════════════════════════
	// Activer l'onglet Résumé par défaut
	// ═══════════════════════════════════════════════════════════════════════
	summaryIdx, _ := f.GetSheetIndex(sheetSummary)
	f.SetActiveSheet(summaryIdx)

	// ══ Envoi du fichier ══════════════════════════════════════════════════
	filename := fmt.Sprintf("rapport_kpi_%s_%s.xlsx", startDate, endDate)
	c.Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	c.Set("Cache-Control", "no-cache, no-store, must-revalidate")

	if err := f.Write(c.Response().BodyWriter()); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status":  "error",
			"message": "Erreur lors de la génération du fichier Excel",
			"error":   err.Error(),
		})
	}
	return nil
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Helpers
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

func addSheetTitle(f *excelize.File, sheet, title, cellFrom, cellTo string, style int) {
	f.MergeCell(sheet, cellFrom, cellTo)
	f.SetCellValue(sheet, cellFrom, title)
	f.SetCellStyle(sheet, cellFrom, cellFrom, style)
}

func setCell(f *excelize.File, sheet, cell string, value interface{}, style int) {
	f.SetCellValue(sheet, cell, value)
	f.SetCellStyle(sheet, cell, cell, style)
}

func setInfoRow(f *excelize.File, sheet string, row int, label, value string, styles map[string]int) {
	f.SetCellValue(sheet, fmt.Sprintf("A%d", row), label)
	f.SetCellStyle(sheet, fmt.Sprintf("A%d", row), fmt.Sprintf("A%d", row), styles["info"])
	f.SetCellValue(sheet, fmt.Sprintf("B%d", row), value)
	f.SetCellStyle(sheet, fmt.Sprintf("B%d", row), fmt.Sprintf("B%d", row), styles["infoVal"])
}

func writeHeaderRow(f *excelize.File, sheet string, rowNum int, headers []string, cols []string, widths []float64, style int) {
	for i, h := range headers {
		cell := fmt.Sprintf("%s%d", cols[i], rowNum)
		f.SetCellValue(sheet, cell, h)
		f.SetCellStyle(sheet, cell, cell, style)
		if i < len(widths) {
			f.SetColWidth(sheet, cols[i], cols[i], widths[i])
		}
	}
	f.SetRowHeight(sheet, rowNum, 35)
}

func rowStyles(isAlt bool, styles map[string]int) (ds, ns, ps int) {
	if isAlt {
		return styles["altRow"], styles["altNum"], styles["altPct"]
	}
	return styles["data"], styles["num"], styles["pct"]
}

func pctToStatus(pct float64, styles map[string]int) (string, int) {
	switch {
	case pct >= 100:
		return "ATTEINT", styles["green"]
	case pct >= 80:
		return "EN COURS", styles["orange"]
	default:
		return "NON ATTEINT", styles["red"]
	}
}

// colLetter retourne la lettre de colonne Excel pour un index 0-based
func colLetter(idx int) string {
	return string(rune('A' + idx))
}

var _ = colLetter // évite unused warning
var _ = strconv.Itoa

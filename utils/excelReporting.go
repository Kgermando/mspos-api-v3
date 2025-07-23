package utils

import (
	"fmt"
	"time"

	"github.com/xuri/excelize/v2"
)

// ExcelReportConfig holds configuration for Excel reports
type ExcelReportConfig struct {
	Title       string
	CompanyName string
	ReportDate  time.Time
	Author      string
}

// CreateExcelFile creates a new Excel file with standard formatting
func CreateExcelFile(config ExcelReportConfig) *excelize.File {
	f := excelize.NewFile()
	return f
}

// SetupExcelStyles creates and returns common Excel styles
func SetupExcelStyles(f *excelize.File) (map[string]int, error) {
	styles := make(map[string]int)

	// Title style
	titleStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold:   true,
			Size:   18,
			Color:  "1F4E79",
			Family: "Calibri",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
		},
		Fill: excelize.Fill{
			Type:    "pattern",
			Color:   []string{"E7F3FF"},
			Pattern: 1,
		},
	})
	if err != nil {
		return nil, err
	}
	styles["title"] = titleStyle

	// Header style
	headerStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold:   true,
			Size:   12,
			Color:  "FFFFFF",
			Family: "Calibri",
		},
		Fill: excelize.Fill{
			Type:    "pattern",
			Color:   []string{"4472C4"},
			Pattern: 1,
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
		},
	})
	if err != nil {
		return nil, err
	}
	styles["header"] = headerStyle

	// Data style
	dataStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Size:   10,
			Family: "Calibri",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "left",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "D3D3D3", Style: 1},
			{Type: "top", Color: "D3D3D3", Style: 1},
			{Type: "bottom", Color: "D3D3D3", Style: 1},
			{Type: "right", Color: "D3D3D3", Style: 1},
		},
	})
	if err != nil {
		return nil, err
	}
	styles["data"] = dataStyle

	// Number style
	numberStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Size:   10,
			Family: "Calibri",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "right",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "D3D3D3", Style: 1},
			{Type: "top", Color: "D3D3D3", Style: 1},
			{Type: "bottom", Color: "D3D3D3", Style: 1},
			{Type: "right", Color: "D3D3D3", Style: 1},
		},
		NumFmt: 3, // Number format with thousands separator
	})
	if err != nil {
		return nil, err
	}
	styles["number"] = numberStyle

	// Date style
	dateStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Size:   10,
			Family: "Calibri",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "D3D3D3", Style: 1},
			{Type: "top", Color: "D3D3D3", Style: 1},
			{Type: "bottom", Color: "D3D3D3", Style: 1},
			{Type: "right", Color: "D3D3D3", Style: 1},
		},
		NumFmt: 14, // Date format
	})
	if err != nil {
		return nil, err
	}
	styles["date"] = dateStyle

	// Success style (green)
	successStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Size:   10,
			Family: "Calibri",
			Bold:   true,
			Color:  "00B050",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "D3D3D3", Style: 1},
			{Type: "top", Color: "D3D3D3", Style: 1},
			{Type: "bottom", Color: "D3D3D3", Style: 1},
			{Type: "right", Color: "D3D3D3", Style: 1},
		},
	})
	if err != nil {
		return nil, err
	}
	styles["success"] = successStyle

	// Warning style (orange)
	warningStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Size:   10,
			Family: "Calibri",
			Bold:   true,
			Color:  "FF9900",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "D3D3D3", Style: 1},
			{Type: "top", Color: "D3D3D3", Style: 1},
			{Type: "bottom", Color: "D3D3D3", Style: 1},
			{Type: "right", Color: "D3D3D3", Style: 1},
		},
	})
	if err != nil {
		return nil, err
	}
	styles["warning"] = warningStyle

	// Error style (red)
	errorStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Size:   10,
			Family: "Calibri",
			Bold:   true,
			Color:  "FF0000",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "D3D3D3", Style: 1},
			{Type: "top", Color: "D3D3D3", Style: 1},
			{Type: "bottom", Color: "D3D3D3", Style: 1},
			{Type: "right", Color: "D3D3D3", Style: 1},
		},
	})
	if err != nil {
		return nil, err
	}
	styles["error"] = errorStyle

	// Percentage style
	percentageStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Size:   10,
			Family: "Calibri",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "right",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "D3D3D3", Style: 1},
			{Type: "top", Color: "D3D3D3", Style: 1},
			{Type: "bottom", Color: "D3D3D3", Style: 1},
			{Type: "right", Color: "D3D3D3", Style: 1},
		},
		NumFmt: 10, // Percentage format
	})
	if err != nil {
		return nil, err
	}
	styles["percentage"] = percentageStyle

	return styles, nil
}

// AddReportHeader adds a professional header to the Excel sheet
func AddReportHeader(f *excelize.File, sheetName string, config ExcelReportConfig, styles map[string]int) error {
	// Add title
	f.SetCellValue(sheetName, "A1", config.Title)
	f.SetCellStyle(sheetName, "A1", "A1", styles["title"])
	f.MergeCell(sheetName, "A1", "H1")

	// Add company name
	f.SetCellValue(sheetName, "A2", fmt.Sprintf("Entreprise: %s", config.CompanyName))

	// Add report date
	f.SetCellValue(sheetName, "A3", fmt.Sprintf("Date du rapport: %s", config.ReportDate.Format("02/01/2006 15:04:05")))

	// Add author
	if config.Author != "" {
		f.SetCellValue(sheetName, "A4", fmt.Sprintf("Généré par: %s", config.Author))
	}

	return nil
}

// AutoFitColumns automatically adjusts column widths
func AutoFitColumns(f *excelize.File, sheetName string, columns []string, defaultWidth float64) error {
	for _, col := range columns {
		err := f.SetColWidth(sheetName, col, col, defaultWidth)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddSummaryTable adds a summary table to the sheet
func AddSummaryTable(f *excelize.File, sheetName string, data map[string]interface{}, startRow int, styles map[string]int) error {
	currentRow := startRow

	// Add summary title
	f.SetCellValue(sheetName, fmt.Sprintf("A%d", currentRow), "RÉSUMÉ EXÉCUTIF")
	f.SetCellStyle(sheetName, fmt.Sprintf("A%d", currentRow), fmt.Sprintf("B%d", currentRow), styles["header"])
	f.MergeCell(sheetName, fmt.Sprintf("A%d", currentRow), fmt.Sprintf("B%d", currentRow))
	currentRow++

	// Add data
	for key, value := range data {
		f.SetCellValue(sheetName, fmt.Sprintf("A%d", currentRow), key)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", currentRow), value)
		f.SetCellStyle(sheetName, fmt.Sprintf("A%d", currentRow), fmt.Sprintf("A%d", currentRow), styles["data"])
		f.SetCellStyle(sheetName, fmt.Sprintf("B%d", currentRow), fmt.Sprintf("B%d", currentRow), styles["number"])
		currentRow++
	}

	return nil
}

// CreateChart creates a chart in the Excel file
// Note: Chart functionality is temporarily disabled due to API compatibility issues
func CreateChart(f *excelize.File, sheetName string, chartType string, dataRange string, title string) error {
	// TODO: Implement chart creation when excelize API is clarified
	// For now, we'll add a text note about the chart
	f.SetCellValue(sheetName, "E2", fmt.Sprintf("Graphique: %s (%s)", title, chartType))
	f.SetCellValue(sheetName, "E3", fmt.Sprintf("Données: %s", dataRange))

	// Create a simple style for the chart placeholder
	chartStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold:   true,
			Size:   12,
			Color:  "4472C4",
			Family: "Calibri",
		},
		Alignment: &excelize.Alignment{
			Horizontal: "center",
			Vertical:   "center",
		},
		Border: []excelize.Border{
			{Type: "left", Color: "4472C4", Style: 2},
			{Type: "top", Color: "4472C4", Style: 2},
			{Type: "bottom", Color: "4472C4", Style: 2},
			{Type: "right", Color: "4472C4", Style: 2},
		},
	})

	if err == nil {
		f.SetCellStyle(sheetName, "E2", "F3", chartStyle)
		f.MergeCell(sheetName, "E2", "F2")
		f.MergeCell(sheetName, "E3", "F3")
	}

	return nil
}

// AddDataValidation adds data validation to specific cells
func AddDataValidation(f *excelize.File, sheetName string, cellRange string, validationType string, values []string) error {
	dv := excelize.NewDataValidation(true)
	dv.SetSqref(cellRange)

	switch validationType {
	case "list":
		formula := ""
		for i, value := range values {
			if i == 0 {
				formula = value
			} else {
				formula += "," + value
			}
		}
		dv.SetDropList(values)
	}

	return f.AddDataValidation(sheetName, dv)
}

// FormatCurrency formats a cell as currency
func FormatCurrency(f *excelize.File, sheetName string, cellRange string) error {
	style, err := f.NewStyle(&excelize.Style{
		NumFmt: 164, // Custom currency format
	})
	if err != nil {
		return err
	}

	return f.SetCellStyle(sheetName, cellRange, cellRange, style)
}

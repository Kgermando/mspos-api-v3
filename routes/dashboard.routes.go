package routes

import (
	"github.com/danny19977/mspos-api-v3/controllers/dashboard"
	ndindividual "github.com/danny19977/mspos-api-v3/controllers/nd_individual"
	"github.com/gofiber/fiber/v2"
)

func setupDashboardRoutes(api fiber.Router) {
	dash := api.Group("/dashboard")

	// ── ND Dashboard ──────────────────────────────────────────────────────────
	// Numeric Distribution: ND% = (POS w/ brand counter>0) / total POS visited
	nd := dash.Group("/numeric-distribution")

	// Section 1 — Table Views (territory × brand grid)
	nd.Get("/table-view-province", dashboard.NDTableViewProvince)
	nd.Get("/table-view-area", dashboard.NDTableViewArea)
	nd.Get("/table-view-subarea", dashboard.NDTableViewSubArea)
	nd.Get("/table-view-commune", dashboard.NDTableViewCommune)

	// Section 2 — Bar Charts (grouped brands per territory)
	nd.Get("/bar-chart-province", dashboard.NDBarChartProvince)
	nd.Get("/bar-chart-area", dashboard.NDBarChartArea)
	nd.Get("/bar-chart-subarea", dashboard.NDBarChartSubArea)
	nd.Get("/bar-chart-commune", dashboard.NDBarChartCommune)

	// Section 3 — Monthly Trend Line Chart
	nd.Get("/line-chart-by-month", dashboard.NDLineChartByMonth)

	// Section 4 — Power Analytics
	nd.Get("/summary-kpi", dashboard.NDSummaryKPI)     // executive KPI card
	nd.Get("/brand-ranking", dashboard.NDBrandRanking) // brands ranked by ND%
	nd.Get("/gap-analysis", dashboard.NDGapAnalysis)   // 3-zone opportunity funnel

	// Section 5 — Advanced Analytics
	nd.Get("/heatmap", dashboard.NDHeatmap)     // brand × territory matrix (?level=province|area|subarea|commune)
	nd.Get("/evolution", dashboard.NDEvolution) // period-over-period ND% comparison

	// SOS Dashboard
	sos := dash.Group("/share-of-stock")

	// Section 1 — Table Views (territory × brand shelf-share grid)
	sos.Get("/table-view-province", dashboard.SOSTableViewProvince)
	sos.Get("/table-view-area", dashboard.SOSTableViewArea)
	sos.Get("/table-view-subarea", dashboard.SOSTableViewSubArea)
	sos.Get("/table-view-commune", dashboard.SOSTableViewCommune)

	// Section 2 — Bar Charts (grouped brands per territory)
	sos.Get("/bar-chart-province", dashboard.SOSBarChartProvince)
	sos.Get("/bar-chart-area", dashboard.SOSBarChartArea)
	sos.Get("/bar-chart-subarea", dashboard.SOSBarChartSubArea)
	sos.Get("/bar-chart-commune", dashboard.SOSBarChartCommune)

	// Section 3 — Monthly Trend Line Chart (SOS% by brand over time)
	sos.Get("/line-chart-by-month", dashboard.SOSLineChartByMonth)

	// Section 4 — Power Analytics
	sos.Get("/summary-kpi", dashboard.SOSSummaryKPI)                 // executive KPI + HHI market structure
	sos.Get("/brand-ranking", dashboard.SOSBrandRanking)             // Pareto ranking by shelf share
	sos.Get("/concentration-index", dashboard.SOSConcentrationIndex) // HHI per territory (?level=...)

	// Section 5 — Advanced Analytics
	sos.Get("/heatmap", dashboard.SOSHeatmap)                   // brand × territory SOS% matrix (?level=...)
	sos.Get("/evolution", dashboard.SOSEvolution)               // period-over-period SOS% delta
	sos.Get("/gap-analysis", dashboard.SOSShareGapAnalysis)     // brands below target shelf share (?target=25)
	sos.Get("/pos-drill-down", dashboard.SOSPosDrillDown)       // POS-level SOS deep-dive (?brand_uuid=...)
	sos.Get("/vs-nd-correlation", dashboard.SOSVsNDCorrelation) // SOS×ND quadrant matrix (leader/niche/...)

	// ── OOS Dashboard ──────────────────────────────────────────────────────────
	// Out-of-Stock Rate: OOS% = (POS w/ brand counter=0) / total POS visited × 100
	oos := dash.Group("/out-of-stock")

	// Section 1 — Table Views (territory × brand grid)
	oos.Get("/table-view-province", dashboard.OOSTableViewProvince)
	oos.Get("/table-view-area", dashboard.OOSTableViewArea)
	oos.Get("/table-view-subarea", dashboard.OOSTableViewSubArea)
	oos.Get("/table-view-commune", dashboard.OOSTableViewCommune)

	// Section 2 — Bar Charts (grouped brands per territory)
	oos.Get("/bar-chart-province", dashboard.OOSBarChartProvince)
	oos.Get("/bar-chart-area", dashboard.OOSBarChartArea)
	oos.Get("/bar-chart-subarea", dashboard.OOSBarChartSubArea)
	oos.Get("/bar-chart-commune", dashboard.OOSBarChartCommune)

	// Section 3 — Monthly Trend Line Chart
	oos.Get("/line-chart-by-month", dashboard.OOSLineChartByMonth)

	// Section 4 — Power Analytics
	oos.Get("/summary-kpi", dashboard.OOSSummaryKPI)       // executive KPI card
	oos.Get("/brand-ranking", dashboard.OOSBrandRanking)   // brands ranked by OOS% (worst first)
	oos.Get("/critical-alert", dashboard.OOSCriticalAlert) // top hotspot territory × brand pairs

	// Section 5 — Advanced Analytics
	oos.Get("/heatmap", dashboard.OOSHeatmap)     // brand × territory OOS% matrix (?level=...)
	oos.Get("/evolution", dashboard.OOSEvolution) // period-over-period OOS% comparison

	// ── WD Dashboard ──────────────────────────────────────────────────────────
	// Weighted Distribution: WD% = SUM(fardes at POS where brand counter>0) / total fardes × 100
	wd := dash.Group("/weighted-distribution")

	// Section 1 — Table Views (territory × brand volume-weighted grid)
	wd.Get("/table-view-province", dashboard.WDTableViewProvince)
	wd.Get("/table-view-area", dashboard.WDTableViewArea)
	wd.Get("/table-view-subarea", dashboard.WDTableViewSubArea)
	wd.Get("/table-view-commune", dashboard.WDTableViewCommune)

	// Section 2 — Bar Charts (grouped brands per territory)
	wd.Get("/bar-chart-province", dashboard.WDBarChartProvince)
	wd.Get("/bar-chart-area", dashboard.WDBarChartArea)
	wd.Get("/bar-chart-subarea", dashboard.WDBarChartSubArea)
	wd.Get("/bar-chart-commune", dashboard.WDBarChartCommune)

	// Section 3 — Monthly Trend Line Chart
	wd.Get("/line-chart-by-month", dashboard.WDLineChartByMonth)

	// Section 4 — Power Analytics
	wd.Get("/summary-kpi", dashboard.WDSummaryKPI)     // executive KPI card
	wd.Get("/brand-ranking", dashboard.WDBrandRanking) // brands ranked by WD% + WD-ND gap
	wd.Get("/gap-analysis", dashboard.WDGapAnalysis)   // volume opportunity funnel

	// Section 5 — Advanced Analytics
	wd.Get("/heatmap", dashboard.WDHeatmap)                   // brand × territory WD% matrix (?level=...)
	wd.Get("/evolution", dashboard.WDEvolution)               // period-over-period WD% comparison
	wd.Get("/vs-nd-correlation", dashboard.WDvsNDCorrelation) // WD×ND quadrant matrix (?threshold=50)
	wd.Get("/pos-drill-down", dashboard.WDPosDrillDown)       // POS-level WD deep-dive (?brand_uuid=...)

	// ── WS Dashboard ──────────────────────────────────────────────────────────
	// Weighted Sales: WS% = SUM(sold at POS where brand counter>0) / total sold × 100
	ws := dash.Group("/weighted-sales")

	// Section 1 — Table Views (territory × brand sales-weighted grid)
	ws.Get("/table-view-province", dashboard.WSTableViewProvince)
	ws.Get("/table-view-area", dashboard.WSTableViewArea)
	ws.Get("/table-view-subarea", dashboard.WSTableViewSubArea)
	ws.Get("/table-view-commune", dashboard.WSTableViewCommune)

	// Section 2 — Bar Charts (grouped brands per territory)
	ws.Get("/bar-chart-province", dashboard.WSBarChartProvince)
	ws.Get("/bar-chart-area", dashboard.WSBarChartArea)
	ws.Get("/bar-chart-subarea", dashboard.WSBarChartSubArea)
	ws.Get("/bar-chart-commune", dashboard.WSBarChartCommune)

	// Section 3 — Monthly Trend Line Chart
	ws.Get("/line-chart-by-month", dashboard.WSLineChartByMonth)

	// Section 4 — Power Analytics
	ws.Get("/summary-kpi", dashboard.WSSummaryKPI)     // executive KPI card
	ws.Get("/brand-ranking", dashboard.WSBrandRanking) // brands ranked by WS% + WS-ND gap
	ws.Get("/gap-analysis", dashboard.WSGapAnalysis)   // 3-zone opportunity funnel

	// Section 5 — Advanced Analytics
	ws.Get("/heatmap", dashboard.WSHeatmap)                   // brand × territory WS% matrix (?level=...)
	ws.Get("/evolution", dashboard.WSEvolution)               // period-over-period WS% comparison
	ws.Get("/vs-nd-correlation", dashboard.WSvsNDCorrelation) // WS×ND quadrant matrix (?threshold=50)
	ws.Get("/pos-drill-down", dashboard.WSPosDrillDown)       // POS-level WS deep-dive (?brand_uuid=...)

	// ── SISH Dashboard ─────────────────────────────────────────────────────────
	// Share In Shop: SISH% = SUM(brand_sold) / SUM(total_sold) × 100
	// Measures a brand's market share of actual units sold across all visited POS.
	// Velocity Index = SISH% / SOS%  (>1: fast mover | <1: slow mover)
	sish := dash.Group("/share-in-shop")

	// Section 1 — Table Views (territory × brand sales-share grid + velocity)
	sish.Get("/table-view-province", dashboard.SISHTableViewProvince)
	sish.Get("/table-view-area", dashboard.SISHTableViewArea)
	sish.Get("/table-view-subarea", dashboard.SISHTableViewSubArea)
	sish.Get("/table-view-commune", dashboard.SISHTableViewCommune)

	// Section 2 — Bar Charts (grouped brands per territory)
	sish.Get("/bar-chart-province", dashboard.SISHBarChartProvince)
	sish.Get("/bar-chart-area", dashboard.SISHBarChartArea)
	sish.Get("/bar-chart-subarea", dashboard.SISHBarChartSubArea)
	sish.Get("/bar-chart-commune", dashboard.SISHBarChartCommune)

	// Section 3 — Monthly Trend Line Chart (SISH% + SOS% + velocity by brand)
	sish.Get("/line-chart-by-month", dashboard.SISHLineChartByMonth)

	// Section 4 — Power Analytics
	sish.Get("/summary-kpi", dashboard.SISHSummaryKPI)       // executive KPI + entropy
	sish.Get("/brand-ranking", dashboard.SISHBrandRanking)   // Pareto ranking + category
	sish.Get("/velocity-index", dashboard.SISHVelocityIndex) // sell-through speed + stock_turn_days

	// Section 5 — Advanced Analytics
	sish.Get("/heatmap", dashboard.SISHHeatmap)                     // brand × territory SISH% matrix (?level=...)
	sish.Get("/evolution", dashboard.SISHEvolution)                 // period-over-period SISH% + velocity delta
	sish.Get("/gap-analysis", dashboard.SISHGapAnalysis)            // brands below target SISH (?target=25)
	sish.Get("/vs-sos-correlation", dashboard.SISHVsSosCorrelation) // SISH×SOS quadrant (fast_leader/sell_through_star/...)
	sish.Get("/pos-drill-down", dashboard.SISHPosDrillDown)         // POS-level SISH deep-dive (?brand_uuid=...)

	// Google Map Dashboard
	gm := dash.Group("/google-map")
	gm.Get("/view", dashboard.GoogleMaps)

	// Sales Evolution Dashboard
	se := dash.Group("/sales-evolution")

	// POS-type breakdown (volume + sold + market share per POS category)
	se.Get("/table-view-province", dashboard.TypePosTableProvince)
	se.Get("/table-view-area", dashboard.TypePosTableArea)
	se.Get("/table-view-subarea", dashboard.TypePosTableSubArea)
	se.Get("/table-view-commune", dashboard.TypePosTableCommune)

	// Price analysis (avg / min / max / revenue per brand per territory)
	se.Get("/table-view-province-price", dashboard.PriceTableProvince)
	se.Get("/table-view-area-price", dashboard.PriceTableArea)
	se.Get("/table-view-subarea-price", dashboard.PriceTableSubArea)
	se.Get("/table-view-commune-price", dashboard.PriceTableCommune)

	// Monthly evolution line chart (MoM trend + growth %)
	se.Get("/evolution-by-month", dashboard.SalesEvolutionByMonth)

	// Period-over-period growth rate comparison (curr vs prev window)
	se.Get("/growth-rate", dashboard.SalesGrowthRate)

	// Brand competition matrix (market share heatmap per geo × brand)
	se.Get("/brand-competition-matrix", dashboard.BrandCompetitionMatrix)

	// Top N POS ranking by farde / sold / revenue
	se.Get("/top-pos-ranking", dashboard.TopPOSRanking)

	// Sales representative performance scorecard
	se.Get("/rep-scorecard", dashboard.SalesRepScorecard)

	// Day-of-week heatmap (which days drive the most sales)
	se.Get("/heatmap-day-of-week", dashboard.SalesHeatmapByDayOfWeek)

	// Single KPI card: farde, sold, revenue, visits, active POS & agents
	se.Get("/summary-kpi", dashboard.SalesSummaryKPI)

	// ── ND Individuel ────────────────────────────────────────────────────────
	// Permet à chaque agent de consulter et défendre son propre ND
	ndi := api.Group("/nd-individual")
	ndi.Get("/summary/:user_uuid", ndindividual.GetNDSummary)  // KPI global de l'agent
	ndi.Get("/by-brand/:user_uuid", ndindividual.GetNDByBrand) // ND par marque
	ndi.Get("/pos-list/:user_uuid", ndindividual.GetNDPosList) // Liste POS visités + ND

	// ── SOS Individuel ───────────────────────────────────────────────────────
	// Permet à chaque agent de consulter et défendre son propre SOS (Share of Stock)
	sosi := api.Group("/sos-individual")
	sosi.Get("/summary/:user_uuid", ndindividual.GetSOSSummary)  // KPI global de l'agent
	sosi.Get("/by-brand/:user_uuid", ndindividual.GetSOSByBrand) // SOS par marque
	sosi.Get("/pos-list/:user_uuid", ndindividual.GetSOSPosList) // Liste POS visités + fardes

	// KPI Dashboard
	kp := dash.Group("/kpi")
	kp.Get("/territory-overview", dashboard.GetKPITerritoryOverview)
	kp.Get("/agent-performance", dashboard.GetAgentPerformanceDetails)
	kp.Get("/pos-insights", dashboard.GetPOSLevelInsights)
	kp.Get("/target-vs-actual", dashboard.GetKPITargetVsActual)
	kp.Get("/absence-analysis", dashboard.GetTeamAbsenceAnalysis)
	kp.Get("/period-comparison", dashboard.GetPeriodComparison)
	kp.Get("/nd-analysis", dashboard.GetNDAnalysisByTerritory)
	kp.Get("/table-view/country", dashboard.TotalVisitsByCountry)
	kp.Get("/table-view/province", dashboard.TotalVisitsByProvince)
	kp.Get("/table-view/area", dashboard.TotalVisitsByArea)
	kp.Get("/table-view/sub-area", dashboard.TotalVisitsBySubArea)
	kp.Get("/table-view/commune", dashboard.TotalVisitsByCommune)
	kp.Get("/user-visit-summary", dashboard.KpiUserVisitSummary)
	kp.Get("/export-excel", dashboard.ExportKPIExcel)

}

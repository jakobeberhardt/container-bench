package mappings

type PolarPlotStyle struct {
	Color     string
	LineStyle string
	LineWidth string
	Mark      string
}

var ProbeStyles = []PolarPlotStyle{
	{Color: "red", LineStyle: "solid", LineWidth: "thick", Mark: "none"},
	{Color: "blue!55!black", LineStyle: "solid", LineWidth: "thick", Mark: "none"},
	{Color: "green!55!black", LineStyle: "solid", LineWidth: "thick", Mark: "none"},
	{Color: "orange", LineStyle: "solid", LineWidth: "thick", Mark: "none"},
	{Color: "purple", LineStyle: "solid", LineWidth: "thick", Mark: "none"},
	{Color: "teal", LineStyle: "solid", LineWidth: "thick", Mark: "none"},
	{Color: "pink!80!red", LineStyle: "solid", LineWidth: "thick", Mark: "none"},
	{Color: "blue!20!violet", LineStyle: "solid", LineWidth: "thick", Mark: "none"},
	
	{Color: "orange", LineStyle: "densely dotted", LineWidth: "thick", Mark: "none"},
	{Color: "green!70!black", LineStyle: "densely dotted", LineWidth: "thick", Mark: "none"},
	{Color: "blue!50!white", LineStyle: "densely dotted", LineWidth: "thick", Mark: "none"},
	{Color: "teal", LineStyle: "densely dotted", LineWidth: "thick", Mark: "none"},
	{Color: "violet", LineStyle: "densely dotted", LineWidth: "thick", Mark: "none"},
	{Color: "olive!50!black", LineStyle: "densely dotted", LineWidth: "thick", Mark: "none"},
	{Color: "pink!80!red", LineStyle: "densely dotted", LineWidth: "thick", Mark: "none"},
	{Color: "purple", LineStyle: "densely dotted", LineWidth: "thick", Mark: "none"},
}

func GetProbeStyle(probeIndex int) PolarPlotStyle {
	if probeIndex < 0 {
		probeIndex = 0
	}
	return ProbeStyles[probeIndex%len(ProbeStyles)]
}

func (ps PolarPlotStyle) ToTikzOptions() string {
	return ps.LineWidth + ",mark=" + ps.Mark + ",color=" + ps.Color + "," + ps.LineStyle
}

package mappings

type PolarPlotStyle struct {
	Color     string
	LineStyle string
	LineWidth string
	Mark      string
}

var ProbeStyles = []PolarPlotStyle{
	{
		Color:     "red",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "blue",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "green!60!black",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "orange",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "purple",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "cyan",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "magenta",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "olive",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "teal",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "brown",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "violet",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "pink",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "lime",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "red!70!black",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "blue!70!black",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
	{
		Color:     "gray!70!black",
		LineStyle: "solid",
		LineWidth: "thick",
		Mark:      "none",
	},
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

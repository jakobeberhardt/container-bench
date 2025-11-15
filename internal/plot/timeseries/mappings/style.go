package mappings

type PlotStyle struct {
	Color       string
	LineStyle   string
	LineWidth   string
	Mark        string
	MarkOptions string
}

var ContainerStyles = []PlotStyle{
	// First 8: Distinct colors with varied line styles, thick, with markers
	{Color: "red", LineStyle: "solid", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.7,fill=red"},
	{Color: "blue", LineStyle: "solid", LineWidth: "thick", Mark: "square*", MarkOptions: "scale=0.7,fill=blue"},
	{Color: "green!70!black", LineStyle: "solid", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.7,fill=green!70!black"},
	{Color: "orange", LineStyle: "solid", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.7,fill=orange"},
	{Color: "purple", LineStyle: "solid", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.7,fill=purple"},
	{Color: "brown", LineStyle: "solid", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.7"},
	{Color: "cyan", LineStyle: "solid", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.7,fill=cyan"},
	{Color: "magenta", LineStyle: "solid", LineWidth: "thick", Mark: "star", MarkOptions: "scale=0.7,fill=magenta"},
	
	// Next 8: Same colors with dashed lines and markers
	{Color: "red", LineStyle: "dashed", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.7,fill=red"},
	{Color: "blue", LineStyle: "dashed", LineWidth: "thick", Mark: "square*", MarkOptions: "scale=0.7,fill=blue"},
	{Color: "green!70!black", LineStyle: "dashed", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.7,fill=green!70!black"},
	{Color: "orange", LineStyle: "dashed", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.7,fill=orange"},
	{Color: "purple", LineStyle: "dashed", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.7,fill=purple"},
	{Color: "brown", LineStyle: "dashed", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.7"},
	{Color: "cyan", LineStyle: "dashed", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.7,fill=cyan"},
	{Color: "magenta", LineStyle: "dashed", LineWidth: "thick", Mark: "star", MarkOptions: "scale=0.7,fill=magenta"},
	
	// Next 8: Same colors with dotted lines and markers
	{Color: "red", LineStyle: "dotted", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.7,fill=red"},
	{Color: "blue", LineStyle: "dotted", LineWidth: "thick", Mark: "square*", MarkOptions: "scale=0.7,fill=blue"},
	{Color: "green!70!black", LineStyle: "dotted", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.7,fill=green!70!black"},
	{Color: "orange", LineStyle: "dotted", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.7,fill=orange"},
	{Color: "purple", LineStyle: "dotted", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.7,fill=purple"},
	{Color: "brown", LineStyle: "dotted", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.7"},
	{Color: "cyan", LineStyle: "dotted", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.7,fill=cyan"},
	{Color: "magenta", LineStyle: "dotted", LineWidth: "thick", Mark: "star", MarkOptions: "scale=0.7,fill=magenta"},
	
	// Next 8: Varied line styles for additional differentiation
	{Color: "red", LineStyle: "dashdotted", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.7,fill=red"},
	{Color: "blue", LineStyle: "dashdotted", LineWidth: "thick", Mark: "square*", MarkOptions: "scale=0.7,fill=blue"},
	{Color: "green!70!black", LineStyle: "dashdotted", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.7,fill=green!70!black"},
	{Color: "orange", LineStyle: "dashdotted", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.7,fill=orange"},
	{Color: "purple", LineStyle: "densely dotted", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.7,fill=purple"},
	{Color: "brown", LineStyle: "densely dashed", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.7"},
}

func GetContainerStyle(containerIndex int) PlotStyle {
	if containerIndex < 0 {
		containerIndex = 0
	}
	return ContainerStyles[containerIndex%len(ContainerStyles)]
}

func (ps PlotStyle) ToTikzOptions() string {
	options := ps.Color
	if ps.LineStyle != "" {
		options += "," + ps.LineStyle
	}
	if ps.LineWidth != "" {
		options += "," + ps.LineWidth
	}
	if ps.Mark != "none" && ps.Mark != "" {
		options += ",mark=" + ps.Mark
		if ps.MarkOptions != "" {
			options += ",mark options={" + ps.MarkOptions + "}"
		}
	}
	return options
}
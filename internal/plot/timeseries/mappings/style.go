package mappings

type PlotStyle struct {
	Color       string
	LineStyle   string
	LineWidth   string
	Mark        string
	MarkOptions string
}

var ContainerStyles = []PlotStyle{
	// First 8: Professional varied line styles with markers for clear differentiation
	{Color: "red", LineStyle: "dotted", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.5,fill=red"},
	{Color: "blue", LineStyle: "densely dashed", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.3"},
	{Color: "green!70!black", LineStyle: "densely dotted", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.3,fill=green!70!black"},
	{Color: "orange", LineStyle: "dashdotted", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.5,fill=orange"},
	{Color: "purple", LineStyle: "loosely dotted", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.5,fill=purple"},
	{Color: "brown", LineStyle: "densely dashed", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.5"},
	{Color: "black", LineStyle: "densely dotted", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.3"},
	{Color: "cyan", LineStyle: "solid", LineWidth: "thick", Mark: "pentagon", MarkOptions: "scale=0.5"},
	
	// Next 8: Solid lines with same colors and markers for additional containers
	{Color: "magenta", LineStyle: "solid", LineWidth: "thick", Mark: "star", MarkOptions: "scale=0.5,fill=magenta"},
	{Color: "red!70!black", LineStyle: "solid", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.5,fill=red!70!black"},
	{Color: "blue!70!black", LineStyle: "solid", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.3"},
	{Color: "teal", LineStyle: "solid", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.5,fill=teal"},
	{Color: "violet", LineStyle: "solid", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.5,fill=violet"},
	{Color: "olive", LineStyle: "solid", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.5,fill=olive"},
	{Color: "pink", LineStyle: "solid", LineWidth: "thick", Mark: "triangle", MarkOptions: "scale=0.5,fill=pink"},
	{Color: "lime", LineStyle: "solid", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.3"},
	
	// Next 8: Dashed lines for even more containers
	{Color: "red", LineStyle: "dashed", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.5,fill=red"},
	{Color: "blue", LineStyle: "dashed", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.3"},
	{Color: "green!70!black", LineStyle: "dashed", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.5,fill=green!70!black"},
	{Color: "orange", LineStyle: "dashed", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.5,fill=orange"},
	{Color: "purple", LineStyle: "dashed", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.5,fill=purple"},
	{Color: "brown", LineStyle: "dashed", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.5"},
	{Color: "cyan", LineStyle: "dashed", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.3"},
	{Color: "magenta", LineStyle: "dashed", LineWidth: "thick", Mark: "star", MarkOptions: "scale=0.5,fill=magenta"},
	
	// Final 8: Densely dotted for maximum differentiation
	{Color: "red", LineStyle: "densely dotted", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.5,fill=red"},
	{Color: "blue", LineStyle: "densely dotted", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.3"},
	{Color: "green!70!black", LineStyle: "densely dotted", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.5,fill=green!70!black"},
	{Color: "orange", LineStyle: "densely dotted", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.5,fill=orange"},
	{Color: "purple", LineStyle: "densely dotted", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.5,fill=purple"},
	{Color: "brown", LineStyle: "densely dotted", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.5"},
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
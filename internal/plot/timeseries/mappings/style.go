package mappings

type PlotStyle struct {
	Color       string
	LineStyle   string
	LineWidth   string
	Mark        string
	MarkOptions string
}

var ContainerStyles = []PlotStyle{
	{Color: "red!80!black", LineStyle: "solid", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.3,fill=red!80!black"},
	{Color: "blue!55!black", LineStyle: "solid", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.25"},
	{Color: "green!55!black", LineStyle: "solid", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.3,fill=green!55!black"},
	{Color: "orange", LineStyle: "solid", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.3,fill=orange"},
	{Color: "orange!40!white", LineStyle: "solid", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.3,fill=orange!40!white"},
	{Color: "teal", LineStyle: "solid", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.3"},
	{Color: "cyan!50!black", LineStyle: "solid", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.3"},
	{Color: "blue!20!violet", LineStyle: "solid", LineWidth: "thick", Mark: "pentagon", MarkOptions: "scale=0.3"},

	{Color: "orange", LineStyle: "densely dotted", LineWidth: "thick", Mark: "star", MarkOptions: "scale=0.4,fill=orange"},
	{Color: "green!70!black", LineStyle: "densely dotted", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.4,fill=red!70!black"},
	{Color: "blue!50!white", LineStyle: "densely dotted", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.4"},
	{Color: "teal", LineStyle: "densely dotted", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.4,fill=teal"},
	{Color: "violet", LineStyle: "densely dotted", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.4,fill=violet"},
	{Color: "olive!50!black", LineStyle: "densely dotted", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.4,fill=olive!50!black"},
	{Color: "pink!80!red", LineStyle: "densely dotted", LineWidth: "thick", Mark: "triangle", MarkOptions: "scale=0.4,fill=pink!80!red"},
	{Color: "purple", LineStyle: "densely dotted", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.4"},

	{Color: "red", LineStyle: "densely dashed", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.4,fill=red"},
	{Color: "blue", LineStyle: "densely dashed", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.4"},
	{Color: "green!70!black", LineStyle: "densely dashed", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.4,fill=green!70!black"},
	{Color: "orange", LineStyle: "densely dashed", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.4,fill=orange"},
	{Color: "purple", LineStyle: "densely dashed", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.4,fill=purple"},
	{Color: "brown", LineStyle: "densely dashed", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.4"},
	{Color: "cyan", LineStyle: "densely dashed", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.4"},
	{Color: "orange", LineStyle: "densely dashed", LineWidth: "thick", Mark: "star", MarkOptions: "scale=0.4,fill=orange"},

	{Color: "red", LineStyle: "dotted", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.4,fill=red"},
	{Color: "blue", LineStyle: "dotted", LineWidth: "thick", Mark: "square", MarkOptions: "scale=0.4"},
	{Color: "green!70!black", LineStyle: "dotted", LineWidth: "thick", Mark: "*", MarkOptions: "scale=0.4,fill=green!70!black"},
	{Color: "orange", LineStyle: "dotted", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.4,fill=orange"},
	{Color: "purple", LineStyle: "dotted", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.4,fill=purple"},
	{Color: "brown", LineStyle: "dotted", LineWidth: "thick", Mark: "x", MarkOptions: "scale=0.4"},
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

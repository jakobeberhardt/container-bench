package mappings

type PlotStyle struct {
	Color       string
	LineStyle   string
	LineWidth   string
	Mark        string
	MarkOptions string
}

var ContainerStyles = []PlotStyle{
	{Color: "red", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "blue", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "green!60!black", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "orange", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "purple", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "cyan", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "magenta", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "brown", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "teal", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "olive", LineStyle: "solid", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	
	{Color: "red", LineStyle: "dashed", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "blue", LineStyle: "dashed", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "green!60!black", LineStyle: "dashed", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "orange", LineStyle: "dashed", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "purple", LineStyle: "dashed", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "cyan", LineStyle: "dashed", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	
	{Color: "red", LineStyle: "dotted", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "blue", LineStyle: "dotted", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "green!60!black", LineStyle: "dotted", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "orange", LineStyle: "dotted", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "purple", LineStyle: "dotted", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	{Color: "cyan", LineStyle: "dotted", LineWidth: "thick", Mark: "none", MarkOptions: ""},
	
	{Color: "red", LineStyle: "solid", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.7,fill=red"},
	{Color: "blue", LineStyle: "solid", LineWidth: "thick", Mark: "square*", MarkOptions: "scale=0.7,fill=blue"},
	{Color: "green!60!black", LineStyle: "solid", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.7,fill=green!60!black"},
	{Color: "orange", LineStyle: "solid", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.7,fill=orange"},
	{Color: "purple", LineStyle: "solid", LineWidth: "thick", Mark: "pentagon*", MarkOptions: "scale=0.7,fill=purple"},
	{Color: "cyan", LineStyle: "solid", LineWidth: "thick", Mark: "star", MarkOptions: "scale=0.7,fill=cyan"},
	{Color: "magenta", LineStyle: "solid", LineWidth: "thick", Mark: "triangle*", MarkOptions: "scale=0.7,fill=magenta"},
	{Color: "brown", LineStyle: "solid", LineWidth: "thick", Mark: "square*", MarkOptions: "scale=0.7,fill=brown"},
	{Color: "teal", LineStyle: "solid", LineWidth: "thick", Mark: "o", MarkOptions: "scale=0.7,fill=teal"},
	{Color: "olive", LineStyle: "solid", LineWidth: "thick", Mark: "diamond*", MarkOptions: "scale=0.7,fill=olive"},
}

func GetContainerStyle(containerIndex int) PlotStyle {
	if containerIndex < 0 {
		containerIndex = 0
	}
	return ContainerStyles[containerIndex%len(ContainerStyles)]
}

func (ps PlotStyle) ToTikzOptions() string {
	if ps.Mark == "none" || ps.Mark == "" {
		return ps.Color + "," + ps.LineStyle + "," + ps.LineWidth
	}
	return ps.Color + "," + ps.LineStyle + "," + ps.LineWidth + ",mark=" + ps.Mark + ",mark options={" + ps.MarkOptions + "}"
}
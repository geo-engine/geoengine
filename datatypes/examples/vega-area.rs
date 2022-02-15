// use vega_lite_4::{Showable, Vegalite};

// fn main() {
//     let spec = r##"
// {
// 	"$schema": "https://vega.github.io/schema/vega-lite/v4.17.0.json",
// 	"data": {
// 		"values": [{
// 				"y": 0,
// 				"x": "2010-06-01T23:20:20"
// 			},
// 			{
// 				"y": 1,
// 				"x": "2011-01-01"
// 			},
// 			{
// 				"y": 4,
// 				"x": "2012-01-01"
// 			},
// 			{
// 				"y": 9,
// 				"x": "2013-01-01"
// 			},
// 			{
// 				"y": 7,
// 				"x": "2014-01-01"
// 			}
// 		]
// 	},
// 	"description": "Area Plot",
// 	"encoding": {
// 		"x": {
// 			"field": "x",
// 			"title": "X Axis Label",
// 			"type": "temporal"
// 		},
// 		"y": {
// 			"field": "y",
// 			"title": "Y Axis Label",
// 			"type": "quantitative"
// 		}
// 	},
// 	"mark": {
// 		"type": "area",
// 		"line": true,
// 		"point": true
// 	}
// }
// "##;

//     let chart: Vegalite = serde_json::from_str(spec).unwrap();

//     dbg!(&chart);

//     // display the chart using `showata`
//     chart.show().unwrap();

//     // print the vega lite spec
//     eprint!("{}", chart.to_string().unwrap());
// }

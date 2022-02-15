// use ndarray::{array, Array2};
// use std::collections::HashMap;
// use vega_lite_4::{
//     BinEnum, EdEncodingBuilder, Mark, Padding, SelectionDefBuilder, SelectionDefType, Showable,
//     SingleDefUnitChannel, Type, VegaliteBuilder, X2ClassBuilder, XClassBuilder, YClassBuilder,
// };

// fn main() {
//     let values: Array2<f64> = array![
//         [0., 1., 0.],
//         [1., 2., 1.],
//         [2., 3., 4.],
//         [3., 4., 9.],
//         [4., 5., 7.]
//     ];

//     let mut selector_1 = HashMap::new();
//     selector_1.insert(
//         "brush".to_string(),
//         SelectionDefBuilder::default()
//             .encodings(vec![SingleDefUnitChannel::X])
//             .selection_def_type(SelectionDefType::Interval)
//             .build()
//             .unwrap(),
//     );

//     let chart = VegaliteBuilder::default()
//         .width(400.0)
//         .height(200.0)
//         .padding(Padding::Double(5.0))
//         .data(values)
//         .mark(Mark::Bar)
//         .encoding(
//             EdEncodingBuilder::default()
//                 .x(XClassBuilder::default()
//                     .field("data.0")
//                     .title("X Axis Label")
//                     .position_def_type(Type::Quantitative)
//                     .bin(BinEnum::Binned)
//                     .build()
//                     .unwrap())
//                 .x2(X2ClassBuilder::default().field("data.1").build().unwrap())
//                 .y(YClassBuilder::default()
//                     .field("data.2")
//                     .title("Y Axis Label")
//                     .position_def_type(Type::Quantitative)
//                     .build()
//                     .unwrap())
//                 .build()
//                 .unwrap(),
//         )
//         .selection(selector_1)
//         .build()
//         .unwrap();

//     // display the chart using `showata`
//     chart.show().unwrap();

//     // print the vega lite spec
//     eprint!("{}", chart.to_string().unwrap());
// }

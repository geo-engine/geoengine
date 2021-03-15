use ndarray::{array, Array2};
use vega_lite_4::{
    EdEncodingBuilder, Mark, Padding, Showable, Type, VegaliteBuilder, XClassBuilder, YClassBuilder,
};

fn main() {
    let values: Array2<f64> = array![[0., 0.], [1., 1.], [2., 4.], [3., 9.], [4., 7.]];

    let chart = VegaliteBuilder::default()
        .title("Stock price")
        .width(400.0)
        .height(200.0)
        .padding(Padding::Double(5.0))
        .description("A simple line plot.")
        .data(values)
        .mark(Mark::Line)
        .encoding(
            EdEncodingBuilder::default()
                .x(XClassBuilder::default()
                    .field("data.0")
                    .title("X Axis Label")
                    .position_def_type(Type::Quantitative)
                    .build()
                    .unwrap())
                .y(YClassBuilder::default()
                    .field("data.1")
                    .title("Y Axis Label")
                    .position_def_type(Type::Quantitative)
                    .build()
                    .unwrap())
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    // display the chart using `showata`
    chart.show().unwrap();

    // print the vega lite spec
    eprint!("{}", chart.to_string().unwrap());
}

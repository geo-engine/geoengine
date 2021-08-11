#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfusionMatrix<'a>{
    counts: Vec<u8>,
    lables: Vec<&'a str>,
    dim: usize,
}

impl<'a> ConfusionMatrix<'a> {
    ///Initiates an empty confusion matrix with given dimension
    pub fn new(dimensions: usize) -> Self {
        ConfusionMatrix{
            counts: vec![],
            lables: vec![],
            dim: dimensions,
        }
    }
    ///Creates a confusion matrix from given predicted and observed values
    pub fn from<T>(predicted: Vec<T>, actual: Vec<T>, lables: Vec<&'a str>) -> Self{
        if predicted.len() != actual.len() {
            panic!("Invalid dimensions")
        } else {
            let count: u8 = 0;

            
            ConfusionMatrix{
                counts: vec![],
                lables: lables,
                dim: predicted.len(),
            }
        }
    }
}

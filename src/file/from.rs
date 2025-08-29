

// trait FromFile<T> {
//     pub fn from_file(file: &str) {

//     }
// }

pub trait FromFile<T> {
    pub fn from_file(file: &str) -> Result<T, Box<dyn std::error::Error>>;
}
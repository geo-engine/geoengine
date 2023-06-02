use uuid::Uuid;

fn main() {
    let id = Uuid::new_v4();
    print!("{}", id);
    print!("{:x}", id.as_u128());
}

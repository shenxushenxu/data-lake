use rand::Rng;

pub fn random_number(size: usize) -> usize{
    let mut rng = rand::rng();
    let random_number = rng.random_range(0..size);
    
    return random_number;
}
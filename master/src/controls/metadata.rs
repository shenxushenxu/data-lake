use memmap2::Mmap;
use tokio::fs::OpenOptions;
use entity_lib::entity::MasterEntity::TableStructure;
use public_function::MASTER_CONFIG;

pub async fn get_metadata(table_name: &str) -> TableStructure{
    let file_path = format!(
            "{}\\{}",
            MASTER_CONFIG.get("master.data.path").unwrap(),
            table_name);


    let mut file = OpenOptions::new()
        .read(true)
        .open(file_path)
        .await
        .unwrap();

    let mut mmap = unsafe{Mmap::map(&file).unwrap()};

    let metadata_message = &mmap[..];


    let table_structure = bincode::deserialize::<TableStructure>(metadata_message).unwrap();

    return table_structure;
}
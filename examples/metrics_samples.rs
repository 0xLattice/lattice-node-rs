use lattice_node::runtime::metrics::{Sample, render};
fn main() {
    let cases = [
        (vec!["Nexus".into()], Some(12), Some(15), 3, 7, 123.5),
        (vec!["Nexus".into()], None, None, 0, 0, 456.25),
        (
            vec!["Nexus".into(), "quote\"slash\\line\r\n".into()],
            Some(u64::MAX),
            None,
            2,
            4,
            789.125,
        ),
    ];
    let values: Vec<_> = cases
        .into_iter()
        .map(
            |(
                chain_path,
                validated_height,
                weighed_height,
                peers,
                mempool_transactions,
                process_start_time,
            )| {
                render(&Sample {
                    chain_path: &chain_path,
                    validated_height,
                    weighed_height,
                    peers,
                    mempool_transactions,
                    process_start_time,
                })
            },
        )
        .collect();
    println!("{}", serde_json::to_string(&values).unwrap());
}

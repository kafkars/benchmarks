//! Process boundary for the benchmark-only public-client adapter and verifier.

fn main() {
    if let Err(error) = kafkars_benchmark_adapter::run(std::env::args_os().skip(1)) {
        eprintln!("kafkars benchmark adapter failed: {error}");
        std::process::exit(1);
    }
}

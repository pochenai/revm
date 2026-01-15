//!
use std::{thread, time::Duration};

use clap::Parser;

/// `cache-killer` subcommand
#[derive(Parser, Debug, Default)]
pub struct Cmd {
    /// target size in Gb
    #[arg(long)]
    size: usize,
}

impl Cmd {
    ///
    pub fn run(&self) {
        // Allocate a vector to occupy the system memory (adjust according to your machine)
        let size_gb = self.size;
        let size_bytes = size_gb * 1024 * 1024 * 1024;
        let page_size = 4096; // 4 KB page
        let num_pages = size_bytes / page_size;

        println!("Allocating ~{} GB of memory...", size_gb);

        // Initialize the memory with u8
        let mut memory: Vec<u8> = Vec::with_capacity(size_bytes);
        unsafe {
            memory.set_len(size_bytes); // Extend the vector without initializing elements
        }

        // Touch each page to ensure the memory is actually allocated by the OS
        println!("Touching each page to force allocation...");
        for i in 0..num_pages {
            let offset = i * page_size;
            memory[offset] = 0xAA;
        }

        println!("Memory allocated and touched. Entering hold loop...");

        // Continuously access memory in a low-CPU manner:
        // iterate over pages every few seconds to prevent memory from being released
        loop {
            for i in (0..num_pages).step_by(1024) {
                // Skip 1024 pages each step to reduce CPU usage
                let offset = i * page_size;
                memory[offset] ^= 0xFF; // Lightly touch the memory
            }
            thread::sleep(Duration::from_secs(5)); // Pause to avoid busy-waiting
        }
    }
}

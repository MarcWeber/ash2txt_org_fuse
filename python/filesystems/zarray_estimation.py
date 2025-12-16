import json
from pathlib import Path
import numpy as np
import math
import sys
import numpy as np
import math

def estimate_zarray_contents_size(zarr_metadata: dict) -> float:
    """
    Calculates the estimated physical size (in bytes) of a Zarr array 
    based purely on its .zarray metadata dictionary and an assumed 
    compression ratio.

    Args:
        zarr_metadata (dict): The loaded content of the .zarray JSON file.

    Returns:
        float: The estimated physical size in bytes.
        
    Note: Error handling (checking for missing keys, zero division, etc.) 
          is omitted as requested, allowing exceptions to be thrown for 
          invalid input data.
    """
    
    # Extract necessary fields (assuming they exist and are valid)
    shape = zarr_metadata['shape']
    dtype_str = zarr_metadata['dtype']
    compressor = zarr_metadata.get('compressor', {}).get('id')

    compression_ratios = {
        "lz4": 2.0,
        "gzip": 2.5,
        "zlib": 2.5,
        "blosc": 3.0,
        "zstd": 4.0,
        "raw": 1.0
    }

    compression_ratio = compression_ratios.get(compressor, 1)

    # --- 1. Calculate Logical Size (Uncompressed) ---
    
    # Calculate Total Number of Elements
    total_elements = np.prod(shape)
    
    # Determine Bytes Per Element
    dtype = np.dtype(dtype_str)
    bytes_per_element = dtype.itemsize
    
    # Calculate Logical Size
    logical_size_bytes = total_elements * bytes_per_element

    # --- 2. Approximate Physical Size (Estimated) ---
    
    # Estimated Physical Size = Logical Size / Compression Ratio
    estimated_physical_size_bytes = logical_size_bytes / compression_ratio
    
    return estimated_physical_size_bytes, compression_ratio, f"compression {compressor} assumed ratio {compression_ratio}"

# --- Example Usage (How to call the simplified function) ---
if __name__ == '__main__':
    # Define a sample .zarray content
    sample_metadata = {
        "chunks": [100, 100], 
        "compressor": {"id": "lz4"}, 
        "dtype": "<f8", 
        "shape": [500, 1000], 
        "zarr_format": 2
    }
    
    # Logical Size is 4,000,000 bytes (4 MB)
    
    assumed_ratio = 8.0 
    
    try:
        estimated_size = estimate_zarr_bytes(sample_metadata, assumed_ratio)
        
        # Expected result: 4,000,000 / 8.0 = 500,000 bytes
        print(f"Logical Size: {np.prod(sample_metadata['shape']) * np.dtype(sample_metadata['dtype']).itemsize} bytes")
        print(f"Assumed Ratio: {assumed_ratio}:1")
        print(f"Estimated Physical Size (Bytes): {estimated_size}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

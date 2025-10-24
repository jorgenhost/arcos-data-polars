import os
def get_file_size(file_path: str):
    """Get file size in bytes and GB."""
    if os.path.exists(file_path):
        size_bytes = os.path.getsize(file_path)
        size_gb = size_bytes / (1024**3)
        # print(f"{file_path}: {size_bytes:,} bytes ({size_gb:.2f} GB)")
        return round(size_gb, 2)
    return 0
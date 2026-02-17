"""S3 utilities for AWS Glue Jobs"""

import boto3
from urllib.parse import urlparse
from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

def list_s3_files(s3_path: str, extension: Optional[str] =None) -> List[str]:
    """
    List all CSV files inside a S3 folder
    
    Args:
        s3_path (str): S3 path in format 's'3://bucket/key'
        extension (str, optional): Filter files by extension (e.g., '.csv'). 
                                   If None, lists all files. Defaults to None.

    Returns:
        List[str]: A list of full S3 paths (e.g., ['s3://bucket/file1.csv', ...])

    Example:
        >>>files = list_s3_files(s3://bucket/key)  
        >>>print(files)
        ['s3://my-bucket/data/file1.csv', 's3://my-bucket/data/file2.csv']
    """
    if not s3_path.startswith("s3://"):
        raise ValueError("s3_path must start with 's3://'")
    
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    s3_client = boto3.client('s3')

    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')

    try:
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']

                    #Ignore folders
                    if key.endswith('/'):
                        continue

                    #Applies filter
                    if extension and not key.endswith(extension):
                        continue

                    full_path = f"s3://{bucket}/{key}"
                    files.append(full_path)

    except Exception as e:
        logger.error("Failed to list objects", 
                     bucket=bucket, 
                     prefix=prefix, 
                     exc_info=True)
        
        raise e
    
    logger.info("Completed listing",
                 bucket=bucket, 
                files_found_count=len(files),
                s3_path=s3_path
            )
    
    return files

def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """
    Extract bucket and key from S3 path
    
    Args:
        s3_path: S3 path in format s3://bucket/key
        
    Returns:
        Tuple (bucket, key)
        
    Example:
        >>> bucket, key = parse_s3_path('s3://my-bucket/data/file.csv')
        >>> print(bucket, key)
        my-bucket data/file.csv
    """
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key


def get_file_size(s3_path: str) -> int:
    """
    Get file size in bytes from S3
    
    Args:
        s3_path: Complete S3 file path
        
    Returns:
        File size in bytes
        
    Raises:
        FileNotFoundError: If file does not exist
        
    Example:
        >>> size = get_file_size('s3://my-bucket/data/file.csv')
        >>> print(f"Size: {size / (1024*1024):.2f} MB")
        Size: 14.23 MB
    """
    bucket, key = parse_s3_path(s3_path)
    s3_client = boto3.client('s3')
    
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(f"File not found: {s3_path}")
    except Exception as e:
        raise Exception(f"Error accessing S3: {str(e)}")


def format_size(size_bytes: int) -> str:
    """
    Format byte size to human-readable string
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted string (e.g., "15.32 MB")
        
    Example:
        >>> print(format_size(15823456))
        15.09 MB
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024*1024):.2f} MB"
    else:
        return f"{size_bytes / (1024*1024*1024):.2f} GB"


def list_files_with_metadata(
    s3_folder_path: str, 
    extension: str = '.csv'
) -> List[Dict]:
    """
    List files with complete metadata from S3 folder
    
    Args:
        s3_folder_path: S3 folder path
        extension: File extension to filter (default: .csv)
        
    Returns:
        List of dicts with file metadata
        
    Example:
        >>> files = list_files_with_metadata('s3://bucket/data/')
        >>> for f in files:
        ...     print(f"{f['name']}: {f['size_formatted']}")
        file1.csv: 14.23 MB
        file2.csv: 8.45 MB
    """
    bucket, prefix = parse_s3_path(s3_folder_path)
    
    # Ensure prefix ends with /
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    s3_client = boto3.client('s3')
    
    files = []
    continuation_token = None
    
    # Handle pagination for large folders
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket, 
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                
                # Filter by extension and ignore folders
                if key.endswith(extension) and not key.endswith('/'):
                    full_path = f"s3://{bucket}/{key}"
                    file_name = key.split('/')[-1]
                    size_bytes = obj['Size']
                    
                    files.append({
                        'path': full_path,
                        'key': key,
                        'name': file_name,
                        'size_bytes': size_bytes,
                        'size_kb': round(size_bytes / 1024, 2),
                        'size_mb': round(size_bytes / (1024*1024), 2),
                        'size_gb': round(size_bytes / (1024*1024*1024), 4),
                        'size_formatted': format_size(size_bytes),
                        'last_modified': obj['LastModified'].isoformat()
                    })
        
        # Check if there are more results
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    
    # Sort by name
    files.sort(key=lambda x: x['name'])
    
    logger.info(
        "files_listed",
        total_files=len(files),
        folder=s3_folder_path,
        extension=extension
    )
    
    return files


def get_folder_statistics(s3_folder_path: str) -> Dict:
    """
    Calculate total size and statistics for an S3 folder
    
    Args:
        s3_folder_path: S3 folder path
        
    Returns:
        Dict with folder statistics
        
    Example:
        >>> stats = get_folder_statistics('s3://bucket/data/')
        >>> print(f"Total: {stats['total_formatted']}")
        Total: 127.45 MB
    """
    files = list_files_with_metadata(s3_folder_path, extension='')
    
    if not files:
        return {
            'total_files': 0,
            'total_bytes': 0,
            'total_mb': 0.0,
            'total_gb': 0.0,
            'total_formatted': '0 B',
            'files': []
        }
    
    total_bytes = sum(f['size_bytes'] for f in files)
    
    return {
        'total_files': len(files),
        'total_bytes': total_bytes,
        'total_kb': round(total_bytes / 1024, 2),
        'total_mb': round(total_bytes / (1024*1024), 2),
        'total_gb': round(total_bytes / (1024*1024*1024), 2),
        'total_formatted': format_size(total_bytes),
        'average_size_mb': round(total_bytes / len(files) / (1024*1024), 2),
        'largest_file': max(files, key=lambda x: x['size_bytes']),
        'smallest_file': min(files, key=lambda x: x['size_bytes']),
        'files': files
    }


def check_file_exists(s3_path: str) -> bool:
    """
    Check if a file exists in S3
    
    Args:
        s3_path: Complete S3 file path
        
    Returns:
        True if file exists, False otherwise
        
    Example:
        >>> if check_file_exists('s3://bucket/file.csv'):
        ...     print("File exists")
    """
    bucket, key = parse_s3_path(s3_path)
    s3_client = boto3.client('s3')
    
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception as e:
        logger.error(f"Error checking file existence: {str(e)}")
        return False


def get_file_metadata(s3_path: str) -> Dict:
    """
    Get complete metadata for a single file
    
    Args:
        s3_path: Complete S3 file path
        
    Returns:
        Dict with file metadata
        
    Example:
        >>> metadata = get_file_metadata('s3://bucket/file.csv')
        >>> print(metadata['size_formatted'])
        14.23 MB
    """
    bucket, key = parse_s3_path(s3_path)
    s3_client = boto3.client('s3')
    
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        
        size_bytes = response['ContentLength']
        file_name = key.split('/')[-1]
        
        return {
            'path': s3_path,
            'bucket': bucket,
            'key': key,
            'name': file_name,
            'size_bytes': size_bytes,
            'size_kb': round(size_bytes / 1024, 2),
            'size_mb': round(size_bytes / (1024*1024), 2),
            'size_gb': round(size_bytes / (1024*1024*1024), 4),
            'size_formatted': format_size(size_bytes),
            'last_modified': response['LastModified'].isoformat(),
            'content_type': response.get('ContentType', 'unknown'),
            'etag': response.get('ETag', '').strip('"')
        }
    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(f"File not found: {s3_path}")
    except Exception as e:
        raise Exception(f"Error getting file metadata: {str(e)}")


# For testing purposes
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python s3_helpers.py s3://bucket/path/")
        sys.exit(1)
    
    s3_path = sys.argv[1]
    
    # Check if it's a file or folder
    if s3_path.endswith('/'):
        # Folder
        print(f"\n📊 Analyzing folder: {s3_path}\n")
        stats = get_folder_statistics(s3_path)
        
        print(f"Total files: {stats['total_files']}")
        print(f"Total size: {stats['total_formatted']}")
        print(f"Average size: {stats['average_size_mb']} MB")
        
        if stats['files']:
            print(f"\n📁 Files:")
            for file in stats['files']:
                print(f"  - {file['name']}: {file['size_formatted']}")
            
            print(f"\n📈 Largest file: {stats['largest_file']['name']} ({stats['largest_file']['size_formatted']})")
            print(f"📉 Smallest file: {stats['smallest_file']['name']} ({stats['smallest_file']['size_formatted']})")
    else:
        # Single file
        print(f"\n📄 Analyzing file: {s3_path}\n")
        metadata = get_file_metadata(s3_path)
        
        print(f"Name: {metadata['name']}")
        print(f"Size: {metadata['size_formatted']}")
        print(f"Last modified: {metadata['last_modified']}")
        print(f"Content type: {metadata['content_type']}")
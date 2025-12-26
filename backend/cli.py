#!/usr/bin/env python3
"""
YXDB Converter CLI Tool
Command-line interface for converting YXDB files to SQL
"""

import argparse
import sys
import os
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.yxdb_parser import YxdbParser, MockYxdbParser
from services.sql_converter import SqlConverter


def format_bytes(size: int) -> str:
    """Format bytes to human readable string"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def progress_callback(progress):
    """Print progress to console"""
    bar_length = 40
    filled = int(bar_length * progress.percentage / 100)
    bar = '█' * filled + '░' * (bar_length - filled)
    
    sys.stdout.write(f"\r[{bar}] {progress.percentage:.1f}% - {progress.message}")
    sys.stdout.flush()
    
    if progress.percentage >= 100 or progress.status == 'error':
        print()


def convert_file(input_path: str, output_path: str, table_name: str = 'data', batch_size: int = 10000):
    """Convert a YXDB file to SQLite"""
    print(f"\n📁 Input:  {input_path}")
    print(f"📦 Output: {output_path}")
    print(f"📋 Table:  {table_name}")
    print()
    
    # Check input file
    if not os.path.exists(input_path):
        print(f"❌ Error: Input file not found: {input_path}")
        return False
    
    file_size = os.path.getsize(input_path)
    print(f"📊 File size: {format_bytes(file_size)}")
    
    # Initialize parser
    print("\n🔍 Reading file schema...")
    try:
        parser = YxdbParser(input_path)
    except ImportError:
        print("⚠️  yxdb library not installed. Using mock data.")
        parser = MockYxdbParser(input_path, num_records=10000)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False
    
    # Show schema
    schema = parser.get_schema()
    print(f"✅ Found {len(schema)} columns:")
    for col in schema[:10]:
        print(f"   • {col['name']} ({col['original_type']} → {col['sql_type']})")
    if len(schema) > 10:
        print(f"   ... and {len(schema) - 10} more")
    
    # Estimate records
    estimated_records = max(file_size // 200, 100)
    print(f"\n📊 Estimated records: ~{estimated_records:,}")
    
    # Convert
    print("\n🔄 Converting to SQLite...")
    start_time = time.time()
    
    converter = SqlConverter(output_path, table_name)
    
    try:
        final_progress = converter.convert_from_parser(
            parser,
            batch_size=batch_size,
            progress_callback=progress_callback,
            estimated_records=estimated_records
        )
        
        elapsed = time.time() - start_time
        
        if final_progress.status == 'completed':
            output_size = os.path.getsize(output_path)
            records_per_sec = final_progress.processed_records / elapsed if elapsed > 0 else 0
            
            print(f"\n✅ Conversion successful!")
            print(f"   📊 Records: {final_progress.processed_records:,}")
            print(f"   📦 Output size: {format_bytes(output_size)}")
            print(f"   ⏱️  Time: {elapsed:.2f}s ({records_per_sec:,.0f} records/sec)")
            print(f"\n   Database saved to: {output_path}")
            return True
        else:
            print(f"\n❌ Conversion failed: {final_progress.error}")
            return False
            
    except Exception as e:
        print(f"\n❌ Error during conversion: {e}")
        return False


def show_schema(input_path: str):
    """Show schema of a YXDB file"""
    if not os.path.exists(input_path):
        print(f"❌ Error: File not found: {input_path}")
        return
    
    print(f"\n📁 File: {input_path}")
    print(f"📊 Size: {format_bytes(os.path.getsize(input_path))}")
    
    try:
        parser = YxdbParser(input_path)
    except ImportError:
        print("⚠️  yxdb library not installed.")
        return
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    schema = parser.get_schema()
    print(f"\n📋 Schema ({len(schema)} columns):\n")
    
    print(f"{'#':<4} {'Name':<30} {'YXDB Type':<15} {'SQL Type':<10} {'Size':<6}")
    print("-" * 70)
    
    for i, col in enumerate(schema, 1):
        size = col.get('size', 0)
        size_str = str(size) if size > 0 else '-'
        print(f"{i:<4} {col['name']:<30} {col['original_type']:<15} {col['sql_type']:<10} {size_str:<6}")


def main():
    parser = argparse.ArgumentParser(
        description='YXDB to SQL Converter - Convert Alteryx database files to SQLite',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Convert a file:
    python cli.py convert input.yxdb output.db

  Show schema:
    python cli.py schema input.yxdb

  Convert with custom table name:
    python cli.py convert input.yxdb output.db --table my_data
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Convert command
    convert_parser = subparsers.add_parser('convert', help='Convert YXDB to SQLite')
    convert_parser.add_argument('input', help='Input YXDB file path')
    convert_parser.add_argument('output', help='Output SQLite database path')
    convert_parser.add_argument('--table', '-t', default='data', help='Table name (default: data)')
    convert_parser.add_argument('--batch', '-b', type=int, default=10000, help='Batch size (default: 10000)')
    
    # Schema command
    schema_parser = subparsers.add_parser('schema', help='Show YXDB file schema')
    schema_parser.add_argument('input', help='Input YXDB file path')
    
    args = parser.parse_args()
    
    if args.command == 'convert':
        success = convert_file(args.input, args.output, args.table, args.batch)
        sys.exit(0 if success else 1)
    elif args.command == 'schema':
        show_schema(args.input)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

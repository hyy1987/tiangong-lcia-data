import glob
import gzip
import json
import os
from pathlib import Path


def extract_version_from_json(json_file_path):
    """从JSON文件中提取dataSetVersion"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 提取版本号: LCIAMethodDataSet.administrativeInformation.publicationAndOwnership.common:dataSetVersion
        lcia_dataset = data.get('LCIAMethodDataSet', {})
        admin_info = lcia_dataset.get('administrativeInformation', {})
        publication = admin_info.get('publicationAndOwnership', {})
        version = publication.get('common:dataSetVersion', '')
        
        return version
    except Exception as e:
        print(f"提取版本号失败 {json_file_path}: {e}")
        return None

def compress_json_files():
    """压缩data2目录中的JSON文件为GZ格式"""
    
    # 源目录和目标目录
    source_dir = os.path.join("data", "json")
    target_dir = os.path.join("data", "json_compressed")
    
    # 确保目标目录存在
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    
    # 获取所有JSON文件
    json_pattern = os.path.join(source_dir, "*.json")
    json_files = glob.glob(json_pattern)
    
    if not json_files:
        print(f"在 {source_dir} 中未找到JSON文件")
        return
    
    print(f"找到 {len(json_files)} 个JSON文件")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    for json_file in json_files:
        try:
            # 获取文件名（不含扩展名）作为UUID
            file_path = Path(json_file)
            uuid = file_path.stem  # 文件名不含扩展名
            
            print(f"\n处理文件: {file_path.name}")
            print(f"UUID: {uuid}")
            
            # 提取版本号
            version = extract_version_from_json(json_file)
            if not version:
                print(f"⚠ 无法提取版本号，跳过文件: {file_path.name}")
                error_count += 1
                continue
            
            print(f"版本号: {version}")
            
            # 构建新的文件名: uuid_version.json.gz
            new_filename = f"{uuid}_{version}.json.gz"
            target_file = os.path.join(target_dir, new_filename)
            
            # 检查目标文件是否已存在
            if os.path.exists(target_file):
                print(f"⏭ 文件已存在，跳过: {new_filename}")
                skipped_count += 1
                continue
            
            # 读取原始JSON文件内容
            with open(json_file, 'r', encoding='utf-8') as f:
                json_content = f.read()
            
            # 压缩并保存为GZ文件
            with gzip.open(target_file, 'wt', encoding='utf-8') as f:
                f.write(json_content)
            
            # 计算压缩效果
            original_size = os.path.getsize(json_file)
            compressed_size = os.path.getsize(target_file)
            compression_ratio = (1 - compressed_size / original_size) * 100
            
            print(f"✓ 压缩完成: {new_filename}")
            print(f"  原始大小: {original_size:,} 字节")
            print(f"  压缩大小: {compressed_size:,} 字节")
            print(f"  压缩率: {compression_ratio:.1f}%")
            
            processed_count += 1
            
        except Exception as e:
            print(f"✗ 处理文件出错 {json_file}: {e}")
            error_count += 1
    
    # 输出总结
    print(f"\n{'='*50}")
    print(f"压缩完成统计:")
    print(f"  成功处理: {processed_count} 个文件")
    print(f"  跳过文件: {skipped_count} 个文件")
    print(f"  出错文件: {error_count} 个文件")
    print(f"  总文件数: {len(json_files)} 个文件")
    print(f"压缩文件保存在: {target_dir}")

def list_compressed_files():
    """列出压缩后的文件"""
    target_dir = os.path.join("data", "json_compressed")
    
    if not os.path.exists(target_dir):
        print("压缩目录不存在")
        return
    
    gz_files = glob.glob(os.path.join(target_dir, "*.gz"))
    
    print(f"\n压缩文件列表 ({len(gz_files)} 个文件):")
    print("-" * 60)
    
    total_size = 0
    for gz_file in sorted(gz_files):
        file_path = Path(gz_file)
        file_size = os.path.getsize(gz_file)
        total_size += file_size
        
        # 从文件名提取UUID和版本
        name_parts = file_path.stem.replace('.json', '').split('_')
        if len(name_parts) >= 2:
            uuid = name_parts[0]
            version = '_'.join(name_parts[1:])
            print(f"{file_path.name}")
            print(f"  UUID: {uuid}")
            print(f"  版本: {version}")
            print(f"  大小: {file_size:,} 字节")
        else:
            print(f"{file_path.name} (文件名格式异常)")
        print()
    
    print(f"总大小: {total_size:,} 字节 ({total_size/1024/1024:.2f} MB)")

if __name__ == "__main__":
    print("开始压缩JSON文件...")
    compress_json_files()
    list_compressed_files()
    print("处理完成！")

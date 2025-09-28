import glob
import json
import os


def extract_model_name_from_json(json_file_path):
    """从JSON文件中提取modelName"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 查找LCIAMethodDataSet.LCIAMethodInformation.impactModel.modelName
        if isinstance(data, dict):
            # 检查多层嵌套结构
            lcia_dataset = data.get('LCIAMethodDataSet', {})
            if isinstance(lcia_dataset, dict):
                lcia_info = lcia_dataset.get('LCIAMethodInformation', {})
                if isinstance(lcia_info, dict):
                    impact_model = lcia_info.get('impactModel', {})
                    if isinstance(impact_model, dict):
                        model_name = impact_model.get('modelName')
                        if model_name:
                            return model_name
        
        # 如果没有找到，返回None
        return None
    except Exception as e:
        print(f"读取文件 {json_file_path} 出错: {e}")
        return None

def extract_description_from_json(json_file_path):
    """从JSON文件中提取描述信息"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 提取描述信息
        lcia_dataset = data.get('LCIAMethodDataSet', {})
        if isinstance(lcia_dataset, dict):
            lcia_info = lcia_dataset.get('LCIAMethodInformation', {})
            if isinstance(lcia_info, dict):
                dataset_info = lcia_info.get('dataSetInformation', {})
                if isinstance(dataset_info, dict):
                    # 尝试获取name字段
                    name_info = dataset_info.get('common:name', {})
                    description = name_info
                    if description:
                        return description
        
        return "Unknown"
    except Exception as e:
        print(f"提取描述失败 {json_file_path}: {e}")
        return "Unknown"

def extract_version_from_json(json_file_path):
    """从JSON文件中提取版本信息"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 提取版本信息
        lcia_dataset = data.get('LCIAMethodDataSet', {})
        if isinstance(lcia_dataset, dict):
            admin_info = lcia_dataset.get('administrativeInformation', {})
            if isinstance(admin_info, dict):
                publication = admin_info.get('publicationAndOwnership', {})
                if isinstance(publication, dict):
                    version = publication.get('common:dataSetVersion', '')
                    if version:
                        return version
        
        return "Unknown"
    except Exception as e:
        print(f"提取版本失败 {json_file_path}: {e}")
        return "Unknown"

def calculate_file_size(file_size_bytes):
    """计算文件大小的可读格式"""
    if file_size_bytes < 1024:
        return f"{file_size_bytes}B"
    elif file_size_bytes < 1024 * 1024:
        return f"{file_size_bytes / 1024:.1f}KB"
    else:
        return f"{file_size_bytes / (1024 * 1024):.1f}MB"

def update_list_with_model_names():
    """基于data2目录中的JSON文件重新生成list.json"""
    
    # 读取list_order.txt文件获取排序顺序
    order_list = []
    try:
        with open("data/list_order.txt", 'r', encoding='utf-8') as f:
            order_list = [line.strip() for line in f.readlines() if line.strip()]
        print(f"从list_order.txt读取到 {len(order_list)} 个文件顺序")
    except FileNotFoundError:
        print("⚠ 未找到list_order.txt文件，将使用默认排序")
    
    # 获取所有JSON文件
    json_files = glob.glob("data/json/*.json")
    
    if not json_files:
        print("在data2目录中未找到JSON文件")
        return
    
    print(f"找到 {len(json_files)} 个JSON文件")
    
    # 创建新的list数据结构
    list_data = {
        "metadata": {
            "description": "LCIA Methods compressed files list",
            "totalFiles": len(json_files),
            "format": "gzip compressed JSON",
        },
        "files": []
    }
    
    total_original_size = 0
    total_compressed_size = 0
    updated_count = 0
    error_count = 0
    
    # 创建文件映射字典，用于按顺序处理
    file_entries = {}
    
    for json_file in json_files:
        try:
            file_path = os.path.basename(json_file)
            file_uuid = os.path.splitext(file_path)[0]  # 去掉.json扩展名得到UUID
            
            print(f"\n处理文件: {file_path}")
            
            # 提取各种信息
            model_name = extract_model_name_from_json(json_file)
            description = extract_description_from_json(json_file)
            version = extract_version_from_json(json_file)
            
            # 计算原始文件大小
            original_size = os.path.getsize(json_file)
            total_original_size += original_size
            
            # 检查对应的压缩文件
            compressed_filename = f"{file_uuid}_{version}.json.gz"
            compressed_file_path = os.path.join("data", "compressed", compressed_filename)
            
            if os.path.exists(compressed_file_path):
                compressed_size = os.path.getsize(compressed_file_path)
                total_compressed_size += compressed_size
                size_display = calculate_file_size(compressed_size)
            else:
                size_display = calculate_file_size(original_size)
                print(f"⚠ 未找到压缩文件: {compressed_filename}")
            
            # 创建文件条目
            file_entry = {
                "filename": compressed_filename,
                "id": file_uuid,
                "version": version,
                "size": size_display,
                "description": description,
                "impactModel": model_name if model_name else "Unknown"
            }
            
            # 将文件条目存储到字典中，以compressed_filename为键
            file_entries[compressed_filename] = file_entry
            
            print(f"✓ UUID: {file_uuid}")
            print(f"  版本: {version}")
            print(f"  描述: {description}")
            print(f"  模型: {model_name if model_name else 'Unknown'}")
            print(f"  大小: {size_display}")
            
            updated_count += 1
            
        except Exception as e:
            print(f"✗ 处理文件出错 {json_file}: {e}")
            error_count += 1
    
    # 按照list_order.txt的顺序添加文件到files数组
    if order_list:
        print(f"\n按照list_order.txt顺序排列文件...")
        for filename in order_list:
            if filename in file_entries:
                list_data["files"].append(file_entries[filename])
                print(f"✓ 已按顺序添加: {filename}")
            else:
                print(f"⚠ 在处理结果中未找到: {filename}")
        
        # 添加任何未在order_list中的文件
        for filename, entry in file_entries.items():
            if filename not in order_list:
                list_data["files"].append(entry)
                print(f"⚠ 额外添加未在order_list中的文件: {filename}")
    else:
        # 如果没有order_list，使用默认排序
        print(f"\n使用默认排序...")
        for filename in sorted(file_entries.keys()):
            list_data["files"].append(file_entries[filename])
    
    # 计算总体压缩率
    if total_original_size > 0 and total_compressed_size > 0:
        compression_ratio = int((1 - total_compressed_size / total_original_size) * 100)
        list_data["metadata"]["compressionRatio"] = f"{compression_ratio}%"
        list_data["metadata"]["originalSize"] = calculate_file_size(total_original_size)
        list_data["metadata"]["totalSize"] = calculate_file_size(total_compressed_size)
    
    # 保存list.json
    list_file_path = "data/list.json"
    with open(list_file_path, 'w', encoding='utf-8') as f:
        json.dump(list_data, f, ensure_ascii=False, indent=4)
    
    print(f"\n=== 更新完成 ===")
    print(f"成功处理: {updated_count} 个文件")
    print(f"出错文件: {error_count} 个文件")
    print(f"原始总大小: {calculate_file_size(total_original_size)}")
    print(f"压缩总大小: {calculate_file_size(total_compressed_size)}")
    if total_original_size > 0:
        print(f"压缩率: {int((1 - total_compressed_size / total_original_size) * 100)}%")
    print(f"已保存到: {list_file_path}")

def show_model_name_summary():
    """显示modelName的统计摘要"""
    with open("data/list.json", 'r', encoding='utf-8') as f:
        list_data = json.load(f)
    
    model_names = {}
    for file_entry in list_data['files']:
        model_name = file_entry.get('modelName', 'Unknown')
        if model_name in model_names:
            model_names[model_name] += 1
        else:
            model_names[model_name] = 1
    
    print(f"\n=== ModelName统计 ===")
    for model_name, count in model_names.items():
        print(f"{model_name}: {count} 个文件")

if __name__ == "__main__":
    print("基于data2目录重新生成list.json...")
    update_list_with_model_names()
    show_model_name_summary()

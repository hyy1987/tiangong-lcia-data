
import os
import glob
import json
from pathlib import Path

# 获取LCIAMethodDataSet.characterisationFactors.factor
def get_flow_factors(json_file_path):
    """从JSON文件中提取LCIAMethodDataSet.characterisationFactors.factor"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 提取characterisationFactors
        lcia_dataset = data.get('LCIAMethodDataSet', {})
        lcia_info = lcia_dataset.get('LCIAMethodInformation', {})
        ds_info = lcia_info.get('dataSetInformation', {})
        id = ds_info.get('common:UUID', '')
        char_factors = lcia_dataset.get('characterisationFactors', {})
        factors = char_factors.get('factor', [])
        
        # 返回factor列表中的特定属性
        my_factors = []
        if isinstance(factors, list):
            for factor in factors:
                ff = factor.get("referenceToFlowDataSet", {})
                my_factors.append({
                    "@refObjectId": ff.get("@refObjectId", None),
                    "@version": ff.get("@version", None),
                    "exchangeDirection": factor.get("exchangeDirection", None),
                    "factor": [{
                        "key": id,
                        "value": factor.get("meanValue", None),
                    }]
                })
        elif isinstance(factors, dict):
                factor = factors
                ff = factor.get("referenceToFlowDataSet", {})
                my_factors.append({
                    "@refObjectId": ff.get("@refObjectId", None),
                    "@version": ff.get("@version", None),
                    "exchangeDirection": factor.get("exchangeDirection", None),
                    "factor": [{
                        "key": id,
                        "value": factor.get("meanValue", None),
                    }]
                })

        return my_factors

    except Exception as e:
        print(f"提取factor失败 {json_file_path}: {e}")
        return None

# 获取data/json目录下所有JSON文件内容，并调用get_flow_factors
def get_all_flow_factors():
    source_dir = os.path.join("data", "json")
    json_pattern = os.path.join(source_dir, "*.json")
    json_files = glob.glob(json_pattern)
    
    if not json_files:
        print(f"在 {source_dir} 中未找到JSON文件")
        return []
    
    print(f"找到 {len(json_files)} 个JSON文件，开始处理...")
    
    list_factors = []
    for i, json_file in enumerate(json_files, 1):
        file_name = os.path.basename(json_file)
        print(f"[{i}/{len(json_files)}] 处理文件: {file_name}")
        
        factors = get_flow_factors(json_file)
        if factors:
            list_factors.append(factors)
            print(f"  ✓ 提取到 {len(factors)} 个因子")
        else:
            print(f"  ✗ 提取失败")

    if not list_factors:
        print("没有成功提取到任何因子")
        return []

    print(f"\n开始合并因子数据...")
    merge_factors = list_factors[0]
    
    # 创建一个字典来快速查找已存在的因子，避免嵌套循环
    factor_index = {}
    for idx, mf in enumerate(merge_factors):
        key = (mf["@refObjectId"], mf["@version"], mf["exchangeDirection"])
        factor_index[key] = idx
    
    for i, factors in enumerate(list_factors[1:], 2):
        print(f"合并第 {i} 个文件的因子... (共 {len(factors)} 个)")
        
        merged_count = 0
        new_count = 0
        
        for factor in factors:
            key = (factor["@refObjectId"], factor["@version"], factor["exchangeDirection"])
            
            if key in factor_index:
                # 找到匹配的因子，合并数据
                idx = factor_index[key]
                merge_factors[idx]["factor"].extend(factor["factor"])
                merged_count += 1
            else:
                # 新因子，添加到列表和索引
                merge_factors.append(factor)
                factor_index[key] = len(merge_factors) - 1
                new_count += 1
        
        print(f"  合并了 {merged_count} 个已有因子，新增 {new_count} 个因子")
    
    print(f"合并完成，总共 {len(merge_factors)} 个唯一因子")
    return merge_factors
        
if __name__ == "__main__":
    all_factors = get_all_flow_factors()
    # 把all_factors写入data/flow_factors.json
    output_file = os.path.join("data", "flow_factors.json")
    output_gz_file = output_file + '.gz'
    
    # 删除已存在的文件
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"已删除旧文件: {output_file}")
    
    if os.path.exists(output_gz_file):
        os.remove(output_gz_file)
        print(f"已删除旧文件: {output_gz_file}")
    
    # 写入新的JSON文件
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_factors, f, ensure_ascii=False, indent=4)

    # 把flow_factors.json压缩为gz
    import gzip
    with open(output_file, 'rb') as f_in:
        with gzip.open(output_gz_file, 'wb') as f_out:
            f_out.writelines(f_in)
    
    print(f"已将所有factor写入 {output_file}")
    print(f"已生成压缩文件 {output_gz_file}")

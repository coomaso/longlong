import requests
import time
import json
import random 

# 设置请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://www.zhulong.com/',
    'Accept': 'application/json, text/plain, */*',
    # 根据请求添加了 Cookie 头
    'Cookie': 'Hm_lvt_b4c6201caa3cb2837f622d668e688cfd=1751961635,1753093327,1753604656,1753919554; HMACCOUNT=FB83163D0F4ABA49; Hm_lpvt_b4c6201caa3cb2837f622d668e688cfd=1753944243; agency_id=2; PHPSESSID=kn642ohvac1s51eikt07d9pq13; agency_id=2; app_id=1; uuid=110758d4-723c-46ae-b6e9-3c6ec7cd1c2d; uid=15789975; username=%E6%BD%87%E6%B4%92%E9%94%8B; category_id=37; access_token=5423fbc605314aefbc3ee33ae9f68fd625f0bd88; ZLID=e8b5rpjaghEmoW2ylvfXapBetkxKx5LWLY0_Oem_9DIzXg50W5bFk17QEC91uTWA; province_id=13; city_id=195; reg_time=2022-02-15+16%3A55%3A02; ip_province_id=13; ip_city_id=195; zluuid=768F828E-8A1D-4AB3-99AC-FA941D8FCA29; bbs-uid=15789975; bbs-username=%E6%BD%87%E6%B4%92%E9%94%8B; only_uuid=19f3a264-dc31-2feb-02f7-3dc1a6e68477; Hm_lvt_49541358a94eea717001819b500f76c8=1749719683; Hm_lvt_918b0a71c46422e304aa32471c74fd98=1749719683; Hm_lvt_09566c1e6ae94ce8c4f40a54aed90f86=1749719704; Hm_lvt_d165b0df9d8b576128f53e461359a530=1749719704; fd=https%3A//www.baidu.com/link%3Furl%3DuQ4PZ636ul8QRiIsegAcrognuEADven6VHTYhbAj6BuMBSa2GfajoXzOzRycIYC9ZNozJxKYHqwyTe16XLfTTa%26wd%3D%26eqid%3Dc25b1d6c009856cc00000006684a9ad2; latest_search=%E4%B8%80%E5%BB%BA%E5%BB%BA%E7%AD%91%E5%9B%BE%E6%96%87%E5%AD%A6%E4%B9%A0%E9%80%9A2025; pcid=5990896829; agency_id=2; historicalSearchKeyWords=[%22%E8%8A%B1%E7%AF%AE%E5%A4%96%E6%9E%B6%20%E5%9B%9B%E4%BB%A3%E5%BB%BA%E7%AD%91%22%2C%22%E5%8C%BB%E9%99%A2%E6%8A%80%E6%9C%AF%E6%A0%87%22%2C%22%E6%B2%88%E9%98%B3%E4%B8%AD%E5%BE%B7%E5%9B%AD%E5%9F%BA%E7%A1%80%E5%8F%8A%E5%85%AC%E5%85%B1%E8%AE%BE%E6%96%BD%E5%BB%BA%E8%AE%BEPPP%E9%A1%B9%E7%9B%AE%22%2C%22%E9%9B%A8%E6%B1%A1%E5%88%86%E6%B5%81%22%2C%22%E4%B8%B4%E6%97%B6%E7%94%A8%E6%B0%B4%E6%96%B9%E6%A1%88%22%2C%22EPC+O%E6%8A%80%E6%9C%AF%E6%A0%87%22]; f=pc_bbsgroup_elite_0_1; fl=pc_down_group_0_1%252Cpc_bbsgroup_elite_0_1; sid=DC968705-BD59-45DB-8A6F-370A5440088A; b_visit=1753944243561'
}

# 新增：重试机制的配置
MAX_RETRIES = 3  # 最大重试次数
INITIAL_RETRY_DELAY = 5  # 初始重试延迟（秒）

def make_request_with_retry(method, url, headers, params, timeout, retries=0):
    """
    带重试机制的 HTTP 请求函数。
    method: 'get' 或 'post'
    url: 请求的 URL
    headers: 请求头
    params: 请求参数
    timeout: 请求超时时间
    retries: 当前重试次数
    """
    try:
        response = requests.request(method, url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()  # 检查 HTTP 状态码，如果不是 2xx 则抛出异常
        return response
    except requests.exceptions.RequestException as e:
        if retries < MAX_RETRIES:
            delay = INITIAL_RETRY_DELAY * (2 ** retries) + random.uniform(0, 2) # 指数退避加随机抖动
            print(f"请求失败: {url} - {e}。第 {retries + 1}/{MAX_RETRIES} 次重试，等待 {delay:.2f} 秒...")
            time.sleep(delay)
            return make_request_with_retry(method, url, headers, params, timeout, retries + 1)
        else:
            print(f"请求失败: {url} - {e}。已达到最大重试次数 ({MAX_RETRIES})，放弃请求。")
            raise # 抛出异常，让调用者处理最终失败

def get_categories():
    """获取所有一级分类"""
    url = "https://www.zhulong.com/bbs/prod-api/home/resource/category"
    params = {'t': int(time.time() * 1000)}
    
    try:
        response = make_request_with_retry('get', url, headers, params, timeout=10) # 使用重试函数
        data = response.json()
        
        if data.get('errNo') == 0:
            return data['result']
        else:
            print(f"获取分类失败: {data.get('msg')}")
            return []
    except Exception as e:
        print(f"请求分类出错: {str(e)}")
        return []

def get_subcategories(category_id):
    """获取指定分类的子分类"""
    url = "https://www.zhulong.com/bbs/prod-api/home/resource/group"
    params = {
        'category_id': category_id,
        't': int(time.time() * 1000)
    }
    
    try:
        response = make_request_with_retry('get', url, headers, params, timeout=10) # 使用重试函数
        data = response.json()
        
        if data.get('errNo') == 0:
            return data['result']
        else:
            print(f"获取子分类失败 (category_id={category_id}): {data.get('msg')}")
            return []
    except Exception as e:
        print(f"请求子分类出错: {str(e)}")
        return []

def get_threads(group_id, page=1, limit=10):
    """获取指定子分类的帖子内容"""
    url = "https://www.zhulong.com/bbs/prod-api/group/group/getGroupThreadTag"
    params = {
        'gid': group_id,
        'page': page,
        'limit': limit,
        'type': 'hot',
        't': int(time.time() * 1000)
    }
    
    try:
        response = make_request_with_retry('get', url, headers, params, timeout=10) # 使用重试函数
        data = response.json()
        
        if data.get('errNo') == 0:
            # 提取帖子列表
            thread_list = data.get('result', {}).get('thread', {}).get('list', [])
            # 提取总页数用于翻页
            total_pages = data.get('result', {}).get('thread', {}).get('maxPage', 1)
            return thread_list, total_pages
        else:
            print(f"获取帖子失败 (group_id={group_id}, page={page}): {data.get('msg')}")
            return [], 1
    except Exception as e:
        print(f"请求帖子出错: {str(e)}")
        return [], 1

def get_thread_detail(tid):
    """获取帖子详细信息"""
    url = f"https://www.zhulong.com/bbs/prod-api/thread/thread/getThreadForTid?tid={tid}"
    params = {'t': int(time.time() * 1000)}
    
    try:
        response = make_request_with_retry('get', url, headers, params, timeout=10) # 使用重试函数
        data = response.json()
        
        if data.get('errNo') == 0:
            result = data.get('result', {})
            
            # 提取所需详细信息
            detail = {
                'content': result.get('content', ''),
                'tags': result.get('tags', ''),
                'message': result.get('message', ''),
                'attachlist': result.get('attachlist', []),
                'pics': result.get('picsArray', []),
                'group_name': result.get('group_name', ''),
                'pic': result.get('pic', '')
            }
            return detail
        else:
            print(f"获取帖子详情失败 (tid={tid}): {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"请求帖子详情出错 (tid={tid}): {str(e)}")
        return {}

def process_category(category):
    """处理单个分类及其子分类"""
    category_id = category['id']
    category_name = category['category_name']
    
    print(f"\n>>>> 开始处理分类: {category_name} (ID:{category_id}) <<<<") # 增加提示
    subcategories = get_subcategories(category_id)
    
    results = []
    for sub_idx, sub in enumerate(subcategories): # 增加子分类索引
        group_id = sub['group_id']
        group_name = sub['group_name']
        page = 1
        total_threads_in_subcategory = []
        
        print(f"  └─ ({sub_idx + 1}/{len(subcategories)}) 正在获取子分类: {group_name} (ID:{group_id})") # 增加子分类提示
        
        # 分页获取所有帖子
        while True:
            threads, total_pages = get_threads(group_id, page)
            if not threads and page == 1: # 如果第一页就没数据，可能是问题，但如果不是第一页，说明爬完了
                print(f"      子分类 '{group_name}' (ID:{group_id}) 在第 {page} 页没有更多帖子或获取失败。") # 增加没有更多帖子提示
                break
            elif not threads and page > 1:
                print(f"      子分类 '{group_name}' (ID:{group_id}) 在第 {page} 页没有更多帖子。") # 增加没有更多帖子提示
                break
                
            # 为每个帖子获取详细信息
            detailed_threads = []
            for thread_idx, thread in enumerate(threads): # 增加帖子索引
                tid = thread.get('tid')
                thread_title = thread.get('title', '无标题帖子') # 获取帖子标题用于提示
                if tid:
                    print(f"        -> (帖子 {thread_idx + 1}/{len(threads)}) 正在获取帖子详情: '{thread_title}' (TID:{tid})") # 增加帖子详情提示
                    detail = get_thread_detail(tid)
                    # 合并基本信息和详细信息
                    full_thread = {**thread, **detail}
                    detailed_threads.append(full_thread)
                else:
                    print(f"        -> (帖子 {thread_idx + 1}/{len(threads)}) 警告: 帖子缺少 TID，跳过详情获取。标题: '{thread_title}'") # 增加缺少TID提示
                    detailed_threads.append(thread)
                time.sleep(random.uniform(10, 30)) # 帖子详情请求之间的小随机延迟
            
            total_threads_in_subcategory.extend(detailed_threads)
            print(f"      子分类 '{group_name}' 已获取第 {page}/{total_pages} 页，共 {len(threads)} 条帖子详情。") # 更新页面获取提示
            
            # 检查是否还有下一页
            if page >= total_pages:
                print(f"      子分类 '{group_name}' (ID:{group_id}) 所有 {total_pages} 页帖子已获取完毕。") # 增加所有页面获取完毕提示
                break
                
            page += 1
            # 在页面/请求组之间引入更长的随机延迟 (30-60 秒)
            sleep_time = random.uniform(30, 60)
            print(f"      暂停 {sleep_time:.2f} 秒，然后继续到下一页/子分类...")
            time.sleep(sleep_time) 
            
        # 保存当前子分类的结果
        results.append({
            'category_id': category_id,
            'category_name': category_name,
            'subcategory_id': group_id,
            'subcategory_name': group_name,
            'threads': total_threads_in_subcategory
        })
    
    return results

def main():
    # 获取所有分类
    categories = get_categories()
    print(f"共获取到 {len(categories)} 个一级分类")
    
    all_results = []
    total_subcategories_count = 0
    total_threads_count = 0

    # 循环处理每个分类（单线程）
    for cat_idx, cat in enumerate(categories): # 增加分类索引
        try:
            print(f"\n===== 正在处理第 {cat_idx + 1}/{len(categories)} 个一级分类 =====") # 增加主循环分类提示
            category_results = process_category(cat) 
            all_results.extend(category_results)
            
            # 更新最终摘要的计数
            total_subcategories_count += len(category_results)
            for res in category_results:
                total_threads_count += len(res['threads'])
            
            # 在处理完一个分类后，增加 10-60 秒的随机延迟
            if cat_idx < len(categories) - 1: # 避免在最后一个分类处理完后也等待
                sleep_time_main_loop = random.uniform(10, 60)
                print(f"\n<<<< 处理完分类 '{cat.get('category_name', 'N/A')}'。暂停 {sleep_time_main_loop:.2f} 秒，然后继续处理下一个分类... >>>>") # 增加分类处理完成提示
                time.sleep(sleep_time_main_loop)

        except Exception as e:
            print(f"处理分类 '{cat.get('category_name', 'N/A')}' 出错: {str(e)}")
                
    # 将结果保存到 JSON 文件
    output_filename = 'zhulong_full_data.json'
    print(f"\n\n正在将所有爬取到的数据保存到文件: {output_filename}...") # 增加保存文件提示
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    
    print("\n--- 爬取摘要 ---")
    print(f"数据爬取完成! 结果已保存到 {output_filename}")
    print(f"处理了 {len(categories)} 个一级分类。")
    print(f"找到了 {total_subcategories_count} 个子分类。")
    print(f"总共收集了 {total_threads_count} 条帖子。")
    print("------------------------")

if __name__ == "__main__":
    main()

import requests
import time
import json
import random
import os
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('zhulong_crawler.log')
    ]
)

# 测试模式设置
TEST_MODE = True  # 启用测试模式
MAX_TEST_PAGES = 10  # 测试时最大页面数
MAX_TEST_SUBCATEGORIES = 2  # 测试时最大子分类数

# 设置请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://www.zhulong.com/',
    'Accept': 'application/json, text/plain, */*',
}

# 动态Cookie管理
def get_dynamic_cookie():
    """生成或更新Cookie - 测试版本"""
    # 这里使用一个简单的Cookie，实际项目中应该动态获取
    return 'Hm_lvt_b4c6201caa3cb2837f622d668e688cfd=1751961635; HMACCOUNT=FB83163D0F4ABA49;'

# 重试机制的配置
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 3
REQUEST_TIMEOUT = 15

def make_request_with_retry(method, url, params=None, timeout=REQUEST_TIMEOUT, retries=0):
    """
    带重试机制的 HTTP 请求函数
    """
    # 动态设置Cookie
    headers['Cookie'] = get_dynamic_cookie()
    
    try:
        # 添加随机延迟避免请求过于频繁
        time.sleep(random.uniform(0.5, 1.5))
        
        response = requests.request(
            method, 
            url, 
            headers=headers, 
            params=params, 
            timeout=timeout
        )
        response.raise_for_status()
        
        # 检查API错误码
        try:
            data = response.json()
            if data.get('errNo') != 0:
                logging.warning(f"API返回错误: {data.get('msg')} (errNo: {data.get('errNo')})")
                raise requests.exceptions.RequestException(f"API Error: {data.get('msg')}")
            return response
        except json.JSONDecodeError:
            # 如果响应不是JSON，直接返回
            return response
            
    except requests.exceptions.RequestException as e:
        if retries < MAX_RETRIES:
            delay = INITIAL_RETRY_DELAY * (2 ** retries) * random.uniform(0.8, 1.2)
            logging.warning(f"请求失败: {url} - {e}。第 {retries + 1}/{MAX_RETRIES} 次重试，等待 {delay:.2f} 秒...")
            time.sleep(delay)
            return make_request_with_retry(method, url, params, timeout, retries + 1)
        else:
            logging.error(f"请求失败: {url} - {e}。已达到最大重试次数 ({MAX_RETRIES})")
            return None

def get_categories():
    """获取所有一级分类"""
    url = "https://www.zhulong.com/bbs/prod-api/home/resource/category"
    params = {'t': int(time.time() * 1000)}
    
    try:
        response = make_request_with_retry('get', url, params)
        if response is None:
            logging.error("获取分类请求失败")
            return []
            
        data = response.json()
        
        if data.get('errNo') == 0:
            categories = data['result']
            logging.info(f"获取到 {len(categories)} 个一级分类")
            
            # 测试模式下只返回前2个分类
            if TEST_MODE:
                return categories[:2]
            return categories
        else:
            logging.error(f"获取分类失败: {data.get('msg')}")
            return []
    except Exception as e:
        logging.error(f"请求分类出错: {str(e)}")
        return []

def get_subcategories(category_id, category_name):
    """获取指定分类的子分类"""
    url = "https://www.zhulong.com/bbs/prod-api/home/resource/group"
    params = {
        'category_id': category_id,
        't': int(time.time() * 1000)
    }
    
    try:
        response = make_request_with_retry('get', url, params)
        if response is None:
            logging.error("获取子分类请求失败")
            return []
            
        data = response.json()
        
        if data.get('errNo') == 0:
            subcategories = data['result']
            logging.info(f"[分类: {category_name} (ID:{category_id})] 获取到 {len(subcategories)} 个子分类")
            
            # 测试模式下只返回前2个子分类
            if TEST_MODE:
                return subcategories[:MAX_TEST_SUBCATEGORIES]
            return subcategories
        else:
            logging.error(f"[分类: {category_name} (ID:{category_id})] 获取子分类失败: {data.get('msg')}")
            return []
    except Exception as e:
        logging.error(f"[分类: {category_name} (ID:{category_id})] 请求子分类出错: {str(e)}")
        return []

def get_threads(group_id, group_name, category_name, page=1, limit=10):
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
        response = make_request_with_retry('get', url, params)
        if response is None:
            logging.error(f"[分类: {category_name}][子分类: {group_name} (ID:{group_id})] 获取帖子列表请求失败")
            return [], 1
            
        data = response.json()
        
        if data.get('errNo') == 0:
            # 提取帖子列表
            thread_list = data.get('result', {}).get('thread', {}).get('list', [])
            # 提取总页数用于翻页
            total_pages = data.get('result', {}).get('thread', {}).get('maxPage', 1)
            
            # 测试模式下限制最大页数
            if TEST_MODE and total_pages > MAX_TEST_PAGES:
                total_pages = MAX_TEST_PAGES
                
            logging.info(f"[分类: {category_name}][子分类: {group_name} (ID:{group_id})] 第 {page} 页获取到 {len(thread_list)} 条帖子 (共 {total_pages} 页)")
            return thread_list, total_pages
        else:
            logging.error(f"[分类: {category_name}][子分类: {group_name} (ID:{group_id})] 第 {page} 页获取帖子失败: {data.get('msg')}")
            return [], 1
    except Exception as e:
        logging.error(f"[分类: {category_name}][子分类: {group_name} (ID:{group_id})] 第 {page} 页请求帖子出错: {str(e)}")
        return [], 1

def get_thread_detail(tid, thread_title, group_name, category_name):
    """获取帖子详细信息 - 测试版本（仅获取基本信息）"""
    # 在测试模式下，我们只获取部分关键信息以节省时间
    logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title} (TID:{tid})] 获取帖子详情")
    
    # 简化版的详情获取
    return {
        'content': f"测试内容 - TID:{tid}",
        'tags': "测试标签",
        'message': "测试消息",
        'attachlist': [],
        'pics': [],
        'group_name': "测试分组",
        'pic': ""
    }

def process_category(category):
    """处理单个分类及其子分类"""
    category_id = category['id']
    category_name = category['category_name']
    
    logging.info(f"开始处理分类: {category_name} (ID:{category_id})")
    subcategories = get_subcategories(category_id, category_name)
    
    if not subcategories:
        logging.warning(f"分类 {category_name} (ID:{category_id}) 没有获取到子分类")
        return []
    
    results = []
    for sub_idx, sub in enumerate(subcategories):
        group_id = sub['group_id']
        group_name = sub['group_name']
        page = 1
        total_threads_in_subcategory = []
        
        logging.info(f"[分类: {category_name}] 处理子分类: {group_name} (ID:{group_id}) [{sub_idx+1}/{len(subcategories)}]")
        
        # 分页获取所有帖子
        while True:
            try:
                threads, total_pages = get_threads(group_id, group_name, category_name, page)
                if not threads:
                    if page == 1:
                        logging.warning(f"[分类: {category_name}][子分类: {group_name} (ID:{group_id})] 第一页无数据")
                    break
                
                # 为每个帖子获取详细信息
                detailed_threads = []
                for thread in threads:
                    tid = thread.get('tid')
                    thread_title = thread.get('title', '无标题帖子')
                    if tid:
                        detail = get_thread_detail(tid, thread_title, group_name, category_name)
                        full_thread = {**thread, **detail}
                        detailed_threads.append(full_thread)
                    else:
                        logging.warning(f"[分类: {category_name}][子分类: {group_name}] 帖子缺少TID: {thread_title}")
                        detailed_threads.append(thread)
                
                total_threads_in_subcategory.extend(detailed_threads)
                logging.info(f"[分类: {category_name}][子分类: {group_name}] 第 {page}/{total_pages} 页完成, 累计帖子数: {len(total_threads_in_subcategory)}")
                
                # 检查是否还有下一页
                if page >= total_pages:
                    logging.info(f"[分类: {category_name}][子分类: {group_name}] 所有页面获取完成")
                    break
                    
                page += 1
                
                # 页面间延迟
                if page <= total_pages:
                    delay = random.uniform(3, 8)  # 测试模式下缩短延迟
                    logging.info(f"[分类: {category_name}][子分类: {group_name}] 等待 {delay:.1f}秒后获取下一页...")
                    time.sleep(delay)
                    
            except Exception as e:
                logging.error(f"[分类: {category_name}][子分类: {group_name}] 处理出错: {str(e)}")
                break
        
        # 保存当前子分类的结果
        results.append({
            'category_id': category_id,
            'category_name': category_name,
            'subcategory_id': group_id,
            'subcategory_name': group_name,
            'threads': total_threads_in_subcategory
        })
        
        # 子分类间延迟
        if sub_idx < len(subcategories) - 1:
            delay = random.uniform(5, 15)  # 测试模式下缩短延迟
            logging.info(f"[分类: {category_name}] 等待 {delay:.1f}秒后处理下一个子分类...")
            time.sleep(delay)
    
    logging.info(f"[分类: {category_name}] 处理完成")
    return results

def save_results_incrementally(results, filename):
    """增量保存结果到文件"""
    try:
        if not results:
            logging.info("没有结果需要保存")
            return
            
        mode = 'a' if os.path.exists(filename) else 'w'
        with open(filename, mode, encoding='utf-8') as f:
            if mode == 'w':
                f.write('[')
            else:
                # 移动到文件末尾并覆盖最后的 ']'
                f.seek(0, os.SEEK_END)
                file_size = f.tell()
                if file_size > 1:
                    f.seek(file_size - 1)
                    f.write(',')
            
            json.dump(results, f, ensure_ascii=False, indent=2)
            f.write(']')
            
        logging.info(f"数据已保存到 {filename}")
    except Exception as e:
        logging.error(f"保存数据出错: {str(e)}")

def main():
    output_filename = 'zhulong_test_data.json' if TEST_MODE else 'zhulong_full_data.json'
    
    # 初始化输出文件
    if os.path.exists(output_filename):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.rename(output_filename, f"zhulong_data_{timestamp}.json")
    
    # 创建空JSON数组
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write('[]')
    
    logging.info("=" * 50)
    logging.info(f"开始爬取 - {'测试模式' if TEST_MODE else '正式模式'}")
    if TEST_MODE:
        logging.info(f"测试配置: 最大页数={MAX_TEST_PAGES}, 最大子分类数={MAX_TEST_SUBCATEGORIES}")
    logging.info("=" * 50)
    
    # 获取所有分类
    categories = get_categories()
    logging.info(f"共获取到 {len(categories)} 个一级分类")
    
    total_subcategories_count = 0
    total_threads_count = 0

    # 循环处理每个分类
    for cat_idx, cat in enumerate(categories):
        category_name = cat['category_name']
        try:
            logging.info(f"处理分类 [{cat_idx+1}/{len(categories)}]: {category_name}")
            category_results = process_category(cat)
            
            # 保存当前分类结果
            if category_results:
                save_results_incrementally(category_results, output_filename)
                
                # 更新计数
                total_subcategories_count += len(category_results)
                for res in category_results:
                    total_threads_count += len(res['threads'])
            
            # 分类间延迟
            if cat_idx < len(categories) - 1:
                delay = random.uniform(10, 20)  # 测试模式下缩短延迟
                logging.info(f"等待 {delay:.1f}秒后处理下一个分类...")
                time.sleep(delay)
                
        except Exception as e:
            logging.error(f"处理分类 {category_name} 出错: {str(e)}")
            # 出错后等待
            delay = random.uniform(30, 60)
            logging.info(f"出错后等待 {delay:.1f}秒...")
            time.sleep(delay)
    
    # 最终摘要
    logging.info("\n" + "=" * 50)
    logging.info("爬取摘要:")
    logging.info(f"处理了 {len(categories)} 个一级分类")
    logging.info(f"找到了 {total_subcategories_count} 个子分类")
    logging.info(f"总共收集了 {total_threads_count} 条帖子")
    logging.info(f"结果已保存到: {output_filename}")
    logging.info("=" * 50)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
    except Exception as e:
        logging.error(f"程序发生未处理异常: {str(e)}")

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

# 设置请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://www.zhulong.com/',
    'Accept': 'application/json, text/plain, */*',
}

# 动态Cookie管理
def get_dynamic_cookie():
    """生成或更新Cookie"""
    # 这里应该包含获取有效Cookie的逻辑
    # 实际项目中需要从文件或环境变量中读取/更新Cookie
    return 'Hm_lvt_b4c6201caa3cb2837f622d668e688cfd=1751961635,1753093327,1753604656,1753919554; HMACCOUNT=FB83163D0F4ABA49; ...'

# 重试机制的配置
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 3
REQUEST_TIMEOUT = 15

def make_request_with_retry(method, url, params=None, timeout=REQUEST_TIMEOUT, retries=0):
    """
    带重试机制的 HTTP 请求函数
    """
    # 动态设置Cookie
    headers['Cookie'] = get_dynamic_cookie()
    
    try:
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
        except json.JSONDecodeError:
            pass
            
        return response
    except requests.exceptions.RequestException as e:
        if retries < MAX_RETRIES:
            delay = INITIAL_RETRY_DELAY * (2 ** retries) * random.uniform(0.8, 1.2)
            logging.warning(f"请求失败: {url} - {e}。第 {retries + 1}/{MAX_RETRIES} 次重试，等待 {delay:.2f} 秒...")
            time.sleep(delay)
            return make_request_with_retry(method, url, params, timeout, retries + 1)
        else:
            logging.error(f"请求失败: {url} - {e}。已达到最大重试次数 ({MAX_RETRIES})")
            raise

def get_categories():
    """获取所有一级分类"""
    url = "https://www.zhulong.com/bbs/prod-api/home/resource/category"
    params = {'t': int(time.time() * 1000)}
    
    try:
        response = make_request_with_retry('get', url, params)
        data = response.json()
        logging.info(f"获取到 {len(data.get('result', []))} 个一级分类")
        if data.get('errNo') == 0:
            return data['result']
        else:
            logging.error(f"获取分类失败: {data.get('msg')}")
            return []
    except Exception as e:
        logging.error(f"请求分类出错: {str(e)}")
        return []

def get_subcategories(category_id):
    """获取指定分类的子分类"""
    url = "https://www.zhulong.com/bbs/prod-api/home/resource/group"
    params = {
        'category_id': category_id,
        't': int(time.time() * 1000)
    }
    
    try:
        response = make_request_with_retry('get', url, params)
        data = response.json()
        
        if data.get('errNo') == 0:
            logging.info(f"分类 {category_id} 获取到 {len(data['result'])} 个子分类")
            return data['result']
        else:
            logging.error(f"获取子分类失败 (category_id={category_id}): {data.get('msg')}")
            return []
    except Exception as e:
        logging.error(f"请求子分类出错: {str(e)}")
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
        response = make_request_with_retry('get', url, params)
        data = response.json()
        
        if data.get('errNo') == 0:
            # 提取帖子列表
            thread_list = data.get('result', {}).get('thread', {}).get('list', [])
            # 提取总页数用于翻页
            total_pages = data.get('result', {}).get('thread', {}).get('maxPage', 1)
            logging.info(f"子分类 {group_id} 第 {page} 页获取到 {len(thread_list)} 条帖子")
            return thread_list, total_pages
        else:
            logging.error(f"获取帖子失败 (group_id={group_id}, page={page}): {data.get('msg')}")
            return [], 1
    except Exception as e:
        logging.error(f"请求帖子出错: {str(e)}")
        return [], 1

def get_thread_detail(tid):
    """获取帖子详细信息"""
    url = f"https://www.zhulong.com/bbs/prod-api/thread/thread/getThreadForTid?tid={tid}"
    params = {'t': int(time.time() * 1000)}
    
    try:
        # 帖子详情请求前添加随机延迟
        time.sleep(random.uniform(0.5, 1.5))
        
        response = make_request_with_retry('get', url, params)
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
            logging.warning(f"获取帖子详情失败 (tid={tid}): {data.get('msg')}")
            return {}
    except Exception as e:
        logging.error(f"请求帖子详情出错 (tid={tid}): {str(e)}")
        return {}

def process_category(category):
    """处理单个分类及其子分类"""
    category_id = category['id']
    category_name = category['category_name']
    
    logging.info(f"开始处理分类: {category_name} (ID:{category_id})")
    subcategories = get_subcategories(category_id)
    
    results = []
    for sub_idx, sub in enumerate(subcategories):
        group_id = sub['group_id']
        group_name = sub['group_name']
        page = 1
        total_threads_in_subcategory = []
        
        logging.info(f"处理子分类: {group_name} (ID:{group_id}) [{sub_idx+1}/{len(subcategories)}]")
        
        # 分页获取所有帖子
        while True:
            try:
                threads, total_pages = get_threads(group_id, page)
                if not threads:
                    if page == 1:
                        logging.warning(f"子分类 {group_name} 第一页无数据")
                    break
                
                # 为每个帖子获取详细信息
                detailed_threads = []
                for thread in threads:
                    tid = thread.get('tid')
                    thread_title = thread.get('subject', '无标题帖子')
                    if tid:
                        detail = get_thread_detail(tid)
                        full_thread = {**thread, **detail}
                        detailed_threads.append(full_thread)
                    else:
                        logging.warning(f"帖子缺少TID: {thread_title}")
                        detailed_threads.append(thread)
                
                total_threads_in_subcategory.extend(detailed_threads)
                logging.info(f"子分类 {group_name} 第 {page}/{total_pages} 页完成")
                
                # 检查是否还有下一页
                if page >= total_pages:
                    logging.info(f"子分类 {group_name} 所有页面获取完成")
                    break
                    
                page += 1
                
                # 页面间延迟
                if page <= total_pages:
                    delay = random.uniform(10, 30)
                    logging.info(f"等待 {delay:.1f}秒后获取下一页...")
                    time.sleep(delay)
                    
            except Exception as e:
                logging.error(f"处理子分类 {group_name} 出错: {str(e)}")
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
            delay = random.uniform(15, 45)
            logging.info(f"等待 {delay:.1f}秒后处理下一个子分类...")
            time.sleep(delay)
    
    return results

def save_results_incrementally(results, filename):
    """增量保存结果到文件"""
    try:
        mode = 'a' if os.path.exists(filename) else 'w'
        with open(filename, mode, encoding='utf-8') as f:
            if mode == 'w':
                f.write('[')
            else:
                f.seek(0, os.SEEK_END)
                f.seek(f.tell() - 1, os.SEEK_SET)  # 移到最后一个字符前
                f.write(',')
            
            json.dump(results, f, ensure_ascii=False, indent=2)
            f.write(']')
            
        logging.info(f"数据已保存到 {filename}")
    except Exception as e:
        logging.error(f"保存数据出错: {str(e)}")

def main():
    output_filename = 'zhulong_full_data.json'
    
    # 初始化输出文件
    if os.path.exists(output_filename):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.rename(output_filename, f"zhulong_data_{timestamp}.json")
    
    # 创建空JSON数组
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write('[]')
    
    # 获取所有分类
    categories = get_categories()
    logging.info(f"共获取到 {len(categories)} 个一级分类")
    
    total_subcategories_count = 0
    total_threads_count = 0

    # 循环处理每个分类
    for cat_idx, cat in enumerate(categories):
        try:
            logging.info(f"处理分类 [{cat_idx+1}/{len(categories)}]: {cat['category_name']}")
            category_results = process_category(cat)
            
            # 保存当前分类结果
            save_results_incrementally(category_results, output_filename)
            
            # 更新计数
            total_subcategories_count += len(category_results)
            for res in category_results:
                total_threads_count += len(res['threads'])
            
            # 分类间延迟
            if cat_idx < len(categories) - 1:
                delay = random.uniform(30, 90)
                logging.info(f"等待 {delay:.1f}秒后处理下一个分类...")
                time.sleep(delay)
                
        except Exception as e:
            logging.error(f"处理分类 {cat.get('category_name', 'N/A')} 出错: {str(e)}")
            # 出错后长时间等待
            delay = random.uniform(120, 300)
            logging.info(f"出错后等待 {delay:.1f}秒...")
            time.sleep(delay)
    
    # 最终摘要
    logging.info("\n--- 爬取摘要 ---")
    logging.info(f"数据爬取完成! 结果已保存到 {output_filename}")
    logging.info(f"处理了 {len(categories)} 个一级分类")
    logging.info(f"找到了 {total_subcategories_count} 个子分类")
    logging.info(f"总共收集了 {total_threads_count} 条帖子")
    logging.info("------------------------")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
    except Exception as e:
        logging.error(f"程序发生未处理异常: {str(e)}")

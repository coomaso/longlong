import requests
import time
import json
import random
import os
import re
from datetime import datetime
import logging
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.parse

# 全局配置
TEST_MODE = True  # 启用测试模式
MAX_TEST_PAGES = 10  # 测试时最大页面数
MAX_TEST_SUBCATEGORIES = 2  # 测试时最大子分类数
DOWNLOAD_ATTACHMENTS = True  # 是否下载附件
MAX_CONCURRENT_DOWNLOADS = 3  # 每个帖子同时下载的最大附件数
MAX_RETRIES = 3  # 最大重试次数
INITIAL_RETRY_DELAY = 3  # 初始重试延迟（秒）
REQUEST_TIMEOUT = 15  # 请求超时时间（秒）

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
    """生成或更新Cookie - 测试版本"""
    # 这里使用一个简单的Cookie，实际项目中应该动态获取
    return 'Hm_lvt_b4c6201caa3cb2837f622d668e688cfd=1751961635; HMACCOUNT=FB83163D0F4ABA49;'

def sanitize_filename(name):
    """清理文件名中的非法字符"""
    # 移除Windows文件名中不允许的字符
    return re.sub(r'[\\/*?:"<>|]', "", name)

def get_real_download_url(down_direct_url):
    """获取真实的下载链接"""
    try:
        # 添加随机延迟
        time.sleep(random.uniform(0.5, 1.5))
        
        # 设置动态Cookie
        headers['Cookie'] = get_dynamic_cookie()
        
        response = requests.get(
            down_direct_url,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        data = response.json()
        if data.get('errNo') == 0:
            real_url = data['result']['url']
            # 处理URL中的转义字符
            real_url = real_url.replace('\\/', '/')
            return real_url
        else:
            logging.warning(f"获取真实下载链接失败: {data.get('msg')}")
            return None
    except Exception as e:
        logging.error(f"获取真实下载链接出错: {str(e)}")
        return None

def download_file(url, file_path, max_retries=MAX_RETRIES):
    """
    下载文件并保存到指定路径
    :param url: 文件URL
    :param file_path: 本地保存路径
    :param max_retries: 最大重试次数
    :return: 成功返回True，失败返回False
    """
    # 确保目录存在
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            # 设置动态Cookie
            headers['Cookie'] = get_dynamic_cookie()
            
            # 添加随机延迟
            time.sleep(random.uniform(0.5, 1.5))
            
            response = requests.get(
                url, 
                headers=headers, 
                stream=True,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            # 获取文件大小
            file_size = int(response.headers.get('Content-Length', 0))
            
            # 下载文件
            with open(file_path, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # 过滤掉保持连接的新块
                        f.write(chunk)
                        downloaded += len(chunk)
            
            # 验证文件大小
            actual_size = os.path.getsize(file_path)
            if file_size > 0 and actual_size != file_size:
                logging.warning(f"文件大小不匹配: 预期 {file_size} 字节, 实际 {actual_size} 字节")
                os.remove(file_path)  # 删除不完整的文件
                continue
            
            return True
            
        except Exception as e:
            logging.error(f"下载文件失败 (尝试 {attempt+1}/{max_retries}): {str(e)}")
            time.sleep(2 ** attempt)  # 指数退避
    
    return False

def download_worker(task_queue, category_name, group_name, thread_title, tid):
    """工作线程函数，处理单个附件下载"""
    while not task_queue.empty():
        try:
            attach = task_queue.get_nowait()
            down_direct_url = attach.get('url')
            attach_name = attach.get('name', f'unnamed_attachment_{tid}_{time.time()}')
            
            if not down_direct_url:
                logging.warning(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] 附件缺少URL")
                task_queue.task_done()
                continue
            
            # 第一步：获取真实的下载链接
            logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                         f"获取真实下载链接: {down_direct_url}")
            
            real_url = get_real_download_url(down_direct_url)
            if not real_url:
                logging.warning(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                                f"获取真实下载链接失败")
                task_queue.task_done()
                continue
            
            # 清理文件名
            clean_category = sanitize_filename(category_name)
            clean_group = sanitize_filename(group_name)
            clean_title = sanitize_filename(thread_title)
            clean_name = sanitize_filename(attach_name)
            
            # 构建保存路径
            save_path = os.path.join(
                "downloads",
                clean_category,
                clean_group,
                clean_title,
                clean_name
            )
            
            # 第二步：下载文件
            logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                         f"开始下载附件: {attach_name} ({os.path.basename(real_url)})")
            
            success = download_file(real_url, save_path)
            
            result = {
                'name': attach_name,
                'down_direct_url': down_direct_url,
                'real_url': real_url,
                'local_path': save_path if success else None,
                'success': success
            }
            
            task_queue.task_done()
            return result
        except queue.Empty:
            break
        except Exception as e:
            logging.error(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] 下载工作线程出错: {str(e)}")
            task_queue.task_done()
            return None
    
    return None

def download_attachments(attachlist, category_name, group_name, thread_title, tid):
    """并发下载帖子的所有附件"""
    if not DOWNLOAD_ATTACHMENTS or not attachlist:
        return []
    
    # 创建任务队列
    task_queue = queue.Queue()
    for attach in attachlist:
        task_queue.put(attach)
    
    total_attachments = len(attachlist)
    downloaded_files = []
    
    # 使用线程池下载附件
    with ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_DOWNLOADS, total_attachments)) as executor:
        futures = []
        for _ in range(min(MAX_CONCURRENT_DOWNLOADS, total_attachments)):
            future = executor.submit(
                download_worker, 
                task_queue, 
                category_name, 
                group_name, 
                thread_title, 
                tid
            )
            futures.append(future)
        
        # 等待所有任务完成
        task_queue.join()
        
        # 收集结果
        for future in as_completed(futures):
            result = future.result()
            if result:
                downloaded_files.append(result)
    
    # 统计下载结果
    success_count = sum(1 for f in downloaded_files if f['success'])
    if success_count > 0:
        logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                     f"附件下载完成: {success_count}/{total_attachments} 成功")
    else:
        logging.warning(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                        f"附件下载失败: 0/{total_attachments} 成功")
    
    return downloaded_files

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
    """获取帖子详细信息"""
    url = f"https://www.zhulong.com/bbs/prod-api/thread/thread/getThreadForTid?tid={tid}"
    params = {'t': int(time.time() * 1000)}
    
    try:
        # 添加随机延迟
        time.sleep(random.uniform(0.5, 1.5))
        
        response = make_request_with_retry('get', url, params)
        if response is None:
            logging.error(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title} (TID:{tid})] 获取详情请求失败")
            return {}
            
        data = response.json()
        
        if data.get('errNo') == 0:
            result = data.get('result', {})
            
            # 提取所需详细信息
            detail = {
                'content': result.get('content', ''),
                'tags': result.get('tags', ''),
                'message': result.get('message', ''),
                'attachlist': result.get('attachlist', []),
                'hot': result.get('sum_hot', []), # 热度
                'star': result.get('star', []), # 星级
                'pics': result.get('picsArray', []),
                'group_name': result.get('group_name', ''),
                'pic': result.get('pic', '')
            }
            
            logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title} (TID:{tid})] 获取到 {len(detail['attachlist'])} 个附件")
            return detail
        else:
            logging.warning(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title} (TID:{tid})] 获取详情失败: {data.get('msg')}")
            return {}
    except Exception as e:
        logging.error(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title} (TID:{tid})] 请求详情出错: {str(e)}")
        return {}

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
                    
                    if not tid:
                        logging.warning(f"[分类: {category_name}][子分类: {group_name}] 帖子缺少TID: {thread_title}")
                        detailed_threads.append(thread)
                        continue
                    
                    # 获取帖子详情
                    detail = get_thread_detail(tid, thread_title, group_name, category_name)
                    full_thread = {**thread, **detail}
                    
                    # 下载附件（如果有）
                    if 'attachlist' in detail and detail['attachlist']:
                        # 记录开始时间
                        start_time = time.time()
                        downloaded = download_attachments(
                            detail['attachlist'], 
                            category_name, 
                            group_name, 
                            thread_title, 
                            tid
                        )
                        full_thread['downloaded_attachments'] = downloaded
                        
                        # 计算并记录下载耗时
                        download_time = time.time() - start_time
                        logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                                     f"附件下载耗时: {download_time:.2f}秒")
                    
                    detailed_threads.append(full_thread)
                
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
    
    # 确保下载目录存在
    os.makedirs('downloads', exist_ok=True)
    
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
    logging.info(f"附件下载: {'启用' if DOWNLOAD_ATTACHMENTS else '禁用'}")
    if DOWNLOAD_ATTACHMENTS:
        logging.info(f"并发下载: 每个帖子最多同时下载 {MAX_CONCURRENT_DOWNLOADS} 个附件")
    logging.info("=" * 50)
    
    # 获取所有分类
    categories = get_categories()
    logging.info(f"共获取到 {len(categories)} 个一级分类")
    
    total_subcategories_count = 0
    total_threads_count = 0
    total_attachments_count = 0
    total_downloaded_attachments = 0

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
                    
                    # 计算附件数量
                    for thread in res['threads']:
                        attachments = thread.get('downloaded_attachments', [])
                        total_attachments_count += len(attachments)
                        total_downloaded_attachments += sum(1 for a in attachments if a.get('success', False))
            
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
    if DOWNLOAD_ATTACHMENTS:
        logging.info(f"发现 {total_attachments_count} 个附件")
        logging.info(f"成功下载 {total_downloaded_attachments} 个附件")
    logging.info(f"结果已保存到: {output_filename}")
    logging.info(f"附件保存到: downloads/ 目录")
    logging.info("=" * 50)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
    except Exception as e:
        logging.error(f"程序发生未处理异常: {str(e)}")

import requests
import time
import json
import random
import os
import re
import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import unquote, quote, urlparse
from datetime import datetime

# Global Configuration
TEST_MODE = True
MAX_TEST_PAGES = 10
MAX_TEST_SUBCATEGORIES = 2
DOWNLOAD_ATTACHMENTS =  # Flase True
MAX_CONCURRENT_DOWNLOADS = 1
MAX_RETRIES = 1
INITIAL_RETRY_DELAY = 3
REQUEST_TIMEOUT = 15

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('zhulong_crawler.log', encoding='utf-8')
    ]
)

# Set request headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Referer': 'https://www.zhulong.com/',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Sec-Ch-Ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
}

# 使用会话保持连接
session = requests.Session()
session.headers.update(headers)

# Dynamic Cookie Management
def get_dynamic_cookie():
    return 'Hm_lvt_b4c6201caa3cb2837f622d668e688cfd=1751961635,1753093327,1753604656,1753919554; HMACCOUNT=FB83163D0F4ABA49; Hm_lpvt_b4c6201caa3cb2837f622d668e688cfd=1753995544; agency_id=2; PHPSESSID=kn642ohvac1s51eikt07d9pq13; agency_id=2; app_id=1; uuid=110758d4-723c-46ae-b6e9-3c6ec7cd1c2d; uid=15789975; username=%E6%BD%87%E6%B4%92%E9%94%8B; category_id=37; access_token=5423fbc605314aefbc3ee33ae9f68fd625f0bd88; ZLID=e8b5rpjaghEmoW2ylvfXapBetkxKx5LWLY0_Oem_9DIzXg50W5bFk17QEC91uTWA; province_id=13; city_id=195; reg_time=2022-02-15+16%3A55%3A02; ip_province_id=13; ip_city_id=195; zluuid=768F828E-8A1D-4AB3-99AC-FA941D8FCA29; bbs-uid=15789975; bbs-username=%E6%BD%87%E6%B4%92%E9%94%8B; only_uuid=19f3a264-dc31-2feb-02f7-3dc1a6e68477; Hm_lvt_49541358a94eea717001819b500f76c8=1749719683; Hm_lvt_918b0a71c46422e304aa32471c74fd98=1749719683; Hm_lvt_09566c1e6ae94ce8c4f40a54aed90f86=1749719704; Hm_lvt_d165b0df9d8b576128f53e461359a530=1749719704; fd=https%3A//www.baidu.com/link%3Furl%3DuQ4PZ636ul8QRiIsegAcrognuEADven6VHTYhbAj6BuMBSa2GfajoXzOzRycIYC9ZNozJxKYHqwyTe16XLfTTa%26wd%3D%26eqid%3Dc25b1d6c009856cc00000006684a9ad2; latest_search=%E4%B8%80%E5%BB%BA%E5%BB%BA%E7%AD%91%E5%9B%BE%E6%96%87%E5%AD%A6%E4%B9%A0%E9%80%9A2025; pcid=5990896829; agency_id=2; historicalSearchKeyWords=[%22%E6%94%AF%E6%8A%A4%E7%AE%B1%22%2C%22%E8%8A%B1%E7%AF%AE%E5%A4%96%E6%9E%B6%20%E5%9B%9B%E4%BB%A3%E5%BB%BA%E7%AD%91%22%2C%22%E5%8C%BB%E9%99%A2%E6%8A%80%E6%9C%AF%E6%A0%87%22%2C%22%E6%B2%88%E9%98%B3%E4%B8%AD%E5%BE%B7%E5%9B%AD%E5%9F%BA%E7%A1%80%E5%8F%8A%E5%85%AC%E5%85%B1%E8%AE%BE%E6%96%BD%E5%BB%BA%E8%AE%BEPPP%E9%A1%B9%E7%9B%AE%22%2C%22%E9%9B%A8%E6%B1%A1%E5%88%86%E6%B5%81%22%2C%22%E4%B8%B4%E6%97%B6%E7%94%A8%E6%B0%B4%E6%96%B9%E6%A1%88%22]; f=pc_bbsgroup_elite_0_1; fl=pc_down_group_0_1%252Cpc_bbsgroup_elite_0_1; sid=1F67E644-13DF-4A11-A1B9-449846831A2E; b_visit=1753995544971'

def sanitize_filename(name):
    """清理文件名中的非法字符"""
    return re.sub(r'[\\/*?:"<>|]', "", name)

def download_file(url, file_path, referer_url=None, max_retries=MAX_RETRIES):
    """下载文件并保存到本地"""
    # 检查文件是否已存在
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        logging.info(f"文件已存在，跳过下载: {os.path.basename(file_path)}")
        return True
    
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # 创建增强的请求头
    download_headers = {
        **headers,
        'Referer': referer_url or 'https://www.zhulong.com/',
        'Origin': 'https://www.zhulong.com'
    }
    
    for attempt in range(max_retries):
        try:
            # 添加随机延迟避免频繁请求
            time.sleep(random.uniform(1.0, 2.5))
            
            response = make_request_with_retry(
                'get', 
                url, 
                headers=download_headers, 
                stream=True,
                timeout=30  # 增加下载超时时间
            )
            
            if response is None:
                logging.error(f"下载文件请求失败，已达最大重试次数")
                return False

            file_size = int(response.headers.get('Content-Length', 0))
            start_time = time.time()
            downloaded_size = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # 计算下载速度
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            speed = downloaded_size / elapsed / 1024  # KB/s
                            if downloaded_size % (1024 * 100) == 0:  # 每100KB更新一次
                                logging.debug(f"下载进度: {downloaded_size}/{file_size} bytes, 速度: {speed:.2f} KB/s")
            
            actual_size = os.path.getsize(file_path)
            if file_size > 0 and actual_size != file_size:
                logging.warning(f"文件大小不匹配: 预期 {file_size} 字节, 实际 {actual_size} 字节")
                os.remove(file_path)
                continue
            
            download_time = time.time() - start_time
            logging.info(f"文件下载成功: {os.path.basename(file_path)}, 大小: {actual_size} 字节, 耗时: {download_time:.2f}秒")
            return True
            
        except Exception as e:
            logging.error(f"下载文件失败 (尝试 {attempt+1}/{max_retries}): {str(e)}")
            time.sleep(2 ** attempt)
    
    return False

def get_attachment_download_info(tid):
    """获取附件下载信息"""
    url = "https://www.zhulong.com/bbs/prod-api/attachment/attachment/downLog"
    api_headers = {**headers, "Referer": f"https://www.zhulong.com/bbs/d/{tid}.html"}

    try:
        response = make_request_with_retry('get', url, params={"tid": tid}, headers=api_headers, timeout=REQUEST_TIMEOUT)
        if response is None:
            logging.error(f"获取下载信息请求失败，已达最大重试次数")
            return None

        data = response.json()
        if data.get("errNo") == 0 and data.get("result") and data["result"].get("attachments"):
            attachments = data["result"]["attachments"]
            file_info = []

            for item in attachments:
                filename = re.sub(r'[\\/:"*?<>|]+', "_", item.get("filename") or item.get("title"))
                download_url = item.get("url")

                if download_url:
                    # 正确解码URL并处理特殊字符
                    download_url = unquote(download_url)
                    
                    # 确保URL路径正确编码
                    parsed = urlparse(download_url)
                    safe_path = quote(parsed.path, safe='/:?=&')
                    safe_url = parsed._replace(path=safe_path).geturl()
                    
                    file_info.append({"filename": filename, "download_url": safe_url})

            return {
                "tid": tid,
                "title": data["result"].get("title", ""),
                "files": file_info
            }
        else:
            logging.warning(f"获取附件下载信息失败: API返回数据不完整或有误. 响应: {data}")
            return None
    except Exception as e:
        logging.error(f"获取附件下载信息出错: {str(e)}")
    return None

def download_worker(task_queue, category_name, group_name, thread_title, tid):
    """工作线程函数，处理附件下载任务"""
    downloaded_files = []
    
    while True:
        try:
            # 增加超时时间避免永久阻塞
            file_item = task_queue.get(timeout=10)
            attach_name = file_item['filename']
            download_url = file_item['download_url']
            
            if not download_url:
                logging.error(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] 附件 '{attach_name}' 缺少下载链接，已跳过。")
                downloaded_files.append({
                    'name': attach_name,
                    'success': False,
                    'error': 'Missing download URL'
                })
                task_queue.task_done()
                continue
            
            # 创建安全的文件名和路径
            clean_category = sanitize_filename(category_name)
            clean_group = sanitize_filename(group_name)
            clean_title = sanitize_filename(thread_title)
            clean_name = sanitize_filename(attach_name)
            
            save_path = os.path.join(
                "downloads",
                clean_category,
                clean_group,
                clean_title,
                clean_name
            )
            
            # 创建帖子详情页URL作为Referer
            referer_url = f"https://www.zhulong.com/bbs/d/{tid}.html"
            
            logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                         f"开始下载附件: {attach_name}")
            
            # 下载文件并传递Referer
            success = download_file(download_url, save_path, referer_url=referer_url)
            
            result = {
                'name': attach_name,
                'down_direct_url': download_url,
                'real_url': download_url,
                'local_path': save_path if success else None,
                'success': success
            }
            downloaded_files.append(result)
            
            logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                         f"附件 {attach_name} 下载 {'成功' if success else '失败'}")
            
            task_queue.task_done()
            
        except queue.Empty:
            # 队列为空时退出循环
            break
        except Exception as e:
            logging.error(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] 下载工作线程出错: {str(e)}")
            if 'file_item' in locals():
                downloaded_files.append({
                    'name': attach_name,
                    'success': False,
                    'error': str(e)
                })
                task_queue.task_done()
    
    return downloaded_files

def download_attachments(files_to_download, category_name, group_name, thread_title, tid):
    """下载帖子的所有附件"""
    if not DOWNLOAD_ATTACHMENTS or not files_to_download:
        return []
    
    task_queue = queue.Queue()
    for file_item in files_to_download:
        task_queue.put(file_item)
    
    total_attachments = len(files_to_download)
    downloaded_files = []
    
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
        
        task_queue.join()
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                downloaded_files.extend(result)
    
    success_count = sum(1 for f in downloaded_files if f.get('success', False))
    if success_count > 0:
        logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                     f"附件下载完成: {success_count}/{total_attachments} 成功")
    else:
        logging.warning(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                        f"附件下载失败: 0/{total_attachments} 成功")
    
    return downloaded_files

def make_request_with_retry(method, url, params=None, headers=None, timeout=REQUEST_TIMEOUT, retries=0, stream=False):
    """带重试机制的请求函数"""
    cookies = get_dynamic_cookie()
    
    # 使用全局session，但更新特定请求的headers
    request_headers = headers or globals()['headers']
    
    try:
        # 添加随机延迟避免请求过于频繁
        time.sleep(random.uniform(1.0, 2.5))
        
        response = session.request(
            method, 
            url, 
            headers=request_headers, 
            params=params,
            cookies={'Cookie': cookies},
            timeout=timeout,
            stream=stream
        )
        response.raise_for_status()
        
        if not stream:
            try:
                data = response.json()
                if isinstance(data, dict) and data.get('errNo') != 0:
                    logging.warning(f"API返回错误: {data.get('msg')} (errNo: {data.get('errNo')})")
                    raise requests.exceptions.RequestException(f"API Error: {data.get('msg')}")
                return response
            except json.JSONDecodeError:
                return response
        else:
            return response
            
    except requests.exceptions.RequestException as e:
        if retries < MAX_RETRIES:
            delay = INITIAL_RETRY_DELAY * (2 ** retries) * random.uniform(0.8, 1.2)
            logging.warning(f"请求失败: {url} - {e}。第 {retries + 1}/{MAX_RETRIES} 次重试，等待 {delay:.2f} 秒...")
            time.sleep(delay)
            return make_request_with_retry(method, url, params, headers, timeout, retries + 1, stream=stream)
        else:
            logging.error(f"请求失败: {url} - {e}。已达到最大重试次数 ({MAX_RETRIES})")
            return None

def get_categories():
    """获取一级分类"""
    url = "https://www.zhulong.com/bbs/prod-api/home/resource/category"
    params = {'t': int(time.time() * 1000)}
    
    try:
        logging.info("正在获取一级分类...")
        response = make_request_with_retry('get', url, params)
        if response is None:
            logging.error("获取分类请求失败")
            return []
        
        try:
            data = response.json()
        except json.JSONDecodeError:
            logging.error(f"API返回非JSON响应: {response.text[:200]}")
            return []
        
        if not isinstance(data, dict):
            logging.error(f"API返回的数据类型错误，期望字典，实际得到: {type(data)}")
            return []

        if data.get('errNo') == 0:
            categories = data.get('result', [])
            if not isinstance(categories, list):
                logging.error(f"API返回的分类数据结构有误，应为列表，但获取到的是 {type(categories)}")
                return []

            logging.info(f"获取到 {len(categories)} 个一级分类")
            
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
    """获取子分类"""
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
        
        try:
            data = response.json()
        except json.JSONDecodeError:
            logging.error(f"API返回非JSON响应: {response.text[:200]}")
            return []
            
        if not isinstance(data, dict):
            logging.error(f"API返回的数据类型错误，期望字典，实际得到: {type(data)}")
            return []
            
        if data.get('errNo') == 0:
            subcategories = data.get('result', [])
            if not isinstance(subcategories, list):
                logging.error(f"API返回的子分类数据结构有误，应为列表，但获取到的是 {type(subcategories)}")
                return []
            
            logging.info(f"[分类: {category_name} (ID:{category_id})] 获取到 {len(subcategories)} 个子分类")
            
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
    """获取帖子列表"""
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
        
        try:
            data = response.json()
        except json.JSONDecodeError:
            logging.error(f"API返回非JSON响应: {response.text[:200]}")
            return [], 1
            
        if not isinstance(data, dict):
            logging.error(f"API返回的数据类型错误，期望字典，实际得到: {type(data)}")
            return [], 1
            
        if data.get('errNo') == 0:
            thread_list = data.get('result', {}).get('thread', {}).get('list', [])
            total_pages = data.get('result', {}).get('thread', {}).get('maxPage', 1)
            
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
    """获取帖子详情"""
    url = f"https://www.zhulong.com/bbs/prod-api/thread/thread/getThreadForTid?tid={tid}"
    params = {'t': int(time.time() * 1000)}
    
    try:
        # 添加随机延迟
        time.sleep(random.uniform(1.0, 2.0))
        
        response = make_request_with_retry('get', url, params)
        if response is None:
            logging.error(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title} (TID:{tid})] 获取详情请求失败")
            return {}
        
        try:
            data = response.json()
        except json.JSONDecodeError:
            logging.error(f"API返回非JSON响应: {response.text[:200]}")
            return {}
            
        if not isinstance(data, dict):
            logging.error(f"API返回的数据类型错误，期望字典，实际得到: {type(data)}")
            return {}
            
        if data.get('errNo') == 0:
            result = data.get('result', {})
            
            detail = {
                'content': result.get('content', ''),
                'tags': result.get('tags', ''),
                'message': result.get('message', ''),
                'hot': result.get('sum_hot', []),
                'star': result.get('star', []),
                'pics': result.get('picsArray', []),
                'group_name': result.get('group_name', ''),
                'pic': result.get('pic', '')
            }
            
            # 获取附件信息
            attachment_info = get_attachment_download_info(tid)
            if attachment_info:
                detail['attachlist'] = attachment_info['files']
                logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title} (TID:{tid})] 获取到 {len(attachment_info['files'])} 个附件信息")
                for idx, attach in enumerate(attachment_info['files']):
                    logging.info(f"    - 附件 {idx+1}: {attach.get('filename')}")
            else:
                detail['attachlist'] = []
                logging.warning(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title} (TID:{tid})] 无法获取附件下载信息")
            
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
        
        while True:
            try:
                threads, total_pages = get_threads(group_id, group_name, category_name, page)
                if not threads:
                    if page == 1:
                        logging.warning(f"[分类: {category_name}][子分类: {group_name} (ID:{group_id})] 第一页无数据")
                    break
                    
                detailed_threads = []
                for thread in threads:
                    tid = thread.get('tid')
                    thread_title = thread.get('title', '无标题帖子')
                    
                    if not tid:
                        logging.warning(f"[分类: {category_name}][子分类: {group_name}] 帖子缺少TID: {thread_title}")
                        detailed_threads.append(thread)
                        continue
                        
                    detail = get_thread_detail(tid, thread_title, group_name, category_name)
                    full_thread = {**thread, **detail}
                    
                    if 'attachlist' in detail and detail['attachlist']:
                        start_time = time.time()
                        
                        # 直接使用从get_thread_detail获取的附件信息
                        downloaded = download_attachments(
                            detail['attachlist'], 
                            category_name, 
                            group_name, 
                            thread_title, 
                            tid
                        )
                        full_thread['downloaded_attachments'] = downloaded
                        
                        download_time = time.time() - start_time
                        logging.info(f"[分类: {category_name}][子分类: {group_name}][帖子: {thread_title}] "
                                     f"附件下载耗时: {download_time:.2f}秒")
                    
                    detailed_threads.append(full_thread)
                
                total_threads_in_subcategory.extend(detailed_threads)
                logging.info(f"[分类: {category_name}][子分类: {group_name}] 第 {page}/{total_pages} 页完成, 累计帖子数: {len(total_threads_in_subcategory)}")
                
                if page >= total_pages:
                    logging.info(f"[分类: {category_name}][子分类: {group_name}] 所有页面获取完成")
                    break
                    
                page += 1
                
                if page <= total_pages:
                    delay = random.uniform(3, 8)
                    logging.info(f"[分类: {category_name}][子分类: {group_name}] 等待 {delay:.1f}秒后获取下一页...")
                    time.sleep(delay)
                    
            except Exception as e:
                logging.error(f"[分类: {category_name}][子分类: {group_name}] 处理出错: {str(e)}")
                break
        
        results.append({
            'category_id': category_id,
            'category_name': category_name,
            'subcategory_id': group_id,
            'subcategory_name': group_name,
            'threads': total_threads_in_subcategory
        })
        
        if sub_idx < len(subcategories) - 1:
            delay = random.uniform(5, 15)
            logging.info(f"[分类: {category_name}] 等待 {delay:.1f}秒后处理下一个子分类...")
            time.sleep(delay)
    
    logging.info(f"[分类: {category_name}] 处理完成")
    return results

def save_results_incrementally(results, filename):
    """增量保存结果到JSON文件"""
    try:
        if not results:
            logging.info("没有结果需要保存")
            return
        
        existing_data = []
        if os.path.exists(filename) and os.path.getsize(filename) > 2:
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except (json.JSONDecodeError, Exception):
                existing_data = []

        existing_data.extend(results)

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)

        logging.info(f"数据已增量保存到 {filename}")
    except Exception as e:
        logging.error(f"保存数据出错: {str(e)}")

def main():
    """主函数"""
    output_filename = 'zhulong_test_data.json' if TEST_MODE else 'zhulong_full_data.json'
    
    os.makedirs('downloads', exist_ok=True)
    
    logging.info("=" * 50)
    logging.info(f"开始爬取 - {'测试模式' if TEST_MODE else '正式模式'}")
    if TEST_MODE:
        logging.info(f"测试配置: 最大页数={MAX_TEST_PAGES}, 最大子分类数={MAX_TEST_SUBCATEGORIES}")
    logging.info(f"附件下载: {'启用' if DOWNLOAD_ATTACHMENTS else '禁用'}")
    if DOWNLOAD_ATTACHMENTS:
        logging.info(f"并发下载: 每个帖子最多同时下载 {MAX_CONCURRENT_DOWNLOADS} 个附件")
    logging.info("=" * 50)
    
    categories = get_categories()
    
    if not categories:
        logging.error("未获取到任何分类，程序终止")
        return
    
    logging.info(f"共获取到 {len(categories)} 个一级分类")
    
    all_results = []
    total_subcategories_count = 0
    total_threads_count = 0
    total_attachments_count = 0
    total_downloaded_attachments = 0

    for cat_idx, cat in enumerate(categories):
        category_name = cat['category_name']
        try:
            logging.info(f"处理分类 [{cat_idx+1}/{len(categories)}]: {category_name}")
            category_results = process_category(cat)
            
            if category_results:
                all_results.extend(category_results)
                save_results_incrementally(all_results, output_filename)
                
                total_subcategories_count += len(category_results)
                for res in category_results:
                    total_threads_count += len(res['threads'])
                    
                    for thread in res['threads']:
                        attachments = thread.get('downloaded_attachments', [])
                        total_attachments_count += len(attachments)
                        total_downloaded_attachments += sum(1 for a in attachments if a.get('success', False))
            
            if cat_idx < len(categories) - 1:
                delay = random.uniform(10, 20)
                logging.info(f"等待 {delay:.1f}秒后处理下一个分类...")
                time.sleep(delay)
                
        except Exception as e:
            logging.error(f"处理分类 {category_name} 出错: {str(e)}")
            delay = random.uniform(30, 60)
            logging.info(f"出错后等待 {delay:.1f}秒...")
            time.sleep(delay)
    
    logging.info(f"所有爬取任务完成，正在将数据保存到文件: {output_filename}...")
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

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

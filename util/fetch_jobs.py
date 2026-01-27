from playwright.sync_api import Browser 
from playwright.sync_api import Response
from playwright.sync_api._generated import Page , Locator


def get_bytedance_jobs(browser: Browser) -> dict | list:
    url: str = "https://jobs.bytedance.com/campus/position?keywords=&category=6704215862603155720%2C6704215956018694411%2C6704215862557018372%2C6704215957146962184%2C6704215886108035339%2C6704215897130666254%2C6704215888985327886%2C6704216109274368264%2C6938376045242353957%2C6704215958816295181%2C6704219534724696331%2C6704215963966900491%2C6704217321877014787%2C6704216296701036811%2C6704216635923761412%2C6704219452277262596&location=&project=7503447747358361864%2C7493737120754911496%2C7481474995534301447%2C7468181472685164808&type=&job_hot_flag=&current=1&limit=500&functionCategory=&tag="

    res : dict = {} 
    page: Page = browser.new_page()

    def handle_response(response: Response):
        nonlocal res
        if (
            "api/v1/search/job/posts" in response.url
            and response.request.method == "POST"
        ):
            try:
                res = response.json()
                print("bytedance crawl success")
            except Exception as e:
                print("bytedance crawl fail", e)


    page.on("response", handle_response)

    page.goto(url, wait_until="networkidle", timeout=60000)

    page.close()


    return res["data"]["job_post_list"]

def get_alibaba_jobs(browser: Browser) -> dict | list:

    print("start dealing with alibaba star")

    url: str = "https://talent.alibaba.com/campus/position-list?campusType=freshman&lang=zh"
    all_jobs: list = []
    api_results: list = []
    
    page: Page = browser.new_page()

    def handle_response(response: Response):
        if (
            "talent.alibaba.com/position/search" in response.url
            and response.request.method == "POST"
        ):
            try:
                data = response.json()
                if data.get("success") and "content" in data:
                    api_results.append(data)
                    print(f"✓ 获取第 {len(api_results)} 页")
            except Exception as e:
                print(f"✗ 解析失败: {e}")

    page.on("response", handle_response)
    page.goto(url, wait_until="networkidle", timeout=60000)
    
    # 处理第一页
    if api_results:
        datas = api_results[0]["content"].get("datas", [])
        all_jobs.extend(datas)
        print(f"✓ 第1页: 获取 {len(datas)} 个岗位")
    
    # 翻页获取剩余数据
    next_page_button : Locator = page.locator("button.next-next")
    page_num = 2
    while next_page_button.is_enabled():
        try:
            next_page_button.click()
            page.wait_for_timeout(2000)
            
            # ✅ 关键：检查是否收到新的 API 响应
            if len(api_results) >= page_num:
                datas = api_results[page_num - 1]["content"].get("datas", [])
                all_jobs.extend(datas)
                print(f"✓ 第{page_num}页: 获取 {len(datas)} 个岗位")
            else:
                print(f"⚠ 第{page_num}页: 无响应，停止翻页")
                break
                
        except Exception as e:  # ✅ 错误处理
            print(f"✗ 第{page_num}页翻页失败: {e}")
            break

        page_num += 1
    
    page.close()
    print(f"✓ 共获取 {len(all_jobs)} 个岗位")
    return all_jobs

def get_tencent_jobs(browser: Browser) -> list:

    print("start dealing with tencent jobs")
    url: str = "https://join.qq.com/post.html?query=p_14,p_20"
    page: Page = browser.new_page()

    position_url : list = []

    def handle_response_4_total_list(response : Response):
        nonlocal position_url

        if (
            "join.qq.com/api/v1/position/searchPosition" in response.url
            and response.request.method == "POST"
        ):
            try : 
                data : dict = response.json()
                position_list = data["data"]["positionList"] 
                for api_res in position_list :
                    job_url = "https://join.qq.com/post_detail.html?postid=" + api_res["postId"]
                    position_url.append(job_url)

            except Exception as e :
                print(f"解析腾讯青云总览岗位失败 : {e}")
                
    page.on("response" , handle_response_4_total_list)
    page.goto(url, wait_until="networkidle", timeout=60000)

    next_page_button : Locator = page.locator(".btn-next")
    while next_page_button.is_enabled() :
        next_page_button.click()
        page.wait_for_timeout(2000)

    print(f"{len(position_url)} tencent job url crawled")     

    job_details = []
    def handle_response_4_job_details(response : Response) :
        nonlocal job_details
        if (
            "join.qq.com/api/v1/jobDetails/getJobDetailsByPostId" in response.url
            and response.request.method == "GET"
        ):
            try :
                data: dict = response.json()["data"]
                job_details.append(data)
            except Exception as e:
                print(f"解析岗位详情失败 {e} ")

    page.remove_listener("response", handle_response_4_total_list)
    page.on("response" , handle_response_4_job_details)
    for job_url in position_url : 
        print(job_url)
        page.goto(job_url , wait_until="networkidle" , timeout=60000)
        page.wait_for_timeout(2000)
    
    print(f"{len(position_url)} tencent job details crawled")     
    page.close()

    return job_details


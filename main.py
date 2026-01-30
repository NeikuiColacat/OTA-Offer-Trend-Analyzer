import json
import os
from pathlib import Path
from util.browser import Browser_Manager
from util.fetch_jobs import get_bytedance_jobs, get_alibaba_jobs, get_tencent_jobs

from util.clean_json import (
    clean_alibaba_star_jobs_json,
    clean_bytedance_jobs_json,
    clean_tencent_jobs_json,
)


def save_jobs_as_json(jobs: dict | list, file_name : str, save_path: str = "output") -> None:
    try:
        Path(save_path).mkdir(exist_ok=True)

        output_path: Path = Path(save_path) / file_name
        
        with open(output_path , "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=False, indent=4)
        
        print(f"save {file_name} success")

    except Exception as e:
        print(f"save {file_name} error{e}")


if __name__ == "__main__":
    browser_manager = Browser_Manager()
    browser = browser_manager.browser

    bytedance_jobs = get_bytedance_jobs(browser)

    save_jobs_as_json(bytedance_jobs, "bytedance.json")

    ali_jobs = get_alibaba_jobs(browser) 

    save_jobs_as_json(ali_jobs, "ali_star.json")

    tencent_jobs = get_tencent_jobs(browser)

    save_jobs_as_json(tencent_jobs , "tencent.json")
    

    ali_clean_json = clean_alibaba_star_jobs_json("output/ali_star.json")
    bytedance_clean_json = clean_bytedance_jobs_json("output/bytedance.json")
    tencent_clean_json = clean_tencent_jobs_json("output/tencent.json")

    save_jobs_as_json(ali_clean_json, "ali_clean.json")
    save_jobs_as_json(bytedance_clean_json, "bytedance_clean.json")
    save_jobs_as_json(tencent_clean_json, "tencent_clean.json")
import requests
from bs4 import BeautifulSoup
import json
import os

BASE_URL = "https://quotes.toscrape.com" 

def scrape_quotes():
    quotes_data = []        # เก็บข้อมูล quote
    authors_data = []       # เก็บข้อมูล author
    visited_authors = set() # ใช้กันซ้ำ

    page = 1
    while True:
        res = requests.get(f"{BASE_URL}/page/{page}")  
        if res.status_code != 200:                     
            break

        soup = BeautifulSoup(res.text, "html.parser")
        quotes = soup.select(".quote")                  
        if not quotes:
            break                                       

        for q in quotes:
            # ดึง quote / ชื่อ author / tag
            text = q.select_one(".text").get_text(strip=True)
            author = q.select_one(".author").get_text(strip=True)
            tags = [t.get_text() for t in q.select(".tag")]

            quotes_data.append({
                "author": author,
                "quote": text,
                "tags": tags
            })

            # ลิงก์ไปหน้ารายละเอียด author
            author_link = BASE_URL + q.select_one("a")["href"]

            # ถ้ายังไม่เคยดึงข้อมูล author คนนี้
            if author_link not in visited_authors:
                author_page = requests.get(author_link)
                author_soup = BeautifulSoup(author_page.text, "html.parser")

                name = author_soup.select_one(".author-title").get_text(strip=True)
                birth_date = author_soup.select_one(".author-born-date").get_text(strip=True)
                birth_place = author_soup.select_one(".author-born-location").get_text(strip=True)
                detail = author_soup.select_one(".author-description").get_text(strip=True)

                authors_data.append({
                    "name": name,
                    "birth_date": birth_date,
                    "birth_place": birth_place,
                    "detail": detail
                })

                visited_authors.add(author_link)

        page += 1  # ไปหน้าถัดไป

    # สร้างโฟลเดอร์ data ถ้ายังไม่มี
    os.makedirs("data", exist_ok=True)

    # เส้นทางไฟล์ที่บันทึก
    quotes_path = "data/example_quotes.json"
    authors_path = "data/example_authors.json"

    # บันทึกข้อมูล JSON
    with open(quotes_path, "w", encoding="utf-8") as f:
        json.dump(quotes_data, f, ensure_ascii=False, indent=2)
    print(f" Saved {len(quotes_data)} quotes to {quotes_path} ")

    with open(authors_path, "w", encoding="utf-8") as f:
        json.dump(authors_data, f, ensure_ascii=False, indent=2)
    print(f" Saved {len(authors_data)} authors to {authors_path} ")

    # Return path เพื่อส่งต่อให้ Airflow 
    return quotes_path, authors_path


if __name__ == "__main__":
    scrape_quotes()

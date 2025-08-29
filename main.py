import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

def scrape_article_from_wayback(url, timestamp='20250827152503'):
    '''tries to scrape the article from Wayback Machine https://web.archive.org'''
    # build the wayback machine url
    wayback_url = f"https://web.archive.org/web/{timestamp}/{url}"
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(wayback_url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.content, 'html.parser')
        article_text = ' '.join([p.text for p in soup.find('body').find_all('p')]) #Extract the body of the page
        return article_text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {wayback_url}: {e}")
        return None
    except Exception as e:
        print(f"Error processing URL {wayback_url}: {e}")
        return None


def scrape_article(url):
    '''Scrapes the article from the original source'''
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.content, 'html.parser')
        article_text = ' '.join([p.text for p in soup.find('body').find_all('p')]) # Extract the body of the page

        return article_text

    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return None
    except Exception as e:
        print(f"Error processing URL {url}:{e}")
        return None


def main():
    try:
        df1 = pd.read_csv('./data/part_000000.csv')
        chunk_size = 10
        
        # Loop through the DataFrame in chunks
        for start_row in range(0, len(df1), chunk_size):
            end_row = start_row + chunk_size
            
            # Select a chunk of the DataFrame and make a copy
            chunk_df = df1.iloc[start_row:end_row].copy()
            
            print(f"Processing rows {start_row} to {end_row}...")
            
            # Apply the scraping function to the current chunk
            chunk_df['content'] = chunk_df.apply(lambda row: scrape_article_from_wayback(row['url']), axis=1)
            
            # Save the scraped chunk to a unique CSV file
            chunk_df.to_csv(f'./data/scraped_data_chunk_{start_row}.csv', index=False)
            
            print(f"Saved chunk to ./data/scraped_data_chunk_{start_row}.csv\n")
            
            # Optional: Add a small delay to avoid overwhelming the server
            time.sleep(1)

    except FileNotFoundError as e:
        print(f'Error: One or more CSV files not found: {e}')
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

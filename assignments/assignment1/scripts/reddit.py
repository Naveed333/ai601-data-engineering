import praw  # Reddit API library
import os
import csv  # For saving data as CSV
from dotenv import load_dotenv  # To load environment variables
from datetime import datetime  # For formatting the date

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    user_agent=os.getenv("USER_AGENT"),
)

SUBREDDITS = ["CryptoCurrency", "Bitcoin"]
KEYWORDS = [
    "crypto",
    "bitcoin",
    "btc",
    "BTC",
    "ETH",
    "Ethereum",
    "sol",
    "Blockchain",
    "Altcoin",
    "Crypto Wallet",
    "Web3",
    "Token",
    "NFT",
    "DeFi",
    "Crypto Exchange",
    "Crypto Trading",
    "Crypto Investment",
    "Crypto Market",
    "Crypto News",
    "Crypto Regulation",
    "Crypto Mining",
    "Crypto Payment",
    "Crypto Tax",
]
OUTPUT_FILE = "./datasets/raw/reddit_posts.csv"


### **Part 1: Fetch Data from Reddit (Extract)**
def fetch_reddit_data(subreddits, keywords, limit=100):
    """Fetches posts from specified subreddits containing given keywords."""
    posts = []
    for subreddit_name in subreddits:
        print(f"🚀 Fetching data from r/{subreddit_name}...")
        subreddit = reddit.subreddit(subreddit_name)
        for post in subreddit.hot(limit=limit):  # Fetch top posts
            if any(
                keyword.lower() in (post.title + post.selftext).lower()
                for keyword in keywords
            ):
                posts.append(
                    {
                        "Title": post.title,
                        "Post Text": post.selftext,
                        "Author": post.author.name if post.author else "Unknown",
                        "Date": datetime.utcfromtimestamp(post.created_utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "Upvotes": post.score,
                        "Subreddit": subreddit_name,
                    }
                )

    if posts:
        print(f"✅ Fetched {len(posts)} relevant posts.")
    else:
        print("❌ No matching posts found.")

    return posts


### **Part 2: Save Data to CSV (Load)**
def save_to_csv(data, filename):
    """Saves extracted Reddit data to a CSV file."""
    if not data:
        print("⚠️ No data to save.")
        return

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["Title", "Post Text", "Author", "Date", "Upvotes", "Subreddit"],
        )
        writer.writeheader()
        writer.writerows(data)

    print(f"✅ Data successfully saved to {filename}")


### **Part 3: Clean Data (Transform)**
def clean_data(input_file, output_file):
    cleaned_posts = []

    with open(input_file, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["Post Text"].strip():
                row["Title"] = row["Title"].strip().title()
                row["Post Text"] = row["Post Text"].strip()
                cleaned_posts.append(row)

    if cleaned_posts:
        save_to_csv(cleaned_posts, output_file)
        print(f"✅ Cleaned data saved to {output_file}")
    else:
        print("❌ No valid posts found after cleaning.")


def summarize_data(filename):
    subreddit_counts = {}
    upvote_sums = {}
    total_posts = 0

    with open(filename, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            subreddit = row["Subreddit"]
            upvotes = int(row["Upvotes"])

            subreddit_counts[subreddit] = subreddit_counts.get(subreddit, 0) + 1
            upvote_sums[subreddit] = upvote_sums.get(subreddit, 0) + upvotes
            total_posts += 1

    if total_posts == 0:
        print("⚠️ No data available for summary.")
        return

    print("\n📊 Reddit Data Summary 📊")
    print(f"Total Posts: {total_posts}")
    for subreddit, count in subreddit_counts.items():
        avg_upvotes = upvote_sums[subreddit] / count
        print(f"📌 {subreddit}: {count} posts, Avg Upvotes: {avg_upvotes:.2f}")


if __name__ == "__main__":
    reddit_data = fetch_reddit_data(SUBREDDITS, KEYWORDS, limit=100)

    if reddit_data:
        save_to_csv(reddit_data, OUTPUT_FILE)
        clean_data(OUTPUT_FILE, "./datasets/clean/reddit_posts.csv")
        summarize_data("./datasets/clean/reddit_posts.csv")

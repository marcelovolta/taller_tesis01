from googleapiclient.discovery import build
import pandas as pd
import duckdb
import polars as pl
import logging
import datetime
import re

logger = logging.getLogger(__name__)

API_KEY = "AIzaSyDjcsIMX20u9dslNqL0-pc9MdJ6XGA2_kU"
youtube = build("youtube", "v3", developerKey=API_KEY)

def normalize_title(title: str) -> str:
    '''
    Normalizes the string of a title to improve search accuracy

    Args:
        title (str) the movie tielt to normalize
    
    Returns:
        str: The normalized title
    '''
    title = title.lower()
    title = re.sub(r"\([^)]*\)", "", title)
    title = re.sub(r"[^a-z0-9 ]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title

def find_trailers(movie_title: str, max_results:int =None):
    '''
    Finds trailers by name and returns a dataframe with the video_id, title, and channel_id.

    Args:
        query (str): The name of the trailer to search for.
        max_results (int): The maximum number of results to return.

    Returns:
        pd.DataFrame: A dataframe with the video_id, title, and channel_id.
    '''
    query = f'"{movie_title}" "official trailer" "official teaser"'
    req = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        maxResults=max_results,
        regionCode="US",
        relevanceLanguage="en"
    )
    res = req.execute()
    
    
    videos = []
    for item in res.get("items", []):
        video_id = item["id"]["videoId"]
        returned_title = item["snippet"]["title"]
        channel = item["snippet"]["channelTitle"]

        returned_title_norm = normalize_title(returned_title)
        movie_title_norm = normalize_title(movie_title)

        is_likely_match = (
            movie_title_norm in returned_title_norm
            and "trailer" in returned_title_norm
            and "official" in returned_title_norm
            
        )
    
        if is_likely_match:
            videos.append({
                "video_id": video_id,
                "title": returned_title,
                "channel": channel,
                "published_at": item["snippet"]["publishedAt"],
                "url": f"https://www.youtube.com/watch?v={video_id}"
            })

    
    # return pd.DataFrame(videos)
    return videos

def get_comments(video_id: int, limit_date: datetime.date = None, max_pages: int = None) -> pd.DataFrame:
    '''
    Gets comments for a given video ID and returns a dataframe with the comment_id, author_name, author_channel_id, text, published_at, and like_count.

    Args:
        video_id (int): The ID of the video to get comments for.
        limit_date (datetime.date): The date to limit the comments to.
        max_pages (int): The maximum number of pages to get comments from.
    '''
    rows = []
    req = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100,
        textFormat="plainText",
        order="time"
    )

    pages = 0
    next_Token = None
    logger.info(f"Getting comments for video {video_id} with limit date {limit_date} and max pages {max_pages}")
    while req is not None and (max_pages is None or pages < max_pages):
        res = req.execute()
        for item in res.get("items", []):
            top = item["snippet"]["topLevelComment"]["snippet"]
            published_at = pd.to_datetime(top.get("publishedAt"), utc=True)
            if limit_date is not None and published_at.date() < limit_date:
                rows.append({
                    "video_id": video_id,
                    "comment_id": item["snippet"]["topLevelComment"]["id"],
                    "author_name": top.get("authorDisplayName"),
                    "author_channel_id": top.get("authorChannelId", {}).get("value"),
                    "text": top.get("textDisplay"),
                    "published_at": top.get("publishedAt"),
                    "like_count": top.get("likeCount")
                })
            
        req = youtube.commentThreads().list_next(req, res)
        pages += 1

    return pd.DataFrame(rows)
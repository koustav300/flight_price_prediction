
#-----------------------------------------------------------------------
#very Slow
import pafy

url = "https://www.youtube.com/watch?v=Pp6CO2_YEDE"
video = pafy.new(url)

streams = video.streams
for i in streams:
    print(i)

# get best resolution regardless of format
best = video.getbest()

print(best.resolution, best.extension)

# Download the video
best.download()


#--------------------------------------------------
# It is also slow--- but shows the progress bar
# #Download video
import youtube_dl
ydl_opts = {}

def dwl_vid():
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download([zxt])

channel = 1
while (channel == int(1)):
    #link_of_the_video = input("Copy & paste the URL of the YouTube video you want to download:- ")
    link_of_the_video = test_link

    zxt = link_of_the_video.strip()
    dwl_vid()
    #channel = int(input("Enter 1 if you want to download more videos \nEnter 0 if you are done "))
    channel=0


   # comment_mainContainer = driver.find_elements(By.TAG_NAME, 'ytd-comment-thread-renderer')
    # for comment_div in comment_mainContainer:
    #     comment_text   = comment_div.find_elements(By.ID, 'content-text')[0].text
    #     commenter_name = comment_div.find_elements(By.ID, 'author-text')[0].find_elements(By.TAG_NAME, 'span')[0].text
    #
    #     print('----------------------------- ', commenter_name)
    #     print(comment_text)

from pytube import YouTube
def fetch_vdo_info(ui_link):
    yt = YouTube(ui_link)
    title = yt.title
    views = yt.views
    length = yt.length
    channel_id = yt.channel_id
    thumbnail_img_link = yt.thumbnail_url
    chnl_author = yt.author
    return [title, views, length, channel_id, thumbnail_img_link, chnl_author]
